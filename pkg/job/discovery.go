// Copyright 2024 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package job

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	awstypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/clients/cloudwatch"
	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/clients/tagging"
	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/config"
	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/job/maxdimassociator"
	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/model"
)

type resourceAssociator interface {
	AssociateMetricToResource(cwMetric *model.Metric) (*model.TaggedResource, bool)
}

type getMetricDataProcessor interface {
	Run(ctx context.Context, namespace string, requests []*model.CloudwatchData) ([]*model.CloudwatchData, error)
}

type metricInfo struct {
	cwMetric   *model.Metric
	metricConf *model.MetricConfig
}

const (
	defaultDiscoveryJobPeriod = 60
	// Below 2 settings effectively mean that we query metric data for `[now-15m, now-5m]` range
	// This avoids any gaps in metric data in case of small delays in scheduling. Each scrape cycle
	// runs every 5m, so 2 consecutive scrape cycle has an overlap of 5m window for which both scrape
	// cycles will pull the data. This assumes de-duplication is handled during ingestion.
	defaultDiscoveryJobLength = 600
	defaultDiscoveryJobDelay  = 300
)

var defaultStatistics = []string{
	string(awstypes.StatisticAverage),
	string(awstypes.StatisticSum),
	string(awstypes.StatisticSampleCount),
	string(awstypes.StatisticMaximum),
	string(awstypes.StatisticMinimum),
}

func runDiscoveryJob(
	ctx context.Context,
	logger *slog.Logger,
	job model.DiscoveryJob,
	region string,
	clientTag tagging.Client,
	clientCloudwatch cloudwatch.Client,
	gmdProcessor getMetricDataProcessor,
) ([]*model.TaggedResource, []*model.CloudwatchData) {
	logger.Debug("Get tagged resources")
	hasSearchTags := len(job.SearchTags) > 0

	resources, err := clientTag.GetResources(ctx, job, region)
	if err != nil && hasSearchTags {
		// Early return only if search tags were specified and we couldn't find resources
		if errors.Is(err, tagging.ErrExpectedToFindResources) {
			logger.Error("No tagged resources made it through filtering", "err", err)
		} else {
			logger.Error("Couldn't describe resources", "err", err)
		}
		return nil, nil
	}

	if len(resources) == 0 {
		logger.Debug("No tagged resources", "region", region, "namespace", job.Namespace)
	}

	svc := config.SupportedServices.GetService(job.Namespace)
	getMetricDatas := getMetricDataForQueries(ctx, logger, job, svc, clientCloudwatch, resources, hasSearchTags)
	if len(getMetricDatas) == 0 {
		logger.Info("No metrics data found")
		return resources, nil
	}

	getMetricDatas, err = gmdProcessor.Run(ctx, svc.Namespace, getMetricDatas)
	if err != nil {
		logger.Error("Failed to get metric data", "err", err)
		return nil, nil
	}

	return resources, getMetricDatas
}

func getMetricDataForQueries(
	ctx context.Context,
	logger *slog.Logger,
	discoveryJob model.DiscoveryJob,
	svc *config.ServiceConfig,
	clientCloudwatch cloudwatch.Client,
	resources []*model.TaggedResource,
	hasSearchTags bool,
) []*model.CloudwatchData {
	var getMetricDatas []*model.CloudwatchData

	var assoc resourceAssociator
	if len(svc.DimensionRegexps) > 0 && len(resources) > 0 {
		assoc = maxdimassociator.NewAssociator(logger, discoveryJob.DimensionsRegexps, resources)
	} else {
		// If we don't have dimension regex's and resources there's nothing to associate but metrics shouldn't be skipped
		assoc = nopAssociator{}
	}

	metricNameResourceToMetricMap := make(map[string]map[*model.TaggedResource][]*metricInfo)
	globalResource := &model.TaggedResource{
		ARN:       "global",
		Namespace: discoveryJob.Namespace,
	}

	// Query all recently active metrics for the namespace and filter based on discovery job configuration
	err := clientCloudwatch.ListMetrics(ctx, svc.Namespace, nil /* metric */, discoveryJob.RecentlyActiveOnly, func(page []*model.Metric) {
		data := getFilteredMetricDatas(
			logger,
			discoveryJob.Namespace,
			discoveryJob.ExportedTagsOnMetrics,
			page,
			discoveryJob.Metrics,
			discoveryJob.DimensionNameRequirements,
			assoc,
			hasSearchTags,
			discoveryJob.DedupeResourceMetrics,
			discoveryJob.IncludeAllMetrics,
			metricNameResourceToMetricMap,
			globalResource,
		)

		getMetricDatas = append(getMetricDatas, data...)
	})
	if err != nil {
		logger.Error("Failed to get full metric list", "namespace", svc.Namespace, "err", err)
		return getMetricDatas
	}

	if discoveryJob.DedupeResourceMetrics {
		for _, resourceToMetricMap := range metricNameResourceToMetricMap {
			for resource, metrics := range resourceToMetricMap {
				for _, metric := range metrics {
					metricTags := resource.MetricTags(discoveryJob.ExportedTagsOnMetrics)
					for _, stat := range metric.metricConf.Statistics {
						getMetricDatas = append(getMetricDatas, &model.CloudwatchData{
							MetricName:   metric.metricConf.Name,
							ResourceName: resource.ARN,
							Namespace:    resource.Namespace,
							Dimensions:   metric.cwMetric.Dimensions,
							GetMetricDataProcessingParams: &model.GetMetricDataProcessingParams{
								Period:    metric.metricConf.Period,
								Length:    metric.metricConf.Length,
								Delay:     metric.metricConf.Delay,
								Statistic: stat,
							},
							MetricMigrationParams: model.MetricMigrationParams{
								NilToZero:              metric.metricConf.NilToZero,
								AddCloudwatchTimestamp: metric.metricConf.AddCloudwatchTimestamp,
							},
							Tags:                      metricTags,
							GetMetricDataResult:       nil,
							GetMetricStatisticsResult: nil,
						})
					}
				}
			}
		}
	}

	return getMetricDatas
}

type nopAssociator struct{}

func (ns nopAssociator) AssociateMetricToResource(_ *model.Metric) (*model.TaggedResource, bool) {
	return nil, false
}

func getFilteredMetricDatas(
	logger *slog.Logger,
	namespace string,
	tagsOnMetrics []string,
	metricsList []*model.Metric,
	discoveryJobMetrics []*model.MetricConfig,
	dimensionNameList []string,
	assoc resourceAssociator,
	hasSearchTags bool,
	dedupeResourceMetrics bool,
	includeAllMetrics bool,
	metricNameResourceToMetricMap map[string]map[*model.TaggedResource][]*metricInfo,
	globalResource *model.TaggedResource,
) []*model.CloudwatchData {
	metricNameToDiscoveryJobMetricConfig := make(map[string]*model.MetricConfig)
	for _, djMetric := range discoveryJobMetrics {
		metricNameToDiscoveryJobMetricConfig[djMetric.Name] = djMetric
	}

	getMetricsData := make([]*model.CloudwatchData, 0, len(metricsList))
	for _, cwMetric := range metricsList {
		djMetric, ok := metricNameToDiscoveryJobMetricConfig[cwMetric.MetricName]
		if !ok {
			if !includeAllMetrics {
				continue
			}

			djMetric = &model.MetricConfig{
				Name:       cwMetric.MetricName,
				Statistics: defaultStatistics,
				Period:     defaultDiscoveryJobPeriod,
				Length:     defaultDiscoveryJobLength,
				Delay:      defaultDiscoveryJobDelay,
			}
		}

		if len(dimensionNameList) > 0 && !metricDimensionsMatchNames(cwMetric, dimensionNameList) {
			continue
		}

		matchedResource, skip := assoc.AssociateMetricToResource(cwMetric)
		// Do not skip the metric if no search tags are specified in the configuration
		// as resource matching is not necessary if filtering by tags is not requested
		if skip && hasSearchTags {
			dimensions := make([]string, 0, len(cwMetric.Dimensions))
			for _, dim := range cwMetric.Dimensions {
				dimensions = append(dimensions, fmt.Sprintf("%s=%s", dim.Name, dim.Value))
			}
			logger.Debug("skipping metric unmatched by associator", "metric", djMetric.Name, "dimensions", strings.Join(dimensions, ","))

			continue
		}

		resource := matchedResource
		if resource == nil {
			resource = globalResource
		}

		if dedupeResourceMetrics {
			if _, ok := metricNameResourceToMetricMap[cwMetric.MetricName]; !ok {
				metricNameResourceToMetricMap[cwMetric.MetricName] = make(map[*model.TaggedResource][]*metricInfo)
			}

			resourceToMetricMap := metricNameResourceToMetricMap[cwMetric.MetricName]
			if existingMetrics, ok := resourceToMetricMap[resource]; !ok {
				resourceToMetricMap[resource] = []*metricInfo{{cwMetric: cwMetric, metricConf: djMetric}}
			} else {
				foundSuperset := false
				foundSubset := false
				for i, existingMetric := range existingMetrics {
					if hasSupersetDimensions(cwMetric, existingMetric.cwMetric) {
						// replace existing metric if current metric is more granular
						resourceToMetricMap[resource][i] = &metricInfo{cwMetric: cwMetric, metricConf: djMetric}
						foundSuperset = true
						break
					} else if hasSupersetDimensions(existingMetric.cwMetric, cwMetric) {
						// skip current metric if existing metric is more granular
						foundSubset = true
						break
					}
				}

				if !foundSuperset && !foundSubset {
					// Current metric has a disjoint set of dimensions, add it to the list for which metric
					// data needs to be fetched.
					resourceToMetricMap[resource] = append(
						resourceToMetricMap[resource],
						&metricInfo{cwMetric: cwMetric, metricConf: djMetric},
					)
				}
			}

			// For dedupedResourceMetrics, getMetricsData is populated after iterating over all metrics
			// to be able to dedupe across multiple pages.
		} else {
			metricTags := resource.MetricTags(tagsOnMetrics)
			for _, stat := range djMetric.Statistics {
				getMetricsData = append(getMetricsData, &model.CloudwatchData{
					MetricName:   djMetric.Name,
					ResourceName: resource.ARN,
					Namespace:    namespace,
					Dimensions:   cwMetric.Dimensions,
					GetMetricDataProcessingParams: &model.GetMetricDataProcessingParams{
						Period:    djMetric.Period,
						Length:    djMetric.Length,
						Delay:     djMetric.Delay,
						Statistic: stat,
					},
					MetricMigrationParams: model.MetricMigrationParams{
						NilToZero:              djMetric.NilToZero,
						AddCloudwatchTimestamp: djMetric.AddCloudwatchTimestamp,
					},
					Tags:                      metricTags,
					GetMetricDataResult:       nil,
					GetMetricStatisticsResult: nil,
				})
			}
		}
	}
	return getMetricsData
}

func metricDimensionsMatchNames(metric *model.Metric, dimensionNameRequirements []string) bool {
	if len(dimensionNameRequirements) != len(metric.Dimensions) {
		return false
	}
	for _, dimension := range metric.Dimensions {
		foundMatch := false
		for _, dimensionName := range dimensionNameRequirements {
			if strings.EqualFold(dimension.Name, dimensionName) {
				foundMatch = true
				break
			}
		}
		if !foundMatch {
			return false
		}
	}
	return true
}

func hasSupersetDimensions(current *model.Metric, old *model.Metric) bool {
	if len(current.Dimensions) < len(old.Dimensions) {
		return false
	}

	for _, oldDim := range old.Dimensions {
		found := false
		for _, currentDim := range current.Dimensions {
			if strings.EqualFold(oldDim.Name, currentDim.Name) && oldDim.Value == currentDim.Value {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}
