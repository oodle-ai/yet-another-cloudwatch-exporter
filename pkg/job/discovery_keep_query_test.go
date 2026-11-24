// Copyright 2026 The Prometheus Authors
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
	"slices"
	"testing"

	"github.com/prometheus/common/promslog"
	"github.com/stretchr/testify/require"

	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/clients/cloudwatch"
	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/model"
)

type keepQueryTaggingClient struct{}

func (keepQueryTaggingClient) GetResources(context.Context, model.DiscoveryJob, string) ([]*model.TaggedResource, error) {
	return nil, nil
}

// keepQueryCloudwatchClient lists its metrics. The other methods come from
// the nil embedded interface and panic if discovery calls them.
type keepQueryCloudwatchClient struct {
	cloudwatch.Client
	metrics []*model.Metric
}

func (c keepQueryCloudwatchClient) ListMetrics(_ context.Context, _ string, _ *model.MetricConfig, _ bool, fn func(page []*model.Metric)) error {
	fn(c.metrics)
	return nil
}

// recordingProcessor keeps the queries that discovery sends to
// GetMetricData.
type recordingProcessor struct {
	requests []*model.CloudwatchData
}

func (p *recordingProcessor) Run(_ context.Context, _ string, requests []*model.CloudwatchData) ([]*model.CloudwatchData, error) {
	p.requests = requests
	return requests, nil
}

func requestedQueries(requests []*model.CloudwatchData) []string {
	queries := make([]string, 0, len(requests))
	for _, request := range requests {
		queries = append(queries, request.MetricName+"/"+request.GetMetricDataProcessingParams.Statistic)
	}
	slices.Sort(queries)
	return queries
}

func TestRunDiscoveryJobKeepQuery(t *testing.T) {
	metrics := []*model.Metric{
		{MetricName: "CPUUtilization", Namespace: "AWS/EC2", Dimensions: []model.Dimension{{Name: "InstanceId", Value: "i-1"}}},
		{MetricName: "NetworkIn", Namespace: "AWS/EC2", Dimensions: []model.Dimension{{Name: "InstanceId", Value: "i-1"}}},
	}
	baseJob := model.DiscoveryJob{
		Namespace: "AWS/EC2",
		Metrics: []*model.MetricConfig{
			{Name: "CPUUtilization", Statistics: []string{"Average", "Maximum"}},
			{Name: "NetworkIn", Statistics: []string{"Sum"}},
		},
	}

	tests := []struct {
		name      string
		keepQuery func(*model.CloudwatchData) bool
		want      []string
	}{
		{
			name: "no hook keeps every query",
			want: []string{"CPUUtilization/Average", "CPUUtilization/Maximum", "NetworkIn/Sum"},
		},
		{
			name: "hook removes single statistics",
			keepQuery: func(query *model.CloudwatchData) bool {
				return query.GetMetricDataProcessingParams.Statistic != "Average"
			},
			want: []string{"CPUUtilization/Maximum", "NetworkIn/Sum"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := baseJob
			job.KeepQuery = tt.keepQuery
			processor := &recordingProcessor{}
			_, _ = runDiscoveryJob(
				context.Background(),
				promslog.NewNopLogger(),
				job,
				"us-east-1",
				keepQueryTaggingClient{},
				keepQueryCloudwatchClient{metrics: metrics},
				processor,
			)
			require.Equal(t, tt.want, requestedQueries(processor.requests))
		})
	}
}

// A hook that removes every query must not call GetMetricData at all.
func TestRunDiscoveryJobKeepQueryRemovesAll(t *testing.T) {
	job := model.DiscoveryJob{
		Namespace:         "AWS/EC2",
		IncludeAllMetrics: true,
		KeepQuery:         func(*model.CloudwatchData) bool { return false },
	}
	processor := &recordingProcessor{}
	_, data := runDiscoveryJob(
		context.Background(),
		promslog.NewNopLogger(),
		job,
		"us-east-1",
		keepQueryTaggingClient{},
		keepQueryCloudwatchClient{metrics: []*model.Metric{{MetricName: "CPUUtilization", Namespace: "AWS/EC2"}}},
		processor,
	)
	require.Nil(t, processor.requests)
	require.Nil(t, data)
}
