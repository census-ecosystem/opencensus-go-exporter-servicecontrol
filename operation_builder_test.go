// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package servicecontrol

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"google.golang.org/protobuf/testing/protocmp"

	timestamppb "github.com/golang/protobuf/ptypes/timestamp"
	scpb "google.golang.org/genproto/googleapis/api/servicecontrol/v1"
)

func TestAddAndBuild(t *testing.T) {
	labelTag1 := tag.MustNewKey("label1")
	labelTag2 := tag.MustNewKey("label2")
	int64SumView := &view.View{
		Name:        "testservice.com/request_count",
		TagKeys:     []tag.Key{labelTag1, labelTag2},
		Measure:     stats.Int64("testservice.com/request_count", "description", stats.UnitDimensionless),
		Aggregation: view.Sum(),
	}
	float64SumView := &view.View{
		Name:        "testservice.com/float_sum",
		Measure:     stats.Float64("testservice.com/float_sum", "description", stats.UnitDimensionless),
		Aggregation: view.Sum(),
	}

	int64LastValueView := &view.View{
		Name:        "testservice.com/latency",
		TagKeys:     []tag.Key{labelTag1},
		Measure:     stats.Int64("testservice.com/latency", "description", stats.UnitMilliseconds),
		Aggregation: view.LastValue(),
	}

	float64LastValueView := &view.View{
		Name:        "testservice.com/utilization",
		TagKeys:     []tag.Key{labelTag1},
		Measure:     stats.Float64("testservice.com/utilization", "description", stats.UnitNone),
		Aggregation: view.LastValue(),
	}

	start, err := time.Parse(time.RFC3339, "2019-09-03T11:16:10Z")
	if err != nil {
		t.Fatalf("Cannot set the start time: %v", err)
	}

	tests := []struct {
		name          string
		operationTags []tag.Key
		viewData      []*view.Data
		want          []*scpb.Operation
	}{
		{
			name:          "int64_sum_values_(CUMULATIVE)",
			operationTags: []tag.Key{},
			viewData: []*view.Data{
				{
					View: int64SumView,
					Rows: []*view.Row{
						{
							Tags: []tag.Tag{
								tag.Tag{Key: labelTag1, Value: "label1-value1"},
								tag.Tag{Key: labelTag2, Value: "label2-value1"}},
							Data: &view.SumData{Value: 10},
						},
						{
							Tags: []tag.Tag{
								tag.Tag{Key: labelTag1, Value: "label1-value2"},
								tag.Tag{Key: labelTag2, Value: "label2-value1"}},
							Data: &view.SumData{Value: 13},
						},
					},
					Start: start,
					End:   start.Add(time.Second),
				},
			},
			want: []*scpb.Operation{
				{
					OperationName: "OpenCensus Reported Metrics",
					Labels: map[string]string{
						"common-label": "common-label-value",
					},
					MetricValueSets: []*scpb.MetricValueSet{
						{
							MetricName: "testservice.com/request_count",
							MetricValues: []*scpb.MetricValue{
								{
									Labels: map[string]string{
										"label1": "label1-value1",
										"label2": "label2-value1",
									},
									StartTime: &timestamppb.Timestamp{
										Seconds: 1567509370,
									},
									EndTime: &timestamppb.Timestamp{
										Seconds: 1567509371,
									},
									Value: &scpb.MetricValue_Int64Value{
										Int64Value: 10,
									},
								},
								{
									Labels: map[string]string{
										"label1": "label1-value2",
										"label2": "label2-value1",
									},
									StartTime: &timestamppb.Timestamp{
										Seconds: 1567509370,
									},
									EndTime: &timestamppb.Timestamp{
										Seconds: 1567509371,
									},
									Value: &scpb.MetricValue_Int64Value{
										Int64Value: 13,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name:          "float64_sum_values_(CUMULATIVE)",
			operationTags: []tag.Key{},
			viewData: []*view.Data{
				{
					View: float64SumView,
					Rows: []*view.Row{
						{
							Data: &view.SumData{Value: 1.2},
						},
						{
							Data: &view.SumData{Value: 1.3},
						},
					},
					Start: start,
					End:   start.Add(time.Second),
				},
			},
			want: []*scpb.Operation{
				{
					OperationName: "OpenCensus Reported Metrics",
					Labels: map[string]string{
						"common-label": "common-label-value",
					},
					MetricValueSets: []*scpb.MetricValueSet{
						{
							MetricName: "testservice.com/float_sum",
							MetricValues: []*scpb.MetricValue{
								{
									StartTime: &timestamppb.Timestamp{
										Seconds: 1567509370,
									},
									EndTime: &timestamppb.Timestamp{
										Seconds: 1567509371,
									},
									Value: &scpb.MetricValue_DoubleValue{
										DoubleValue: 1.2,
									},
								},
								{
									StartTime: &timestamppb.Timestamp{
										Seconds: 1567509370,
									},
									EndTime: &timestamppb.Timestamp{
										Seconds: 1567509371,
									},
									Value: &scpb.MetricValue_DoubleValue{
										DoubleValue: 1.3,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name:          "int64_lastValue_values_(GAUGE)",
			operationTags: []tag.Key{},
			viewData: []*view.Data{
				{
					View: int64LastValueView,
					Rows: []*view.Row{
						{
							Tags: []tag.Tag{
								tag.Tag{Key: labelTag1, Value: "label1-value1"},
							},
							Data: &view.LastValueData{Value: 20},
						},
						{
							Tags: []tag.Tag{
								tag.Tag{Key: labelTag1, Value: "label1-value2"},
							},
							Data: &view.LastValueData{Value: 30},
						},
					},
					Start: start,
					End:   start.Add(time.Second),
				},
			},
			want: []*scpb.Operation{
				{
					OperationName: "OpenCensus Reported Metrics",
					Labels: map[string]string{
						"common-label": "common-label-value",
					},
					MetricValueSets: []*scpb.MetricValueSet{
						{
							MetricName: "testservice.com/latency",
							MetricValues: []*scpb.MetricValue{
								{
									Labels: map[string]string{
										"label1": "label1-value1",
									},
									// Start and End times are equal for GAUGE metrics.
									StartTime: &timestamppb.Timestamp{
										Seconds: 1567509371,
									},
									EndTime: &timestamppb.Timestamp{
										Seconds: 1567509371,
									},
									Value: &scpb.MetricValue_Int64Value{
										Int64Value: 20,
									},
								},
								{
									Labels: map[string]string{
										"label1": "label1-value2",
									},
									// Start and End times are equal for GAUGE metrics.
									StartTime: &timestamppb.Timestamp{
										Seconds: 1567509371,
									},
									EndTime: &timestamppb.Timestamp{
										Seconds: 1567509371,
									},
									Value: &scpb.MetricValue_Int64Value{
										Int64Value: 30,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name:          "float64_lastValue_values_(GAUGE)",
			operationTags: []tag.Key{},
			viewData: []*view.Data{
				{
					View: float64LastValueView,
					Rows: []*view.Row{
						{
							Tags: []tag.Tag{
								tag.Tag{Key: labelTag1, Value: "label1-value1"},
							},
							Data: &view.LastValueData{Value: 0.33},
						},
						{
							Tags: []tag.Tag{
								tag.Tag{Key: labelTag1, Value: "label1-value2"},
							},
							Data: &view.LastValueData{Value: 0.50},
						},
					},
					Start: start,
					End:   start.Add(time.Second),
				},
			},
			want: []*scpb.Operation{
				{
					OperationName: "OpenCensus Reported Metrics",
					Labels: map[string]string{
						"common-label": "common-label-value",
					},
					MetricValueSets: []*scpb.MetricValueSet{
						{
							MetricName: "testservice.com/utilization",
							MetricValues: []*scpb.MetricValue{
								{
									Labels: map[string]string{
										"label1": "label1-value1",
									},
									// Start and End times are equal for GAUGE metrics.
									StartTime: &timestamppb.Timestamp{
										Seconds: 1567509371,
									},
									EndTime: &timestamppb.Timestamp{
										Seconds: 1567509371,
									},
									Value: &scpb.MetricValue_DoubleValue{
										DoubleValue: 0.33,
									},
								},
								{
									Labels: map[string]string{
										"label1": "label1-value2",
									},
									// Start and End times are equal for GAUGE metrics.
									StartTime: &timestamppb.Timestamp{
										Seconds: 1567509371,
									},
									EndTime: &timestamppb.Timestamp{
										Seconds: 1567509371,
									},
									Value: &scpb.MetricValue_DoubleValue{
										DoubleValue: 0.50,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			// The labelTag1 is an operation label. Because it has different values
			// when associated with two metrics, the builder generates two operations.
			name: "operation_labels",
			operationTags: []tag.Key{
				labelTag1,
			},
			viewData: []*view.Data{
				{
					View: int64SumView,
					Rows: []*view.Row{
						{
							Tags: []tag.Tag{
								tag.Tag{Key: labelTag1, Value: "label1-value1"},
								tag.Tag{Key: labelTag2, Value: "label2-value1"}},
							Data: &view.SumData{Value: 10},
						},
						{
							Tags: []tag.Tag{
								tag.Tag{Key: labelTag1, Value: "label1-value2"},
								tag.Tag{Key: labelTag2, Value: "label2-value1"}},
							Data: &view.SumData{Value: 13},
						},
					},
					Start: start,
					End:   start.Add(time.Second),
				},
			},
			want: []*scpb.Operation{
				{
					OperationName: "OpenCensus Reported Metrics",
					Labels: map[string]string{
						"common-label": "common-label-value",
						"label1":       "label1-value1",
					},
					MetricValueSets: []*scpb.MetricValueSet{
						{
							MetricName: "testservice.com/request_count",
							MetricValues: []*scpb.MetricValue{
								{
									Labels: map[string]string{
										"label2": "label2-value1",
									},
									StartTime: &timestamppb.Timestamp{
										Seconds: 1567509370,
									},
									EndTime: &timestamppb.Timestamp{
										Seconds: 1567509371,
									},
									Value: &scpb.MetricValue_Int64Value{
										Int64Value: 10,
									},
								},
							},
						},
					},
				},
				{
					OperationName: "OpenCensus Reported Metrics",
					Labels: map[string]string{
						"common-label": "common-label-value",
						"label1":       "label1-value2",
					},
					MetricValueSets: []*scpb.MetricValueSet{
						{
							MetricName: "testservice.com/request_count",
							MetricValues: []*scpb.MetricValue{
								{
									Labels: map[string]string{
										"label2": "label2-value1",
									},
									StartTime: &timestamppb.Timestamp{
										Seconds: 1567509370,
									},
									EndTime: &timestamppb.Timestamp{
										Seconds: 1567509371,
									},
									Value: &scpb.MetricValue_Int64Value{
										Int64Value: 13,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			// Two operation level labels have the same value. One metric uses one
			// label, while another label uses the other. The builder should generate
			// two operations.
			name: "different_operation_labels_with_shared_value",
			operationTags: []tag.Key{
				labelTag1,
				labelTag2,
			},
			viewData: []*view.Data{
				{
					View: int64SumView,
					Rows: []*view.Row{
						{
							Tags: []tag.Tag{
								tag.Tag{Key: labelTag1, Value: "shared-value"},
							},
							Data: &view.SumData{Value: 10},
						},
						{
							Tags: []tag.Tag{
								tag.Tag{Key: labelTag2, Value: "shared-value"},
							},
							Data: &view.SumData{Value: 13},
						},
					},
					Start: start,
					End:   start.Add(time.Second),
				},
			},
			want: []*scpb.Operation{
				{
					OperationName: "OpenCensus Reported Metrics",
					Labels: map[string]string{
						"common-label": "common-label-value",
						"label1":       "shared-value",
					},
					MetricValueSets: []*scpb.MetricValueSet{
						{
							MetricName: "testservice.com/request_count",
							MetricValues: []*scpb.MetricValue{
								{
									StartTime: &timestamppb.Timestamp{
										Seconds: 1567509370,
									},
									EndTime: &timestamppb.Timestamp{
										Seconds: 1567509371,
									},
									Value: &scpb.MetricValue_Int64Value{
										Int64Value: 10,
									},
								},
							},
						},
					},
				},
				{
					OperationName: "OpenCensus Reported Metrics",
					Labels: map[string]string{
						"common-label": "common-label-value",
						"label2":       "shared-value",
					},
					MetricValueSets: []*scpb.MetricValueSet{
						{
							MetricName: "testservice.com/request_count",
							MetricValues: []*scpb.MetricValue{
								{
									StartTime: &timestamppb.Timestamp{
										Seconds: 1567509370,
									},
									EndTime: &timestamppb.Timestamp{
										Seconds: 1567509371,
									},
									Value: &scpb.MetricValue_Int64Value{
										Int64Value: 13,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			// The metrics tags are used to set the operation labels if there the operationTag is nil.
			name: "nil_operation_tags",
			viewData: []*view.Data{
				{
					View: int64SumView,
					Rows: []*view.Row{
						{
							Tags: []tag.Tag{
								tag.Tag{Key: labelTag1, Value: "label1-value1"},
								tag.Tag{Key: labelTag2, Value: "label2-value1"}},
							Data: &view.SumData{Value: 10},
						},
						{
							Tags: []tag.Tag{
								tag.Tag{Key: labelTag1, Value: "label1-value2"},
								tag.Tag{Key: labelTag2, Value: "label2-value1"}},
							Data: &view.SumData{Value: 13},
						},
					},
					Start: start,
					End:   start.Add(time.Second),
				},
			},
			want: []*scpb.Operation{
				{
					OperationName: "OpenCensus Reported Metrics",
					Labels: map[string]string{
						"common-label": "common-label-value",
						"label1":       "label1-value1",
						"label2":       "label2-value1",
					},
					MetricValueSets: []*scpb.MetricValueSet{
						{
							MetricName: "testservice.com/request_count",
							MetricValues: []*scpb.MetricValue{
								{
									StartTime: &timestamppb.Timestamp{
										Seconds: 1567509370,
									},
									EndTime: &timestamppb.Timestamp{
										Seconds: 1567509371,
									},
									Value: &scpb.MetricValue_Int64Value{
										Int64Value: 10,
									},
								},
							},
						},
					},
				},
				{
					OperationName: "OpenCensus Reported Metrics",
					Labels: map[string]string{
						"common-label": "common-label-value",
						"label1":       "label1-value2",
						"label2":       "label2-value1",
					},
					MetricValueSets: []*scpb.MetricValueSet{
						{
							MetricName: "testservice.com/request_count",
							MetricValues: []*scpb.MetricValue{
								{
									StartTime: &timestamppb.Timestamp{
										Seconds: 1567509370,
									},
									EndTime: &timestamppb.Timestamp{
										Seconds: 1567509371,
									},
									Value: &scpb.MetricValue_Int64Value{
										Int64Value: 13,
									},
								},
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewOperationBuilder(tt.operationTags, map[string]string{"common-label": "common-label-value"})
			for _, vd := range tt.viewData {
				if err := builder.Add(vd); err != nil {
					t.Fatal(err)
				}
			}
			ops := builder.Operations()
			if diff := cmp.Diff(ops, tt.want, protocmp.Transform()); diff != "" {
				t.Errorf("Operations differ, -got +want: %s", diff)
			}
		})
	}
}

type unsupportedMeasure struct{}

func (*unsupportedMeasure) Name() string        { return "unsupported" }
func (*unsupportedMeasure) Description() string { return "unsupported" }
func (*unsupportedMeasure) Unit() string        { return "unsupported" }

func TestAddErrors(t *testing.T) {
	latencyView := &view.View{
		Name:        "testservice.com/latency",
		TagKeys:     []tag.Key{},
		Measure:     stats.Int64("testservice.com/latency", "description", stats.UnitMilliseconds),
		Aggregation: view.Sum(),
	}

	countAggregationView := &view.View{
		Name:        "testservice.com/count_aggregation",
		TagKeys:     []tag.Key{},
		Measure:     stats.Int64("testservice.com/count_aggregation", "description", stats.UnitMilliseconds),
		Aggregation: view.Count(),
	}

	unsupportedMeasureView := &view.View{
		Name:        "testservice.com/unsupported_measure",
		TagKeys:     []tag.Key{},
		Measure:     &unsupportedMeasure{},
		Aggregation: view.LastValue(),
	}

	start := time.Now()
	builder := NewOperationBuilder([]tag.Key{}, map[string]string{"common-label": "common-label-value"})

	tests := []struct {
		viewData *view.Data
		want     string
	}{
		{
			viewData: &view.Data{
				View: latencyView,
				Rows: []*view.Row{
					{
						Data: &view.SumData{Value: 10},
					},
				},
				Start: time.Unix(math.MinInt64, math.MinInt32).UTC(),
				End:   start.Add(time.Second),
			},
			want: "invalid metric start time",
		},
		{
			viewData: &view.Data{
				View: latencyView,
				Rows: []*view.Row{
					{
						Data: &view.SumData{Value: 10},
					},
				},
				Start: start,
				End:   time.Unix(math.MinInt64, math.MinInt32).UTC(),
			},
			want: "invalid metric end time",
		},
		{
			viewData: &view.Data{
				View: countAggregationView,
				Rows: []*view.Row{
					{
						Data: &view.CountData{Value: 10},
					},
				},
				Start: start,
				End:   start.Add(time.Second),
			},
			want: "unsupported aggregation type: Count",
		},
		{
			viewData: &view.Data{
				View: latencyView,
				Rows: []*view.Row{
					{
						Data: &view.DistributionData{
							Count:          3,
							Min:            0,
							Max:            10,
							CountPerBucket: []int64{0, 5, 10},
						},
					},
				},
				Start: start,
				End:   start.Add(time.Second),
			},
			want: "unsupported aggregation data type: *view.DistributionData",
		},
		{
			viewData: &view.Data{
				View: unsupportedMeasureView,
				Rows: []*view.Row{
					{
						Data: &view.SumData{Value: 10},
					},
				},
				Start: start,
				End:   start.Add(time.Second),
			},
			want: "unsupported measure type: *servicecontrol.unsupportedMeasure",
		},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			err := builder.Add(tt.viewData)
			if err == nil {
				t.Fatalf("error expected. got nil want %v", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("wrong error, got: %v, want substring: %v", err, tt.want)
			}
			if ops := builder.Operations(); len(ops) != 0 {
				t.Fatalf("unexpected operations, got: %v, want none", ops)
			}
		})
	}
}
