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
	"fmt"
	"sort"
	"strings"

	"github.com/golang/protobuf/ptypes"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"

	timestamppb "github.com/golang/protobuf/ptypes/timestamp"
	scpb "google.golang.org/genproto/googleapis/api/servicecontrol/v1"
)

// OperationBuilder knows how to build the Operation protos to be passed in one ReportRequest.
type OperationBuilder struct {
	// Common labels and their values that should serialized into the
	// Operation.labels field in all the produced Operations. This map should be
	// populated with the labels that have constant values during the process
	// lifetime.
	commonLabelValues map[string]string

	// The set of tags that should be serialized into the Operation.labels field.
	// These should include the tags associated with monitored resource labels.
	// Any tag not in this set will be serialized into the
	// Operation.metric_value_sets.metric_values.labels field.
	// If nil, all tag will be serialized into the Operation.labels.
	commonTagKeyNames map[string]bool

	// The key is the "signature" of the operation tags (operationInstanceBuilder.operationTags)
	operationInstanceBuilderByTagSignature map[string]*operationInstanceBuilder
}

type metricKind int

const (
	metricKindCumulative metricKind = iota
	metricKindGauge
)

// NewOperationBuilder returns an OperationBuilder instance.
func NewOperationBuilder(operationTagKey []tag.Key, labelValues map[string]string) *OperationBuilder {
	var commonTagKeyNames map[string]bool = nil
	if operationTagKey != nil {
		commonTagKeyNames = make(map[string]bool)
		for _, k := range operationTagKey {
			commonTagKeyNames[k.Name()] = true
		}
	}
	return &OperationBuilder{
		commonLabelValues:                      labelValues,
		commonTagKeyNames:                      commonTagKeyNames,
		operationInstanceBuilderByTagSignature: make(map[string]*operationInstanceBuilder),
	}
}

// Add inserts the OpenCensus metric data to be sent to ServiceController.
func (b *OperationBuilder) Add(vd *view.Data) error {
	var kind metricKind
	switch vd.View.Aggregation.Type {
	case view.AggTypeLastValue:
		kind = metricKindGauge
	case view.AggTypeSum:
		kind = metricKindCumulative
	default:
		return fmt.Errorf("unsupported aggregation type: %s", vd.View.Aggregation.Type)
	}

	for _, r := range vd.Rows {
		var operationTags []tag.Tag
		var valueTags []tag.Tag
		for _, t := range r.Tags {
			// If the commonTagNames are not specified, convert any tag to an
			// Operation label.
			if b.commonTagKeyNames == nil || b.commonTagKeyNames[t.Key.Name()] {
				operationTags = append(operationTags, t)
			} else {
				valueTags = append(valueTags, t)
			}
		}
		sig := tagSignature(operationTags)
		oib, existing := b.operationInstanceBuilderByTagSignature[sig]
		if !existing {
			oib = newOperationInstanceBuilder(operationTags)
		}
		if err := oib.add(vd, valueTags, r.Data, kind); err != nil {
			return err
		}
		if !existing {
			b.operationInstanceBuilderByTagSignature[sig] = oib
		}
	}
	return nil
}

// Operations returns the Operation protos generated based on the view.Data passed through the Add method.
func (b *OperationBuilder) Operations() []*scpb.Operation {
	// Sort the operations by the tag singature for consistency.
	var keys []string
	for k := range b.operationInstanceBuilderByTagSignature {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	ops := make([]*scpb.Operation, len(keys))
	for i, k := range keys {
		ops[i] = b.operationInstanceBuilderByTagSignature[k].operation(b.commonLabelValues)
	}
	return ops
}

// operationInstanceBuilder contains metric values and labels that will go into one Operation proto.
type operationInstanceBuilder struct {
	// The tags to be serialized as operation level labels.
	operationTags []tag.Tag

	// Map from the metric type (aka metric name) to the list of its points.
	metricValuesByType map[string][]*scpb.MetricValue
}

func newOperationInstanceBuilder(operationTags []tag.Tag) *operationInstanceBuilder {
	return &operationInstanceBuilder{
		operationTags:      operationTags,
		metricValuesByType: make(map[string][]*scpb.MetricValue),
	}
}

func (b *operationInstanceBuilder) operation(commonLabelValues map[string]string) *scpb.Operation {
	op := &scpb.Operation{
		// This should not change. Chemist aggregation uses this name to build
		// the aggregation key:
		OperationName: "OpenCensus Reported Metrics",
		Labels:        tagsToLabels(b.operationTags),
	}
	for k, v := range commonLabelValues {
		op.Labels[k] = v
	}
	for metricType, values := range b.metricValuesByType {
		op.MetricValueSets = append(op.MetricValueSets, &scpb.MetricValueSet{
			MetricName:   metricType,
			MetricValues: values,
		})
	}
	return op
}

func (b *operationInstanceBuilder) add(viewData *view.Data, tags []tag.Tag, aggData view.AggregationData, kind metricKind) error {
	mv := &scpb.MetricValue{}
	var err error
	mv.StartTime, mv.EndTime, err = metricTimeInterval(viewData, kind)
	if err != nil {
		return err
	}
	if err := setMetricValue(aggData, viewData, mv); err != nil {
		return err
	}
	if len(tags) > 0 {
		mv.Labels = tagsToLabels(tags)
	}

	metricType := viewData.View.Name
	b.metricValuesByType[metricType] = append(b.metricValuesByType[metricType], mv)
	return nil
}

func metricTimeInterval(viewData *view.Data, kind metricKind) (start, end *timestamppb.Timestamp, err error) {
	end, err = ptypes.TimestampProto(viewData.End)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid metric end time: %v", err)
	}
	if kind == metricKindGauge {
		return end, end, nil
	}
	start, err = ptypes.TimestampProto(viewData.Start)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid metric start time: %v", err)
	}
	return
}

func tagsToLabels(tags []tag.Tag) map[string]string {
	labels := make(map[string]string, len(tags))
	for _, tag := range tags {
		labels[tag.Key.Name()] = tag.Value
	}
	return labels
}

func tagSignature(tags []tag.Tag) string {
	var builder strings.Builder
	// This assumes that the tags order is consistent. OpenCensus should sort them
	// according to this comment:
	// https://github.com/census-instrumentation/opencensus-go/blob/b4a14686f0a98096416fe1b4cb848e384fb2b22b/stats/view/view.go#L191-L194
	for _, t := range tags {
		builder.WriteString(t.Key.Name())
		builder.WriteString(t.Value)
	}
	return builder.String()
}

func setMetricValue(aggData view.AggregationData, viewData *view.Data, mv *scpb.MetricValue) error {
	switch v := aggData.(type) {
	case *view.LastValueData:
		return setScalarMetricValue(viewData.View.Measure, v.Value, mv)
	case *view.SumData:
		return setScalarMetricValue(viewData.View.Measure, v.Value, mv)
	// TODO(b/146061426): Support the case *view.AggregationData
	default:
		return fmt.Errorf("unsupported aggregation data type: %T", v)
	}
}

func setScalarMetricValue(measure stats.Measure, value float64, mv *scpb.MetricValue) error {
	switch measure.(type) {
	case *stats.Int64Measure:
		mv.Value = &scpb.MetricValue_Int64Value{
			Int64Value: int64(value),
		}
	case *stats.Float64Measure:
		mv.Value = &scpb.MetricValue_DoubleValue{
			DoubleValue: value,
		}
	default:
		return fmt.Errorf("unsupported measure type: %T", measure)
	}
	return nil
}
