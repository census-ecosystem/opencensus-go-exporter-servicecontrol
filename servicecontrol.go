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

/*
Package servicecontrol implements an OpenCensus exporter for the metrics associated
with GCP API Service metrics (using the ServiceController.Report API).

  * The metrics should be defined in the API service configuration and declared
    under the producer or consumer destinations ([1]).
  * The metrics should be defined as OpenCensus measures (`stats.Measure`)
  * The metric labels and monitored resource labels should be declared as
    OpenCensus tags (`tag.NewKey`) and associaded with the metrics using the
    OpenCensus View instances.

[1] see https://cloud.google.com/service-infrastructure/docs/service-management/reference/rpc/google.api#monitoring
*/
package servicecontrol

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/golang/glog"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/uuid"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"google.golang.org/api/support/bundler"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"

	scpb "google.golang.org/genproto/googleapis/api/servicecontrol/v1"
)

// Options contains the optional configuration parameters for the
// ServiceController exporter (see NewExporter below).
type Options struct {
	// ConsumerProjectID identifies the GCP project of the service consumer.
	// If not set, the client shall ensure that all the reported metrics are
	// producer metrics (are declared in the monitoring/producer_destinations
	// field in the service configuration
	// (see https://cloud.google.com/service-infrastructure/docs/service-management/reference/rpc/google.api#monitoring)
	ConsumerProjectID string

	// ServiceConfigID Specifies which version of service config should be used to
	// process the request. If unspecified or no matching version can be found, the
	// latest one will be used.
	// This may be set to the value of the google.api.Service.id field reported by
	// the Service Management API.
	// (see https://cloud.google.com/service-infrastructure/docs/service-management/reference/rest/v1/services.configs?hl=ca#Service)
	ServiceConfigID string

	// Labels and their values that will be reported to the ServiceController on each
	// request.
	// These should be used to set the labels values that are fixed during the program
	// execution. Examples: 'cloud.googleapis.com/location' or 'cloud.googleapis.com/uid'.
	CommonLabels map[string]string

	// These tags will be converted into Operation level labels. Any monitored
	// resource label to should be added to this list. It can also be used for
	// labels that are shared across metrics.
	OperationTagKeys []tag.Key

	// OnError is a hook to be called when there is an error uploading the metrics.
	// If no set, the errors are logged.
	OnError func(err error)

	// TODO(b/137286316): Add a "Options.Context context.Context" field similar to
	// the one in the OpenCensus Stackdriver Exporter.

	// serviceControlClientConn is used by tests to override the ServiceController
	// service endpoint.
	ServiceControlClientConn *grpc.ClientConn
}

// Exporter knows how to send metrics using the ServiceController API.
type Exporter struct {
	service string

	o Options

	scClient scpb.ServiceControllerClient

	viewDataBundler *bundler.Bundler
}

// NewExporter returns a new exporter instance that pushes the collected metrics
// to ServiceController.
//
// `service` is the name of the managed service as indicated in the
// service configuration (e.g. pubsub.googleapis.com).
func NewExporter(service string, o Options) (*Exporter, error) {
	if strings.TrimSpace(service) == "" {
		return nil, errors.New("service must not be empty")
	}
	e := &Exporter{
		service: service,
		o:       o,
	}
	conn, err := o.clientConn()
	if err != nil {
		return nil, err
	}
	e.scClient = scpb.NewServiceControllerClient(conn)
	e.viewDataBundler = bundler.NewBundler(&view.Data{}, func(bundle interface{}) {
		vds := bundle.([]*view.Data)
		e.handleViewData(vds...)
	})
	return e, nil
}

// ExportView queues the collected metric data to be sent to ServiceController.
func (e *Exporter) ExportView(vd *view.Data) {
	if len(vd.Rows) == 0 {
		return
	}
	if err := e.viewDataBundler.Add(vd, 1); err != nil {
		e.o.handleError(fmt.Errorf("cannot bundle view.Data: %v", err))
	}
}

func (e *Exporter) handleViewData(vds ...*view.Data) {
	opbuilder := NewOperationBuilder(e.o.OperationTagKeys, e.o.CommonLabels)
	for _, vd := range vds {
		if err := opbuilder.Add(vd); err != nil {
			e.o.handleError(fmt.Errorf("cannot add view.Data %v: %w", vd, err))
		}
	}

	ops := opbuilder.Operations()
	if len(ops) == 0 {
		return
	}

	now := ptypes.TimestampNow()
	for _, op := range ops {
		if e.o.ConsumerProjectID != "" {
			op.ConsumerId = "project:" + e.o.ConsumerProjectID
		}
		op.OperationId = uuid.New().String()
		op.StartTime = now
		op.EndTime = now
	}
	req := &scpb.ReportRequest{
		ServiceName:     e.service,
		Operations:      ops,
		ServiceConfigId: e.o.ServiceConfigID,
	}

	// context.Background() is used because this RPC is issued when the OpenCensus
	// decides to flush the metric data. This is done asynchronously from the
	// client code (e.g. a gRpc service method implementation that reports
	// measurements to be exported to ServiceController).
	// TODO(b/137286316): Allow the client to override this Context.
	resp, err := e.scClient.Report(context.Background(), req)
	if err != nil {
		e.o.handleError(err)
		return
	}
	if len(resp.ReportErrors) != 0 {
		e.o.handleError(fmt.Errorf("ReportResponse contains errors: %v", resp))
	}
}

func (o Options) handleError(err error) {
	if o.OnError != nil {
		o.OnError(err)
		return
	}
	glog.Errorf("failed to export to ServiceController: %v", err)
}

func (o Options) clientConn() (*grpc.ClientConn, error) {
	if o.ServiceControlClientConn != nil {
		return o.ServiceControlClientConn, nil
	}
	// TODO(b/137286316): Allow the client to override this Context.
	// In most cases the exporter is created during the service statup so the
	// context.Background() may be sufficient.
	perRPC, err := oauth.NewApplicationDefault(context.Background(), "https://www.googleapis.com/auth/cloud-platform")
	if err != nil {
		return nil, fmt.Errorf("failed to create credentials: %w", err)
	}
	return grpc.Dial("servicecontrol.googleapis.com:443", grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, "")), grpc.WithPerRPCCredentials(perRPC))
}
