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
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/test/bufconn"

	timestamppb "github.com/golang/protobuf/ptypes/timestamp"
	scpb "google.golang.org/genproto/googleapis/api/servicecontrol/v1"
)

func TestNewExporterValidationErrors(t *testing.T) {
	tests := []struct {
		service  string
		location string
		want     string
	}{
		{
			service:  "   ",
			location: "us-west",
		},
		{
			service:  "test.googleapis.com",
			location: "  ",
		},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			_, err := NewExporter(
				tt.service,
				tt.location,
				Options{
					ConsumerProjectID: "test-consumer-project",
					ServiceConfigID:   "first_config",
				})
			if err == nil {
				t.Errorf("got no error, want error. Test: %v", tt)
			}
		})
	}
}

func TestExportView(t *testing.T) {
	start, err := time.Parse(time.RFC3339, "2019-09-03T11:16:10Z")
	if err != nil {
		t.Fatalf("Cannot set the start time: %v", err)
	}
	labelTag1 := tag.MustNewKey("label1")
	labelTag2 := tag.MustNewKey("label2")

	testWrapper(t, Options{
		ConsumerProjectID: "test-consumer-project",
		ServiceConfigID:   "first_config",
		UIDLabel:          "test-instance",
	}, func(f *fixture, e *Exporter) {
		e.ExportView(&view.Data{
			View: requestCountView([]tag.Key{labelTag1, labelTag2}),
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
		})
		req, timeout := f.waitForReportRequest()
		if timeout {
			t.Fatalf("wait for report request timeout got: %v, want: false", timeout)
		}

		want := &scpb.ReportRequest{
			ServiceName:     "testservice.googleapis.com",
			ServiceConfigId: "first_config",
			Operations: []*scpb.Operation{
				{
					OperationName: "OpenCensus Reported Metrics",
					ConsumerId:    "project:test-consumer-project",
					Labels: map[string]string{
						"cloud.googleapis.com/location": "us-west",
						"cloud.googleapis.com/uid":      "test-instance",
					},
					MetricValueSets: []*scpb.MetricValueSet{
						{
							MetricName: "testservice.googleapis.com/request_count",
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
		}

		ingoreOperationFields := cmpopts.IgnoreFields(scpb.Operation{}, "StartTime", "EndTime", "OperationId")
		if diff := cmp.Diff(req, want, ingoreOperationFields); diff != "" {
			t.Errorf("Request diff (want -> got): %s", diff)
		}
		for _, op := range req.GetOperations() {
			if diff := cmp.Diff(op.StartTime, op.EndTime); diff != "" {
				t.Errorf("start_time should equal end_time (want -> got): %s", diff)
			}
			if op.OperationId == "" {
				t.Errorf("invalid operation id, got: %v, want not empty", op.OperationId)
			}
		}
	})
}

func TestAutoUidLabel(t *testing.T) {
	// Force the uuid generator to always return "00000000-0000-4000-8000-000000000000".
	uuid.SetRand(zeroReader{})
	defer uuid.SetRand(nil)

	testWrapper(t, Options{}, func(f *fixture, e *Exporter) {
		viewData := &view.Data{
			View: requestCountView([]tag.Key{}),
			Rows: []*view.Row{
				{
					Tags: []tag.Tag{},
					Data: &view.SumData{Value: 10},
				},
			},
			Start: time.Now(),
			End:   time.Now().Add(time.Second),
		}
		e.ExportView(viewData)

		req, timeout := f.waitForReportRequest()
		if timeout {
			t.Fatalf("wait for report request timeout got: %v, want: false", timeout)
		}
		if len(req.Operations) != 1 {
			t.Fatalf("invalid operation count got: %v want: 1", req.Operations)
		}
		wantUID := "00000000-0000-4000-8000-000000000000"
		uid := req.Operations[0].Labels["cloud.googleapis.com/uid"]
		if uid != wantUID {
			t.Fatalf("invalid cloud.googleapis.com/uid label got: %v want: %v", uid, wantUID)
		}
	})
}

func TestExportViewDataWithNoRows(t *testing.T) {
	testWrapper(t, Options{}, func(f *fixture, e *Exporter) {
		e.ExportView(&view.Data{
			View: requestCountView([]tag.Key{}),
			// empty rows shall cause the error below
			Rows:  []*view.Row{},
			Start: time.Now(),
			End:   time.Now().Add(time.Second),
		})

		req, timeout := f.waitForReportRequest()
		if !timeout {
			t.Fatalf("wait for report request timeout got: %v, want: true", timeout)
		}
		if req != nil {
			t.Errorf("Unexpected request got: %v, want: nil", req)
		}
	})
}

func TestExportDataInvalidViewDataError(t *testing.T) {
	errors := make(chan error)
	testWrapper(t, Options{
		OnError: func(err error) {
			errors <- err
		},
	}, func(f *fixture, e *Exporter) {
		e.ExportView(&view.Data{
			View: requestCountView([]tag.Key{}),
			Rows: []*view.Row{
				{
					Tags: []tag.Tag{},
					// CountData is not supported and it shall trigger the error below
					Data: &view.CountData{Value: 100},
				},
			},
			Start: time.Now(),
			End:   time.Now().Add(time.Second),
		})

		if err := waitForError(errors); err == nil {
			t.Fatal("got no error, want error")
		}
	})
}

func TestReportResponseReportErrors(t *testing.T) {
	errors := make(chan error)
	testWrapper(t, Options{
		OnError: func(err error) {
			errors <- err
		},
	}, func(f *fixture, e *Exporter) {
		// Program the fake ServiceController to return ReportError-s
		f.server.errors = []*scpb.ReportResponse_ReportError{
			{
				OperationId: "op_id",
			},
		}

		e.ExportView(&view.Data{
			View: requestCountView([]tag.Key{}),
			Rows: []*view.Row{
				{
					Tags: []tag.Tag{},
					Data: &view.SumData{Value: 10},
				},
			},
			Start: time.Now(),
			End:   time.Now().Add(time.Second),
		})

		if err := waitForError(errors); err == nil {
			t.Fatal("got no error, want error")
		}
	})
}

func TestReportGrpcError(t *testing.T) {
	errors := make(chan error)
	testWrapper(t, Options{
		OnError: func(err error) {
			errors <- err
		},
	}, func(f *fixture, e *Exporter) {
		// Stop the gRpc server in order to induce the Canceled error below
		f.stop()

		e.ExportView(&view.Data{
			View: requestCountView([]tag.Key{}),
			Rows: []*view.Row{
				{
					Tags: []tag.Tag{},
					Data: &view.SumData{Value: 10},
				},
			},
			Start: time.Now(),
			End:   time.Now().Add(time.Second),
		})

		err := waitForError(errors)
		if err == nil {
			t.Fatal("got no error, want error")
		}
		if code := grpc.Code(err); code != codes.Canceled {
			t.Errorf("error = %v (gRPC code %s), want gRPC code %s", err, code, codes.Canceled)
		}
	})
}

type fakeServiceControllerServer struct {
	requests chan *scpb.ReportRequest
	errors   []*scpb.ReportResponse_ReportError
}

func (*fakeServiceControllerServer) Check(context.Context, *scpb.CheckRequest) (*scpb.CheckResponse, error) {
	return nil, errors.New("check not implemtented")
}

func (s *fakeServiceControllerServer) Report(ctx context.Context, req *scpb.ReportRequest) (*scpb.ReportResponse, error) {
	s.requests <- req
	return &scpb.ReportResponse{ReportErrors: s.errors}, nil
}

func newFakeServiceControllerServer() *fakeServiceControllerServer {
	return &fakeServiceControllerServer{requests: make(chan *scpb.ReportRequest, 10)}
}

func startFakeServiceControllerServerAndConnect(s *fakeServiceControllerServer) (*grpc.ClientConn, func(), error) {
	srvConn := bufconn.Listen(4096)
	grpcServer := grpc.NewServer()
	scpb.RegisterServiceControllerServer(grpcServer, s)
	go grpcServer.Serve(srvConn)

	conn, err := grpc.Dial("in-process", grpc.WithInsecure(), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return srvConn.Dial()
	}))
	if err != nil {
		srvConn.Close()
		grpcServer.GracefulStop()
		return nil, nil, fmt.Errorf("failed to connect to test server: %v", err)
	}
	return conn, func() {
		srvConn.Close()
		conn.Close()
		grpcServer.GracefulStop()
	}, nil
}

type fixture struct {
	server     *fakeServiceControllerServer
	clientConn *grpc.ClientConn
	stop       func()
}

func newFixture() (*fixture, error) {
	f := &fixture{server: newFakeServiceControllerServer()}
	var err error
	if f.clientConn, f.stop, err = startFakeServiceControllerServerAndConnect(f.server); err != nil {
		return nil, err
	}
	return f, nil
}

func (f *fixture) waitForReportRequest() (req *scpb.ReportRequest, timeout bool) {
	select {
	case req := <-f.server.requests:
		return req, false
	case <-time.After(3 * time.Second):
		return nil, true
	}
}

func testWrapper(t *testing.T, o Options, test func(*fixture, *Exporter)) {
	t.Helper()
	f, err := newFixture()
	if err != nil {
		t.Fatal(err)
	}
	defer f.stop()
	o.ServiceControlClientConn = f.clientConn
	e, err := NewExporter(
		"testservice.googleapis.com",
		"us-west",
		o)
	if err != nil {
		t.Fatal(err)
	}
	test(f, e)
}

func waitForError(ec chan error) error {
	select {
	case err := <-ec:
		return err
	case <-time.After(3 * time.Second):
		return errors.New("timeout waiting for an error")
	}
}

func requestCountView(tags []tag.Key) *view.View {
	return &view.View{
		Name:        "testservice.googleapis.com/request_count",
		TagKeys:     tags,
		Measure:     stats.Int64("testservice.googleapis.com/request_count", "description", stats.UnitDimensionless),
		Aggregation: view.Sum(),
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}
