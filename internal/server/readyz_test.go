// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/substrate/substratetest"
)

func TestServerReadyzHTTP(t *testing.T) {
	tests := []struct {
		name  string
		ready func(context.Context) error
		want  int
	}{
		{
			name: "no check",
			want: http.StatusOK,
		},
		{
			name:  "check passes",
			ready: func(context.Context) error { return nil },
			want:  http.StatusOK,
		},
		{
			name:  "check fails",
			ready: func(context.Context) error { return errors.New("connection refused") },
			want:  http.StatusServiceUnavailable,
		},
		{
			name: "check hangs past the timeout",
			ready: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
			want: http.StatusServiceUnavailable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := server.NewServer(memory.NewStore(), newTestReconciler(substratetest.NewServer(t)), server.Options{
				Ready:        tt.ready,
				ReadyTimeout: 50 * time.Millisecond,
			})

			done := make(chan *httptest.ResponseRecorder)
			go func() {
				rec := httptest.NewRecorder()
				srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
				done <- rec
			}()
			select {
			case rec := <-done:
				if rec.Code != tt.want {
					t.Errorf("GET /readyz = %d, want %d", rec.Code, tt.want)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("GET /readyz did not return; the readiness timeout was not applied")
			}

			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
			if rec.Code != http.StatusOK {
				t.Errorf("GET /healthz = %d, want 200 regardless of readiness", rec.Code)
			}
		})
	}
}
