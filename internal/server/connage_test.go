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
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/substrate/substratetest"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// serveWithConnectionAge serves srv like ax-server does, with age applied,
// and returns its address and the client addresses that sent requests.
func serveWithConnectionAge(t *testing.T, age server.ConnectionAge) (string, func() map[string]bool) {
	t.Helper()
	srv := server.NewServer(memory.NewStore(), newTestReconciler(substratetest.NewServer(t)))

	var mu sync.Mutex
	clients := map[string]bool{}
	httpServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		clients[r.RemoteAddr] = true
		mu.Unlock()
		srv.Handler().ServeHTTP(w, r)
	})}
	httpServer.Protocols = new(http.Protocols)
	httpServer.Protocols.SetHTTP1(true)
	httpServer.Protocols.SetUnencryptedHTTP2(true)
	age.Apply(httpServer)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = httpServer.Serve(ln) }()
	t.Cleanup(func() { _ = httpServer.Close() })

	return ln.Addr().String(), func() map[string]bool {
		mu.Lock()
		defer mu.Unlock()
		out := map[string]bool{}
		for k := range clients {
			out[k] = true
		}
		return out
	}
}

func TestConnectionAge_ClientReconnectsAfterMaxAge(t *testing.T) {
	tests := []struct {
		name            string
		age             server.ConnectionAge
		wantReconnected bool
	}{
		{name: "disabled", age: server.ConnectionAge{}, wantReconnected: false},
		{name: "max age", age: server.ConnectionAge{MaxAge: 200 * time.Millisecond}, wantReconnected: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, clients := serveWithConnectionAge(t, tt.age)
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatalf("grpc.NewClient: %v", err)
			}
			defer conn.Close()
			client := v1alpha1.NewAXClient(conn)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			call := func() {
				t.Helper()
				if _, err := client.ListTasks(ctx, &v1alpha1.ListTasksRequest{}); err != nil {
					t.Fatalf("ListTasks: %v", err)
				}
			}

			call()
			time.Sleep(300 * time.Millisecond)
			// Past MaxAge: this call completes, and its response sends GOAWAY.
			call()
			time.Sleep(100 * time.Millisecond)
			call()

			got := len(clients())
			if tt.wantReconnected && got < 2 {
				t.Errorf("calls came from %d connection(s), want a new connection after MaxAge", got)
			}
			if !tt.wantReconnected && got != 1 {
				t.Errorf("calls came from %d connections, want 1", got)
			}
		})
	}
}

func TestConnectionAge_GraceClosesConnection(t *testing.T) {
	age := server.ConnectionAge{MaxAge: 100 * time.Millisecond, Grace: 100 * time.Millisecond}
	addr, _ := serveWithConnectionAge(t, age)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	start := time.Now()
	_ = conn.SetReadDeadline(start.Add(5 * time.Second))
	_, err = conn.Read(make([]byte, 1))
	elapsed := time.Since(start)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("Read = %v, want EOF when the server closes the connection", err)
	}
	if want := 90*time.Millisecond + age.Grace; elapsed < want {
		t.Errorf("connection closed after %v, want at least %v", elapsed, want)
	}
}
