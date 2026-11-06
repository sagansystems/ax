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

package postgres

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/google/ax/internal/controller"
	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/substrate/substratetest"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// tcpProxy forwards connections to target until it is closed.
type tcpProxy struct {
	ln     net.Listener
	target string
	mu     sync.Mutex
	conns  []net.Conn
}

func startProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	p := &tcpProxy{ln: ln, target: target}
	go p.serve()
	t.Cleanup(p.Close)
	return p
}

func (p *tcpProxy) serve() {
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		upstream, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = client.Close()
			continue
		}
		p.mu.Lock()
		p.conns = append(p.conns, client, upstream)
		p.mu.Unlock()
		go func() { _, _ = io.Copy(upstream, client); _ = upstream.Close() }()
		go func() { _, _ = io.Copy(client, upstream); _ = client.Close() }()
	}
}

// Close stops accepting connections and cuts every open one.
func (p *tcpProxy) Close() {
	_ = p.ln.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
}

func TestReadyzFailsWhenDatabaseUnreachable(t *testing.T) {
	u, err := url.Parse(testURL(t))
	if err != nil || u.Host == "" {
		t.Skipf("AX_TEST_POSTGRES_URL is not a URL with a host: %v", err)
	}
	proxy := startProxy(t, u.Host)
	u.Host = proxy.ln.Addr().String()

	s, err := Open(context.Background(), u.String(), Options{})
	if err != nil {
		t.Fatalf("Open through proxy: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	sub := substratetest.NewServer(t)
	srv := server.NewServer(s, controller.NewTaskReconciler(sub.Client(), "test-template", v1alpha1.DefaultSystemNamespace, ""), server.Options{
		Locker:       s,
		Ready:        s.Ping,
		ReadyTimeout: 2 * time.Second,
	})
	readyz := func() int {
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		return rec.Code
	}

	if got := readyz(); got != http.StatusOK {
		t.Fatalf("GET /readyz while PostgreSQL is reachable = %d, want 200", got)
	}

	proxy.Close()

	start := time.Now()
	if got := readyz(); got != http.StatusServiceUnavailable {
		t.Fatalf("GET /readyz after PostgreSQL became unreachable = %d, want 503", got)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("GET /readyz took %v, want it bounded by ReadyTimeout", elapsed)
	}
}
