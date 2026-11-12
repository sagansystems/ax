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

package tunnel

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSanitizeContext(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"my-cluster", "my-cluster"},
		{"gke_project_us-central1_cluster", "gke_project_us-central1_cluster"},
		{"arn:aws:eks:us-west-2:123456789:cluster/prod", "arn_aws_eks_us-west-2_123456789_cluster_prod"},
		{"", "default"},
	}

	for _, tt := range tests {
		got := SanitizeContext(tt.input)
		if got != tt.want {
			t.Errorf("SanitizeContext(%q) = %q; want %q", tt.input, got, tt.want)
		}
	}
}

func TestPortForwardRegex(t *testing.T) {
	cases := []struct {
		log  string
		port string
	}{
		{"Forwarding from 127.0.0.1:54947 -> 8080\n", "54947"},
		{"Forwarding from [::1]:61234 -> 8080\n", "61234"},
		{"Some prelude\nForwarding from 127.0.0.1:8080 -> 8080\n", "8080"},
	}

	for _, c := range cases {
		match := portForwardRegex.FindStringSubmatch(c.log)
		if len(match) < 2 || match[1] != c.port {
			t.Errorf("expected port %q from %q, got %v", c.port, c.log, match)
		}
	}
}

// TestPortForwardWaitsForSlowKubectl reproduces kubectl taking longer than the
// old 5s (EnsureServerURL) and 8s (PortForward) limits to print its listen line,
// as it does when an exec credential plugin such as tsh is slow.
func TestPortForwardWaitsForSlowKubectl(t *testing.T) {
	if testing.Short() {
		t.Skip("waits 9s for a fake kubectl")
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	useFakeKubectl(t, fmt.Sprintf("sleep 9\necho 'Forwarding from 127.0.0.1:%s -> 8080'\nexec sleep 60", u.Port()))
	const kubeContext = "slow-kubectl"

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		start := time.Now()
		got, err := EnsureServerURL(Options{Context: kubeContext})
		if err != nil {
			t.Errorf("EnsureServerURL after %s: %v", time.Since(start), err)
			return
		}
		defer func() { _ = StopTunnelByContext(kubeContext) }()
		if want := "http://127.0.0.1:" + u.Port(); got != want {
			t.Errorf("EnsureServerURL = %q, want %q", got, want)
		}
	}()
	go func() {
		defer wg.Done()
		start := time.Now()
		port, cleanup, err := PortForward(context.Background(), kubeContext, "ate-system", "svc/atenet-router", 443)
		if err != nil {
			t.Errorf("PortForward after %s: %v", time.Since(start), err)
			return
		}
		defer cleanup()
		if fmt.Sprint(port) != u.Port() {
			t.Errorf("PortForward port = %d, want %s", port, u.Port())
		}
	}()
	wg.Wait()
}

// TestPortForwardFailsFastWhenKubectlExits checks that an exited kubectl is
// reported at once rather than after portForwardReadyTimeout.
func TestPortForwardFailsFastWhenKubectlExits(t *testing.T) {
	useFakeKubectl(t, "echo 'error: pods is forbidden' >&2\nexit 1")
	const kubeContext = "failing-kubectl"
	const limit = 10 * time.Second

	start := time.Now()
	_, err := EnsureServerURL(Options{Context: kubeContext})
	if err == nil || !strings.Contains(err.Error(), "forbidden") {
		t.Errorf("EnsureServerURL error = %v, want kubectl's forbidden error", err)
	}
	if d := time.Since(start); d > limit {
		t.Errorf("EnsureServerURL took %s to report kubectl's exit, want under %s", d, limit)
	}

	start = time.Now()
	_, _, err = PortForward(context.Background(), kubeContext, "ate-system", "svc/atenet-router", 443)
	if err == nil || !strings.Contains(err.Error(), "forbidden") {
		t.Errorf("PortForward error = %v, want kubectl's forbidden error", err)
	}
	if d := time.Since(start); d > limit {
		t.Errorf("PortForward took %s to report kubectl's exit, want under %s", d, limit)
	}
}

// useFakeKubectl puts a kubectl running body on PATH and isolates AX_HOME.
func useFakeKubectl(t *testing.T, body string) {
	t.Helper()
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "kubectl"), []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("AX_HOME", t.TempDir())
	t.Setenv("AX_SERVER", "")
}
