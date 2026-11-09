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
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/ax/internal/controller"
	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/substrate/substratetest"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/protobuf/proto"
)

func TestServer_ResyncEgressPolicies_UpdatesRunningTasksOnly(t *testing.T) {
	st := memory.NewStore()
	reconciler := newTestReconciler(substratetest.NewServer(t))
	var configMapErr atomic.Pointer[error]
	var reads atomic.Int32
	reconciler.ConfigMapResolver = func(context.Context, string, string) (map[string]string, error) {
		if err := configMapErr.Load(); err != nil {
			reads.Add(1)
			return nil, *err
		}
		return nil, controller.ErrConfigMapNotFound
	}
	srv := server.NewServer(st, reconciler)
	ctx := context.Background()

	for _, name := range []string{"egress-running", "egress-suspended"} {
		if _, err := srv.CreateTask(ctx, &v1alpha1.CreateTaskRequest{Task: &v1alpha1.Task{
			Metadata: &v1alpha1.ObjectMeta{Name: name},
			Spec:     &v1alpha1.TaskSpec{Image: "alpine"},
		}}); err != nil {
			t.Fatalf("CreateTask %s: %v", name, err)
		}
		if _, err := srv.ResumeTask(ctx, &v1alpha1.ResumeTaskRequest{Name: name}); err != nil {
			t.Fatalf("ResumeTask %s: %v", name, err)
		}
	}
	if _, err := srv.SuspendTask(ctx, &v1alpha1.SuspendTaskRequest{Name: "egress-suspended"}); err != nil {
		t.Fatalf("SuspendTask: %v", err)
	}

	egressCondition := func(name string) *v1alpha1.Condition {
		t.Helper()
		task, err := st.GetTask(ctx, "default", name)
		if err != nil {
			t.Fatalf("GetTask %s: %v", name, err)
		}
		for _, c := range task.GetStatus().GetConditions() {
			if c.GetType() == "EgressPolicyApplied" {
				return c
			}
		}
		return nil
	}

	readErr := errors.New("connection refused")
	configMapErr.Store(&readErr)
	srv.ResyncEgressPoliciesOnce(ctx)

	if got := reads.Load(); got != 1 {
		t.Errorf("ConfigMap reads in one pass = %d, want 1 (the Running task only)", got)
	}
	first := egressCondition("egress-running")
	if first.GetStatus() != "False" || first.GetReason() != "ConfigMapUnavailable" {
		t.Fatalf("running task EgressPolicyApplied = %v, want False (ConfigMapUnavailable)", first)
	}
	if c := egressCondition("egress-suspended"); c != nil {
		t.Errorf("suspended task EgressPolicyApplied = %v, want none", c)
	}

	time.Sleep(10 * time.Millisecond)
	srv.ResyncEgressPoliciesOnce(ctx)
	if got := egressCondition("egress-running"); !proto.Equal(got, first) {
		t.Errorf("EgressPolicyApplied after an unchanged pass = %v, want it unchanged from %v", got, first)
	}
}
