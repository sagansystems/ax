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

package controller_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/ax/internal/controller"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type egressMockServer struct {
	*mockControlServer

	mu       sync.Mutex
	policies map[string]*ateapipb.EgressPolicy
	creates  []*ateapipb.CreateActorEgressPolicyRequest
	updates  []*ateapipb.UpdateActorEgressPolicyRequest
}

func policyKey(ref *ateapipb.ObjectRef) string { return ref.GetAtespace() + "/" + ref.GetName() }

func (m *egressMockServer) GetActorEgressPolicy(_ context.Context, req *ateapipb.GetActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.policies[policyKey(req.GetActor())]
	if !ok {
		return nil, status.Error(codes.NotFound, "no egress policy")
	}
	return proto.Clone(p).(*ateapipb.EgressPolicy), nil
}

func (m *egressMockServer) CreateActorEgressPolicy(_ context.Context, req *ateapipb.CreateActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.creates = append(m.creates, req)
	p := proto.Clone(req.GetEgressPolicy()).(*ateapipb.EgressPolicy)
	applyServerDefaults(p)
	p.Metadata.Uid = "uid-1"
	p.Metadata.Version = 1
	m.policies[policyKey(req.GetActor())] = p
	return p, nil
}

func (m *egressMockServer) UpdateActorEgressPolicy(_ context.Context, req *ateapipb.UpdateActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.updates = append(m.updates, req)
	cur := m.policies[policyKey(req.GetActor())]
	if cur == nil {
		return nil, status.Error(codes.NotFound, "no egress policy")
	}
	md := req.GetEgressPolicy().GetMetadata()
	if md.GetUid() != cur.GetMetadata().GetUid() || md.GetVersion() != cur.GetMetadata().GetVersion() {
		return nil, status.Error(codes.Aborted, "precondition mismatch")
	}
	p := proto.Clone(req.GetEgressPolicy()).(*ateapipb.EgressPolicy)
	applyServerDefaults(p)
	p.Metadata.Version = cur.GetMetadata().GetVersion() + 1
	m.policies[policyKey(req.GetActor())] = p
	return p, nil
}

// applyServerDefaults fills ports the way ate-api-server does on write.
func applyServerDefaults(p *ateapipb.EgressPolicy) {
	for _, r := range p.GetRules() {
		if r.GetHttps() != nil && r.Https.Ports == nil {
			r.Https.Ports = &ateapipb.Ports{Numbers: []int32{443}}
		}
		if r.GetHttp() != nil && r.Http.Ports == nil {
			r.Http.Ports = &ateapipb.Ports{Numbers: []int32{80}}
		}
	}
}

// fakeConfigMaps serves ConfigMaps keyed by namespace/name.
type fakeConfigMaps struct {
	data map[string]map[string]string
	err  error
}

func (f *fakeConfigMaps) resolve(_ context.Context, namespace, name string) (map[string]string, error) {
	if f.err != nil {
		return nil, f.err
	}
	d, ok := f.data[namespace+"/"+name]
	if !ok {
		return nil, controller.ErrConfigMapNotFound
	}
	return d, nil
}

func newEgressReconciler(t *testing.T, cms *fakeConfigMaps) (*controller.TaskReconciler, *egressMockServer) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	t.Cleanup(func() { lis.Close() })

	mockSrv := &egressMockServer{mockControlServer: &mockControlServer{}, policies: map[string]*ateapipb.EgressPolicy{}}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	t.Cleanup(grpcServer.Stop)

	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create substrate client: %v", err)
	}
	t.Cleanup(func() { client.Close() })

	r := controller.NewTaskReconciler(client, "test-template", v1alpha1.DefaultSystemNamespace)
	r.SecretResolver = noSecrets
	r.ConfigMapResolver = cms.resolve
	r.WorkspaceReadyTimeout = 50 * time.Millisecond
	return r, mockSrv
}

func egressTask(phase string) *v1alpha1.Task {
	return &v1alpha1.Task{
		ApiVersion: v1alpha1.APIVersion,
		Kind:       v1alpha1.KindTask,
		Metadata:   &v1alpha1.ObjectMeta{Name: "egress-task", Atespace: "credtest"},
		Spec:       &v1alpha1.TaskSpec{Image: "example.com/image"},
		Status:     &v1alpha1.TaskStatus{Phase: phase},
	}
}

const (
	baselineYAML = `rules:
- tlsPassthrough:
    hostnames: ["proxy.golang.org"]
    ports: {numbers: [443]}
`
	githubYAML = `metadata:
  name: ignored
rules:
- https:
    hostnames: ["api.github.com"]
`
)

func hostnames(rules []*ateapipb.EgressRule) []string {
	var out []string
	for _, r := range rules {
		out = append(out, r.GetTlsPassthrough().GetHostnames()...)
		out = append(out, r.GetHttps().GetHostnames()...)
		out = append(out, r.GetHttp().GetHostnames()...)
	}
	return out
}

func findCondition(task *v1alpha1.Task, condType string) *v1alpha1.Condition {
	for _, c := range task.GetStatus().GetConditions() {
		if c.GetType() == condType {
			return c
		}
	}
	return nil
}

func TestReconcileEgressPolicy_NoConfigMapCreatesNothing(t *testing.T) {
	r, srv := newEgressReconciler(t, &fakeConfigMaps{})

	task, err := r.Reconcile(context.Background(), egressTask(""))
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if len(srv.creates) != 0 || len(srv.updates) != 0 {
		t.Errorf("expected no egress policy writes, got %d creates and %d updates", len(srv.creates), len(srv.updates))
	}
	if c := findCondition(task, "EgressPolicyApplied"); c != nil {
		t.Errorf("expected no EgressPolicyApplied condition, got %v", c)
	}
}

func TestReconcileEgressPolicy_MergesYAMLKeysInOrder(t *testing.T) {
	cms := &fakeConfigMaps{data: map[string]map[string]string{
		"credtest/ax-egress-policy": {
			"github.yaml":   githubYAML,
			"baseline.yaml": baselineYAML,
			"README.md":     "not a policy",
		},
	}}
	r, srv := newEgressReconciler(t, cms)

	for _, phase := range []string{"", "Running"} {
		task, err := r.Reconcile(context.Background(), egressTask(phase))
		if err != nil {
			t.Fatalf("Reconcile(phase %q) failed: %v", phase, err)
		}
		assertCondition(t, task, "EgressPolicyApplied", "True", "EgressPolicyApplied")
	}

	if len(srv.creates) != 1 {
		t.Fatalf("expected 1 create, got %d", len(srv.creates))
	}
	if len(srv.updates) != 0 {
		t.Errorf("expected no update for an unchanged policy, got %d", len(srv.updates))
	}
	req := srv.creates[0]
	if got := policyKey(req.GetActor()); got != "credtest/egress-task" {
		t.Errorf("created policy for actor %q, want credtest/egress-task", got)
	}
	md := req.GetEgressPolicy().GetMetadata()
	if md.GetAtespace() != "credtest" || md.GetName() != "default" {
		t.Errorf("policy metadata = %v, want atespace credtest and name default", md)
	}
	got := hostnames(req.GetEgressPolicy().GetRules())
	want := []string{"proxy.golang.org", "api.github.com"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("rule hostnames = %v, want %v", got, want)
	}
}

func TestReconcileEgressPolicy_UpdatesChangedPolicy(t *testing.T) {
	cm := map[string]string{"policy.yaml": baselineYAML}
	cms := &fakeConfigMaps{data: map[string]map[string]string{"credtest/ax-egress-policy": cm}}
	r, srv := newEgressReconciler(t, cms)

	if _, err := r.Reconcile(context.Background(), egressTask("")); err != nil {
		t.Fatalf("first Reconcile failed: %v", err)
	}
	cm["policy.yaml"] = githubYAML
	task, err := r.Reconcile(context.Background(), egressTask(""))
	if err != nil {
		t.Fatalf("second Reconcile failed: %v", err)
	}
	assertCondition(t, task, "EgressPolicyApplied", "True", "EgressPolicyApplied")

	if len(srv.updates) != 1 {
		t.Fatalf("expected 1 update, got %d", len(srv.updates))
	}
	md := srv.updates[0].GetEgressPolicy().GetMetadata()
	if md.GetUid() != "uid-1" || md.GetVersion() != 1 {
		t.Errorf("update preconditions = uid %q version %d, want uid-1 and 1", md.GetUid(), md.GetVersion())
	}
	if got := hostnames(srv.policies["credtest/egress-task"].GetRules()); len(got) != 1 || got[0] != "api.github.com" {
		t.Errorf("stored policy hostnames = %v, want [api.github.com]", got)
	}
}

func TestReconcileEgressPolicy_InvalidYAMLLeavesPolicy(t *testing.T) {
	cm := map[string]string{"policy.yaml": baselineYAML}
	cms := &fakeConfigMaps{data: map[string]map[string]string{"credtest/ax-egress-policy": cm}}
	r, srv := newEgressReconciler(t, cms)

	if _, err := r.Reconcile(context.Background(), egressTask("")); err != nil {
		t.Fatalf("first Reconcile failed: %v", err)
	}
	before := proto.Clone(srv.policies["credtest/egress-task"])

	for name, value := range map[string]string{
		"unknown field": "rules:\n- bogus: {}\n",
		"bad yaml":      "rules: [\n",
		"two documents": baselineYAML + "---\n" + githubYAML,
	} {
		cm["policy.yaml"] = value
		task, err := r.Reconcile(context.Background(), egressTask(""))
		if err != nil {
			t.Fatalf("%s: Reconcile returned %v, want the task reconciled anyway", name, err)
		}
		assertCondition(t, task, "EgressPolicyApplied", "False", "InvalidEgressPolicy")
		if task.Status.Phase != "Suspended" {
			t.Errorf("%s: phase = %q, want Suspended", name, task.Status.Phase)
		}
	}
	if len(srv.updates) != 0 {
		t.Errorf("expected no update after invalid YAML, got %d", len(srv.updates))
	}
	if !proto.Equal(srv.policies["credtest/egress-task"], before) {
		t.Errorf("policy changed after invalid YAML: %v", srv.policies["credtest/egress-task"])
	}
}

func TestReconcileEgressPolicy_ConfigMapReadError(t *testing.T) {
	r, srv := newEgressReconciler(t, &fakeConfigMaps{err: errors.New("connection refused")})

	task, err := r.Reconcile(context.Background(), egressTask(""))
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	assertCondition(t, task, "EgressPolicyApplied", "False", "ConfigMapUnavailable")
	if len(srv.creates) != 0 {
		t.Errorf("expected no create, got %d", len(srv.creates))
	}
}
