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

package controller

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/google/ax/pkg/apis/v1alpha1"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/encoding/protojson"
	yamlv3 "gopkg.in/yaml.v3"
	"sigs.k8s.io/yaml"
)

const (
	// egressPolicyConfigMap is read from the namespace named after the task's atespace.
	egressPolicyConfigMap = "ax-egress-policy"
	// condEgressPolicy reports whether the actor's egress policy matches ax-egress-policy.
	condEgressPolicy = "EgressPolicyApplied"
	// configMapLookupTimeout bounds the ConfigMap read like secretLookupTimeout.
	configMapLookupTimeout = 5 * time.Second
)

// ErrConfigMapNotFound means the atespace has no egress policy ConfigMap.
var ErrConfigMapNotFound = errors.New("configmap not found")

// ConfigMapResolver returns the data of a Kubernetes ConfigMap.
type ConfigMapResolver func(ctx context.Context, namespace, name string) (map[string]string, error)

// reconcileEgressPolicy reports failures on the task's condition, never as a reconcile error.
func (r *TaskReconciler) reconcileEgressPolicy(ctx context.Context, task *v1alpha1.Task, atespace, actorName string) {
	if r.ConfigMapResolver == nil {
		return
	}
	lookupCtx, cancel := context.WithTimeout(ctx, configMapLookupTimeout)
	data, err := r.ConfigMapResolver(lookupCtx, atespace, egressPolicyConfigMap)
	cancel()
	if errors.Is(err, ErrConfigMapNotFound) {
		r.removeCondition(task, condEgressPolicy)
		return
	}
	if err != nil {
		slog.Warn("could not read egress policy configmap", "atespace", atespace, "error", err)
		r.setCondition(task, condEgressPolicy, "False", "ConfigMapUnavailable", err.Error(), time.Now())
		return
	}

	rules, err := egressRulesFromConfigMap(data)
	if err != nil {
		slog.Warn("invalid egress policy configmap, leaving the actor's policy unchanged", "atespace", atespace, "error", err)
		r.setCondition(task, condEgressPolicy, "False", "InvalidEgressPolicy", err.Error(), time.Now())
		return
	}

	changed, err := r.client.EnsureActorEgressPolicy(ctx, atespace, actorName, rules)
	if err != nil {
		slog.Warn("could not apply egress policy", "atespace", atespace, "actor", actorName, "error", err)
		r.setCondition(task, condEgressPolicy, "False", "EgressPolicyApplyFailed", err.Error(), time.Now())
		return
	}
	if changed {
		slog.Info("applied egress policy to actor", "atespace", atespace, "actor", actorName, "rules", len(rules))
	}
	r.setCondition(task, condEgressPolicy, "True", "EgressPolicyApplied",
		fmt.Sprintf("Actor egress policy matches ConfigMap %s/%s", atespace, egressPolicyConfigMap), time.Now())
}

// egressRulesFromConfigMap merges the rules of every *.yaml key in key order; metadata is ignored.
func egressRulesFromConfigMap(data map[string]string) ([]*ateapipb.EgressRule, error) {
	keys := make([]string, 0, len(data))
	for k := range data {
		if strings.HasSuffix(k, ".yaml") {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	var rules []*ateapipb.EgressRule
	for _, k := range keys {
		policy, err := egressPolicyFromYAML([]byte(data[k]))
		if err != nil {
			return nil, fmt.Errorf("key %q: %w", k, err)
		}
		rules = append(rules, policy.GetRules()...)
	}
	return rules, nil
}

// egressPolicyFromYAML mirrors kubectl-ate's egressPolicyFromManifest at
// Substrate 34bde58: exactly one document, strict protojson.
func egressPolicyFromYAML(data []byte) (*ateapipb.EgressPolicy, error) {
	dec := yamlv3.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&yamlv3.Node{}); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("manifest is empty")
		}
		return nil, fmt.Errorf("invalid YAML: %w", err)
	}
	if err := dec.Decode(&yamlv3.Node{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("manifest holds more than one document, expected one")
	}
	j, err := yaml.YAMLToJSON(data)
	if err != nil {
		return nil, fmt.Errorf("invalid YAML: %w", err)
	}
	if string(j) == "null" {
		return nil, errors.New("manifest is empty")
	}
	policy := &ateapipb.EgressPolicy{}
	if err := protojson.Unmarshal(j, policy); err != nil {
		return nil, fmt.Errorf("invalid EgressPolicy: %w", err)
	}
	return policy, nil
}

// GetKubernetesConfigMap treats 403 as not found: ax-server may read the ConfigMap only in onboarded namespaces.
func GetKubernetesConfigMap(ctx context.Context, namespace, name string) (map[string]string, error) {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	if host == "" {
		return nil, ErrConfigMapNotFound
	}
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	if port == "" {
		port = "443"
	}
	const saDir = "/var/run/secrets/kubernetes.io/serviceaccount/"
	token, err := os.ReadFile(saDir + "token")
	if err != nil {
		return nil, fmt.Errorf("reading service account token: %w", err)
	}
	pool := x509.NewCertPool()
	if ca, err := os.ReadFile(saDir + "ca.crt"); err == nil {
		pool.AppendCertsFromPEM(ca)
	}
	tr := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr}

	url := fmt.Sprintf("https://%s/api/v1/namespaces/%s/configmaps/%s", net.JoinHostPort(host, port), namespace, name)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("getting configmap %s/%s: %w", namespace, name, err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound, http.StatusForbidden:
		return nil, ErrConfigMapNotFound
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("getting configmap %s/%s: %s: %s", namespace, name, resp.Status, strings.TrimSpace(string(body)))
	}
	var cm struct {
		Data map[string]string `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&cm); err != nil {
		return nil, fmt.Errorf("decoding configmap %s/%s: %w", namespace, name, err)
	}
	return cm.Data, nil
}
