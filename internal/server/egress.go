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

package server

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

const (
	egressResyncPageSize = 100
	// egressResyncTaskTimeout bounds the wait for a task's lock and the policy write.
	egressResyncTaskTimeout = 30 * time.Second
)

// ResyncEgressPolicies applies each atespace's ax-egress-policy ConfigMap to its Running tasks every interval until ctx is done.
func (s *Server) ResyncEgressPolicies(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		s.resyncEgressPolicies(ctx)
	}
}

func (s *Server) resyncEgressPolicies(ctx context.Context) {
	for offset := int64(0); ; offset += egressResyncPageSize {
		tasks, err := s.listTasks(ctx, "", egressResyncPageSize, offset)
		if err != nil {
			slog.Warn("egress policy resync: listing tasks", "error", err)
			return
		}
		for _, t := range tasks {
			if t.GetStatus().GetPhase() == v1alpha1.PhaseRunning {
				s.resyncTaskEgressPolicy(ctx, t.GetMetadata().GetAtespace(), t.GetMetadata().GetName())
			}
		}
		if len(tasks) < egressResyncPageSize {
			return
		}
	}
}

// resyncTaskEgressPolicy re-reads the task under its lock so its status write cannot clobber a concurrent Suspend or Resume.
func (s *Server) resyncTaskEgressPolicy(ctx context.Context, atespace, name string) {
	ctx, cancel := context.WithTimeout(ctx, egressResyncTaskTimeout)
	defer cancel()
	unlock, err := s.locker.Lock(ctx, "task", atespace, name)
	if err != nil {
		slog.Warn("egress policy resync: locking task", "atespace", atespace, "task", name, "error", err)
		return
	}
	defer unlock()

	task, err := s.getTask(ctx, atespace, name)
	if errors.Is(err, store.ErrNotFound) {
		return
	}
	if err != nil {
		slog.Warn("egress policy resync: getting task", "atespace", atespace, "task", name, "error", err)
		return
	}
	if task.GetStatus().GetPhase() != v1alpha1.PhaseRunning {
		return
	}
	if !s.reconciler.ReconcileEgressPolicy(ctx, task) {
		return
	}
	if err := s.updateTask(ctx, atespace, name, task.Status); err != nil {
		slog.Warn("egress policy resync: storing task status", "atespace", atespace, "task", name, "error", err)
	}
}
