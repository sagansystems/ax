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
	"math/rand/v2"
	"net"
	"net/http"
	"sync"
	"time"
)

// ConnectionAge is MaxConnectionAge for gRPC served through net/http, where
// grpc.KeepaliveParams has no effect.
type ConnectionAge struct {
	// MaxAge, plus or minus 10% jitter, before the client is asked to reconnect. Zero disables.
	MaxAge time.Duration
	// Grace after MaxAge before the connection is closed. Zero waits indefinitely.
	Grace time.Duration
}

type connAgeKey struct{}

type agedConn struct {
	expiry time.Time
	timer  *time.Timer
}

// Apply makes hs enforce the limit. Call it after setting hs.Handler.
func (c ConnectionAge) Apply(hs *http.Server) {
	if c.MaxAge <= 0 {
		return
	}
	var conns sync.Map // net.Conn -> *agedConn

	nextConnContext := hs.ConnContext
	hs.ConnContext = func(ctx context.Context, conn net.Conn) context.Context {
		if nextConnContext != nil {
			ctx = nextConnContext(ctx, conn)
		}
		ac := &agedConn{expiry: time.Now().Add(c.jittered())}
		if c.Grace > 0 {
			ac.timer = time.AfterFunc(time.Until(ac.expiry)+c.Grace, func() { _ = conn.Close() })
			conns.Store(conn, ac)
		}
		return context.WithValue(ctx, connAgeKey{}, ac)
	}

	nextConnState := hs.ConnState
	hs.ConnState = func(conn net.Conn, state http.ConnState) {
		if state == http.StateClosed || state == http.StateHijacked {
			if v, ok := conns.LoadAndDelete(conn); ok {
				v.(*agedConn).timer.Stop()
			}
		}
		if nextConnState != nil {
			nextConnState(conn, state)
		}
	}

	next := hs.Handler
	hs.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ac, ok := r.Context().Value(connAgeKey{}).(*agedConn); ok && !time.Now().Before(ac.expiry) {
			// net/http sends this as a graceful HTTP/2 GOAWAY.
			w.Header().Set("Connection", "close")
		}
		next.ServeHTTP(w, r)
	})
}

func (c ConnectionAge) jittered() time.Duration {
	j := int64(c.MaxAge / 10)
	if j <= 0 {
		return c.MaxAge
	}
	return c.MaxAge + time.Duration(rand.Int64N(2*j+1)-j)
}
