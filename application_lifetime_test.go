// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package lightpool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modulecollective/lightpool/core"
)

func TestApplicationWorkersRecoverAfterOperationEnds(t *testing.T) {
	for _, reason := range []string{"cancel", "timeout"} {
		t.Run(reason, func(t *testing.T) {
			cfg := testConfig("a")
			cfg.Protocol.ProbeInterval, cfg.Protocol.GossipInterval, cfg.Protocol.PushPullInterval = 0, 0, 0
			cfg.Protocol.ProbeTimeout = 500 * time.Millisecond
			cfg.Protocol.StreamTimeout = time.Second
			if reason == "cancel" {
				cfg.Protocol.StreamTimeout = 10 * time.Second
			}
			started := make(chan struct{}, 16)
			var canceled, invoked atomic.Int64
			cfg.Application = app{request: func(ctx context.Context, r core.Request) (core.Response, error) {
				if r.Kind == core.LocalState {
					invoked.Add(1)
					started <- struct{}{}
					<-ctx.Done()
					canceled.Add(1)
					return core.Response{}, ctx.Err()
				}
				return core.Response{}, nil
			}}
			a := newNode(t, cfg)
			peerCfg := cfg
			peerCfg.Name, peerCfg.Application = "b", nil
			b := newNode(t, peerCfg)
			ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
			defer done()
			if _, err := b.Ping(ctx, a.Identity()); err != nil {
				t.Fatal("initial ping", err)
			}
			var cancels []context.CancelFunc
			results := make(chan error, 9)
			for range 8 {
				joinCtx, cancel := context.WithCancel(ctx)
				defer cancel()
				cancels = append(cancels, cancel)
				go func() { _, err := a.Join(joinCtx, b.Identity().Address.String()); results <- err }()
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatal("worker did not start", ctx.Err())
				}
			}
			if reason == "cancel" {
				// The ninth request must be canceled while still in the worker queue.
				queuedCtx, cancel := context.WithCancel(ctx)
				defer cancel()
				go func() { _, err := a.Join(queuedCtx, b.Identity().Address.String()); results <- err }()
				wait(t, "queued LocalState request", func() bool { return len(a.requests) == 1 })
				cancel()
				if err := <-results; !errors.Is(err, context.Canceled) {
					t.Fatal("queued cancel", err)
				}
				wait(t, "queued operation removed", func() bool { return a.Snapshot().PendingStreams == 8 })
				for _, cancel := range cancels {
					cancel()
				}
			}
			for range 8 {
				select {
				case err := <-results:
					if err == nil {
						t.Fatal("ended join succeeded")
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			wait(t, "operations removed", func() bool { return a.Snapshot().PendingStreams == 0 })
			if _, err := b.Ping(ctx, a.Identity()); err != nil {
				t.Fatalf("workers not recovered: canceled=%d unrelated Ping=%v", canceled.Load(), err)
			}
			wait(t, "callback cancellation", func() bool { return canceled.Load() == 8 })
			if invoked.Load() != 8 {
				t.Fatal("canceled queued request invoked", invoked.Load())
			}
		})
	}
}
