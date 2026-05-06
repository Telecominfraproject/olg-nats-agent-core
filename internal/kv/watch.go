package kv

import (
	"context"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// WatchDesiredConfig watches desired-config updates for a specific target.
func (s *Store) WatchDesiredConfig(ctx context.Context, target string, handler WatchHandler) (StopFunc, error) {
	if handler == nil {
		return nil, validationError("watch_desired_config", "watch handler is required")
	}

	key, err := buildDesiredConfigKey(s.runtime.DesiredConfigKeyPattern(), target)
	if err != nil {
		return nil, err
	}

	kvHandle, err := s.runtime.KeyValue()
	if err != nil {
		return nil, err
	}

	watchCtx, cancelWatch := context.WithCancel(context.Background())
	if ctx != nil && ctx.Done() != nil {
		parentDone := ctx.Done()
		go func() {
			select {
			case <-parentDone:
				cancelWatch()
			case <-watchCtx.Done():
			}
		}()
	}

	watcher, err := kvHandle.Watch(watchCtx, key)
	if err != nil {
		cancelWatch()
		return nil, kvReadError("watch_desired_config", "failed to start desired-config watch", err)
	}

	ready := make(chan struct{})
	done := make(chan struct{})

	go s.consumeWatch(watchCtx, watcher, handler, ready, done)

	if err := waitForWatchReady(ctx, s.runtime.KVTimeout(), ready, done); err != nil {
		cancelWatch()
		_ = watcher.Stop()
		<-done
		return nil, kvReadError("watch_desired_config", "desired-config watch did not become ready", err)
	}

	var once sync.Once
	stop := func() error {
		once.Do(func() {
			cancelWatch()
			watcher.Stop()
			<-done
		})
		return nil
	}

	return stop, nil
}

func waitForWatchReady(ctx context.Context, timeout time.Duration, ready <-chan struct{}, done <-chan struct{}) error {
	waitCtx, cancel := withTimeout(ctx, timeout)
	defer cancel()

	select {
	case <-ready:
		return nil
	case <-done:
		return context.Canceled
	case <-waitCtx.Done():
		return waitCtx.Err()
	}
}

func (s *Store) consumeWatch(ctx context.Context, watcher jetstream.KeyWatcher, handler WatchHandler, ready chan<- struct{}, done chan<- struct{}) {
	defer close(done)

	var readyOnce sync.Once
	markReady := func() {
		readyOnce.Do(func() {
			close(ready)
		})
	}
	defer markReady()

	updates := watcher.Updates()
	for {
		select {
		case <-ctx.Done():
			return
		case entry, ok := <-updates:
			if !ok {
				return
			}
			if entry == nil {
				markReady()
				continue
			}
			if len(entry.Value()) == 0 {
				continue
			}

			rec, err := decodeDesiredConfigRecord(entry.Value())
			if err != nil {
				s.reportAsync(kvReadError("watch_desired_config_decode", "failed to decode desired-config watch entry", err))
				continue
			}

			stored := StoredDesiredConfig{
				Record:   rec,
				Bucket:   entry.Bucket(),
				Key:      entry.Key(),
				Revision: entry.Revision(),
			}
			if !entry.Created().IsZero() {
				stored.CreatedAt = entry.Created()
			} else {
				stored.CreatedAt = rec.Timestamp
			}

			if err := handler(ctx, stored); err != nil {
				s.reportAsync(kvReadError("watch_desired_config_handler", "desired-config watch handler returned error", err))
			}
		}
	}
}
