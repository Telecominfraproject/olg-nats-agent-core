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

	go s.consumeWatch(watchCtx, target, watcher, handler, ready, done)

	if err := waitForWatchReady(ctx, s.runtime.KVTimeout(), ready, done); err != nil {
		cancelWatch()
		_ = watcher.Stop()
		shutdownTimeout := s.runtime.ShutdownTimeout()
		if shutdownTimeout <= 0 {
			shutdownTimeout = 2 * time.Second
		}
		select {
		case <-done:
		case <-time.After(shutdownTimeout):
			s.reportAsync(kvReadError("watch_desired_config_ready_timeout", "desired-config watch ready cleanup timed out", nil))
		}
		return nil, kvReadError("watch_desired_config", "desired-config watch did not become ready", err)
	}

	var once sync.Once
	stop := func() error {
		var stopErr error
		once.Do(func() {
			cancelWatch()
			watcher.Stop()

			shutdownTimeout := s.runtime.ShutdownTimeout()
			if shutdownTimeout <= 0 {
				shutdownTimeout = 2 * time.Second
			}

			select {
			case <-done:
			case <-time.After(shutdownTimeout):
				stopErr = kvReadError("watch_desired_config_stop_timeout", "desired-config watch stop timed out waiting for handler cleanup", nil)
				s.reportAsync(stopErr)
			}
		})
		return stopErr
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

func (s *Store) consumeWatch(ctx context.Context, target string, watcher jetstream.KeyWatcher, handler WatchHandler, ready chan<- struct{}, done chan<- struct{}) {
	defer close(done)

	var readyOnce sync.Once
	markReady := func() {
		readyOnce.Do(func() {
			close(ready)
		})
	}
	defer markReady()

	var buffer []StoredDesiredConfig
	isReady := false

	updates := watcher.Updates()
	for {
		select {
		case <-ctx.Done():
			return
		case entry, ok := <-updates:
			if !ok {
				if ctx.Err() == nil {
					s.reportAsync(kvReadError("watch_desired_config_terminated", "desired-config watcher terminated unexpectedly", nil))
				}
				return
			}
			if entry == nil {
				markReady()
				for _, stored := range buffer {
					if err := handler(ctx, stored); err != nil {
						s.reportAsync(kvReadError("watch_desired_config_handler", "desired-config watch handler returned error", err))
					}
				}
				buffer = nil
				isReady = true
				continue
			}

			deleted := entry.Operation() == jetstream.KeyValueDelete || entry.Operation() == jetstream.KeyValuePurge || len(entry.Value()) == 0
			var rec DesiredConfigRecord
			if !deleted {
				var err error
				rec, err = decodeDesiredConfigRecord(entry.Value())
				if err != nil {
					s.reportAsync(kvReadError("watch_desired_config_decode", "failed to decode desired-config watch entry", err))
					continue
				}
			} else {
				rec = DesiredConfigRecord{
					Target: target,
				}
			}

			stored := StoredDesiredConfig{
				Record:   rec,
				Bucket:   entry.Bucket(),
				Key:      entry.Key(),
				Revision: entry.Revision(),
				Deleted:  deleted,
			}
			if !entry.Created().IsZero() {
				stored.CreatedAt = entry.Created()
			} else {
				stored.CreatedAt = rec.Timestamp
			}

			if !isReady {
				buffer = append(buffer, stored)
			} else {
				if err := handler(ctx, stored); err != nil {
					s.reportAsync(kvReadError("watch_desired_config_handler", "desired-config watch handler returned error", err))
				}
			}
		}
	}
}
