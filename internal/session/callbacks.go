package session

import (
	"context"
	"errors"
	"strings"

	"github.com/Telecominfraproject/olg-nats-agent-core/internal/runtimeerr"
	"github.com/nats-io/nats.go"
)

func (m *Manager) onDisconnect(_ *nats.Conn, err error) {
	m.mu.Lock()
	if m.closing {
		m.mu.Unlock()
		return
	}
	m.setReconnectingLocked(err)
	onDisconnected := m.hooks.OnDisconnected
	m.mu.Unlock()

	if onDisconnected != nil {
		onDisconnected()
	}

	if err != nil {
		disconnectErr := &runtimeerr.Error{
			Code:      runtimeerr.CodeDisconnected,
			Op:        "session_disconnect",
			Message:   "NATS session disconnected",
			Retryable: true,
			Err:       err,
		}
		go m.onAsyncError(disconnectErr)
	}
}

func (m *Manager) onReconnect(nc *nats.Conn) {
	m.mu.RLock()
	if m.closing {
		m.mu.RUnlock()
		return
	}
	m.mu.RUnlock()

	go m.rebindAfterReconnect(nc)
}

func (m *Manager) onClosed(_ *nats.Conn) {
	m.mu.Lock()
	m.setClosedLocked(nil)
	onClosed := m.hooks.OnClosed
	m.mu.Unlock()

	if onClosed != nil {
		onClosed()
	}
}

func (m *Manager) onAsyncError(err error) {
	if err == nil {
		return
	}
	if m.hooks.Logger != nil {
		m.hooks.Logger.Error("session asynchronous error", "error", err)
	}
	if m.hooks.ErrorSink != nil {
		m.hooks.ErrorSink(err)
	}
}

func (m *Manager) rebindAfterReconnect(nc *nats.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), m.effective.Config.Timeouts.KVTimeout)
	defer cancel()

	js, err := m.newJetStream(nc)
	if err != nil {
		m.mu.Lock()
		m.setDegradedLocked(err)
		m.mu.Unlock()
		m.onAsyncError(&runtimeerr.Error{
			Code:      runtimeerr.CodeJetStreamFailed,
			Op:        "reconnect_jetstream",
			Message:   "failed to rebuild JetStream handle after reconnect",
			Retryable: true,
			Err:       err,
		})
		return
	}

	kv, err := m.bindOrCreateKV(ctx, js)
	if err != nil {
		m.mu.Lock()
		m.setDegradedLocked(err)
		m.mu.Unlock()
		m.onAsyncError(&runtimeerr.Error{
			Code:      runtimeerr.CodeJetStreamFailed,
			Op:        "reconnect_kv",
			Message:   "failed to rebind KV bucket after reconnect",
			Retryable: true,
			Err:       err,
		})
		return
	}

	m.mu.Lock()
	if m.closing {
		m.mu.Unlock()
		return
	}
	if m.nc == nil || m.nc != nc {
		m.mu.Unlock()
		return
	}

	m.js = js
	m.kv = kv
	m.setConnectedLocked(nc.ConnectedUrl(), true, true)
	onReconnected := m.hooks.OnReconnected
	m.mu.Unlock()

	if onReconnected != nil {
		onReconnected()
	}
}

func isBucketNotFound(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, nats.ErrBucketNotFound) {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "bucket not found") || strings.Contains(message, "stream not found")
}
