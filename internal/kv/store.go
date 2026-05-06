package kv

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/routerarchitects/nats-agent-core/internal/runtimeerr"
)

// RuntimeProvider exposes session runtime handles required by KV operations.
type RuntimeProvider interface {
	KeyValue() (jetstream.KeyValue, error)
	DesiredConfigBucket() string
	DesiredConfigKeyPattern() string
	KVTimeout() time.Duration
}

// DesiredConfigRecord is the desired-state payload stored in KV.
type DesiredConfigRecord struct {
	Version   string
	RPCID     string
	Target    string
	UUID      string
	Payload   json.RawMessage
	Timestamp time.Time
}

// StoredDesiredConfig wraps desired-state payload and KV metadata.
type StoredDesiredConfig struct {
	Record    DesiredConfigRecord
	Bucket    string
	Key       string
	Revision  uint64
	CreatedAt time.Time
}

// WatchHandler handles desired-config watch updates.
type WatchHandler func(context.Context, StoredDesiredConfig) error

// StopFunc stops an active desired-config watch.
type StopFunc func() error

// Store implements desired-config KV operations backed by JetStream KV.
type Store struct {
	runtime   RuntimeProvider
	errorSink func(error)
}

// NewStore creates a desired-config KV store bound to runtime state.
func NewStore(runtime RuntimeProvider, errorSink func(error)) (*Store, error) {
	if runtime == nil {
		return nil, &runtimeerr.Error{
			Code:      runtimeerr.CodeValidation,
			Op:        "new_kv_store",
			Message:   "runtime provider is required",
			Retryable: false,
		}
	}
	return &Store{runtime: runtime, errorSink: errorSink}, nil
}

// StoreDesiredConfig stores a desired-config record in KV and returns metadata.
func (s *Store) StoreDesiredConfig(ctx context.Context, rec DesiredConfigRecord) (*StoredDesiredConfig, error) {
	if err := validateDesiredConfigRecord(rec); err != nil {
		return nil, err
	}

	key, err := buildDesiredConfigKey(s.runtime.DesiredConfigKeyPattern(), rec.Target)
	if err != nil {
		return nil, err
	}

	kvHandle, err := s.runtime.KeyValue()
	if err != nil {
		return nil, err
	}

	payload, err := encodeDesiredConfigRecord(rec)
	if err != nil {
		return nil, err
	}

	opCtx, cancel := withTimeout(ctx, s.runtime.KVTimeout())
	defer cancel()

	revision, err := kvHandle.Put(opCtx, key, payload)
	if err != nil {
		return nil, kvStoreError("store_desired_config", "failed to write desired config to KV", err)
	}

	createdAt := rec.Timestamp
	entry, getErr := kvHandle.Get(opCtx, key)
	if getErr == nil {
		revision = entry.Revision()
		if !entry.Created().IsZero() {
			createdAt = entry.Created()
		}
	} else {
		s.reportAsync(kvReadError("store_desired_config_post_read", "stored config metadata lookup failed", getErr))
	}

	stored := &StoredDesiredConfig{
		Record:    rec,
		Bucket:    s.runtime.DesiredConfigBucket(),
		Key:       key,
		Revision:  revision,
		CreatedAt: createdAt,
	}
	return stored, nil
}

// LoadDesiredConfig loads the latest desired-config record for a target.
func (s *Store) LoadDesiredConfig(ctx context.Context, target string) (*StoredDesiredConfig, error) {
	key, err := buildDesiredConfigKey(s.runtime.DesiredConfigKeyPattern(), target)
	if err != nil {
		return nil, err
	}

	kvHandle, err := s.runtime.KeyValue()
	if err != nil {
		return nil, err
	}

	opCtx, cancel := withTimeout(ctx, s.runtime.KVTimeout())
	defer cancel()

	entry, err := kvHandle.Get(opCtx, key)
	if err != nil {
		if isConfigNotFound(err) {
			return nil, &runtimeerr.Error{
				Code:      runtimeerr.CodeConfigNotFound,
				Op:        "load_desired_config",
				Key:       key,
				Message:   "desired config not found",
				Retryable: false,
				Err:       err,
			}
		}
		return nil, kvReadError("load_desired_config", "failed to read desired config from KV", err)
	}

	rec, err := decodeDesiredConfigRecord(entry.Value())
	if err != nil {
		return nil, err
	}

	stored := &StoredDesiredConfig{
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

	return stored, nil
}

func withTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); ok || timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func isConfigNotFound(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, nats.ErrKeyNotFound) || errors.Is(err, nats.ErrKeyDeleted) || errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrKeyDeleted) {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "key not found") || strings.Contains(message, "key was deleted")
}

func (s *Store) reportAsync(err error) {
	if err == nil {
		return
	}
	if s.errorSink != nil {
		s.errorSink(err)
	}
}

func encodeDesiredConfigRecord(rec DesiredConfigRecord) ([]byte, error) {
	if err := validateDesiredConfigRecord(rec); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		return nil, &runtimeerr.Error{
			Code:      runtimeerr.CodeEncodeFailed,
			Op:        "store_desired_config",
			Message:   "failed to encode desired config",
			Retryable: false,
			Err:       err,
		}
	}
	return payload, nil
}

func decodeDesiredConfigRecord(data []byte) (DesiredConfigRecord, error) {
	if len(data) == 0 {
		return DesiredConfigRecord{}, &runtimeerr.Error{
			Code:      runtimeerr.CodeValidation,
			Op:        "load_desired_config",
			Message:   "payload is required",
			Retryable: false,
		}
	}

	var rec DesiredConfigRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return DesiredConfigRecord{}, &runtimeerr.Error{
			Code:      runtimeerr.CodeDecodeFailed,
			Op:        "load_desired_config",
			Message:   "failed to decode desired config",
			Retryable: false,
			Err:       err,
		}
	}

	if err := validateDesiredConfigRecord(rec); err != nil {
		return DesiredConfigRecord{}, err
	}

	return rec, nil
}

func validateDesiredConfigRecord(rec DesiredConfigRecord) error {
	const op = "validate_desired_config_record"

	if strings.TrimSpace(rec.Version) == "" {
		return validationError(op, "version is required")
	}
	if strings.TrimSpace(rec.RPCID) == "" {
		return validationError(op, "rpc_id is required")
	}
	if strings.TrimSpace(rec.Target) == "" {
		return validationError(op, "target is required")
	}
	if strings.TrimSpace(rec.UUID) == "" {
		return validationError(op, "uuid is required")
	}
	if rec.Timestamp.IsZero() {
		return validationError(op, "timestamp is required")
	}
	if len(rec.Payload) == 0 {
		return validationError(op, "payload is required")
	}
	if !json.Valid(rec.Payload) {
		return validationError(op, "payload must contain valid JSON")
	}
	if err := validateToken("validate_target", "target", rec.Target); err != nil {
		return err
	}

	return nil
}
