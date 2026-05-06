package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/routerarchitects/nats-agent-core/internal/runtimeerr"
)

type stubRuntimeProvider struct {
	keyValueCalls int
	kv            jetstream.KeyValue
	kvErr         error
	bucket        string
	keyPattern    string
	kvTimeout     time.Duration
}

func (s *stubRuntimeProvider) KeyValue() (jetstream.KeyValue, error) {
	s.keyValueCalls++
	if s.kvErr != nil {
		return nil, s.kvErr
	}
	return s.kv, nil
}

func (s *stubRuntimeProvider) DesiredConfigBucket() string {
	if s.bucket != "" {
		return s.bucket
	}
	return "cfg_desired"
}

func (s *stubRuntimeProvider) DesiredConfigKeyPattern() string {
	if s.keyPattern != "" {
		return s.keyPattern
	}
	return "desired.%s"
}

func (s *stubRuntimeProvider) KVTimeout() time.Duration {
	if s.kvTimeout > 0 {
		return s.kvTimeout
	}
	return 200 * time.Millisecond
}

type stubKeyValue struct {
	putCalls   int
	getCalls   int
	watchCalls int

	lastPutKey   string
	lastPutValue []byte
	lastGetKey   string
	lastWatchKey string

	putFn   func(context.Context, string, []byte) (uint64, error)
	getFn   func(context.Context, string) (jetstream.KeyValueEntry, error)
	watchFn func(context.Context, string, ...jetstream.WatchOpt) (jetstream.KeyWatcher, error)
}

func (s *stubKeyValue) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	s.getCalls++
	s.lastGetKey = key
	if s.getFn != nil {
		return s.getFn(ctx, key)
	}
	return nil, nil
}

func (s *stubKeyValue) GetRevision(context.Context, string, uint64) (jetstream.KeyValueEntry, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *stubKeyValue) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	s.putCalls++
	s.lastPutKey = key
	s.lastPutValue = append([]byte(nil), value...)
	if s.putFn != nil {
		return s.putFn(ctx, key, value)
	}
	return 0, nil
}

func (s *stubKeyValue) PutString(context.Context, string, string) (uint64, error) {
	return 0, fmt.Errorf("not implemented")
}

func (s *stubKeyValue) Create(context.Context, string, []byte, ...jetstream.KVCreateOpt) (uint64, error) {
	return 0, fmt.Errorf("not implemented")
}

func (s *stubKeyValue) Update(context.Context, string, []byte, uint64) (uint64, error) {
	return 0, fmt.Errorf("not implemented")
}

func (s *stubKeyValue) Delete(context.Context, string, ...jetstream.KVDeleteOpt) error {
	return fmt.Errorf("not implemented")
}

func (s *stubKeyValue) Purge(context.Context, string, ...jetstream.KVDeleteOpt) error {
	return fmt.Errorf("not implemented")
}

func (s *stubKeyValue) Watch(ctx context.Context, keys string, opts ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	s.watchCalls++
	s.lastWatchKey = keys
	if s.watchFn != nil {
		return s.watchFn(ctx, keys, opts...)
	}
	return nil, nil
}

func (s *stubKeyValue) WatchAll(context.Context, ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *stubKeyValue) WatchFiltered(context.Context, []string, ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *stubKeyValue) Keys(context.Context, ...jetstream.WatchOpt) ([]string, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *stubKeyValue) ListKeys(context.Context, ...jetstream.WatchOpt) (jetstream.KeyLister, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *stubKeyValue) ListKeysFiltered(context.Context, ...string) (jetstream.KeyLister, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *stubKeyValue) History(context.Context, string, ...jetstream.WatchOpt) ([]jetstream.KeyValueEntry, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *stubKeyValue) Bucket() string {
	return "cfg_desired"
}

func (s *stubKeyValue) PurgeDeletes(context.Context, ...jetstream.KVPurgeOpt) error {
	return fmt.Errorf("not implemented")
}

func (s *stubKeyValue) Status(context.Context) (jetstream.KeyValueStatus, error) {
	return nil, fmt.Errorf("not implemented")
}

type stubKeyValueEntry struct {
	bucket   string
	key      string
	value    []byte
	revision uint64
	created  time.Time
	delta    uint64
	op       jetstream.KeyValueOp
}

func (e stubKeyValueEntry) Bucket() string                  { return e.bucket }
func (e stubKeyValueEntry) Key() string                     { return e.key }
func (e stubKeyValueEntry) Value() []byte                   { return e.value }
func (e stubKeyValueEntry) Revision() uint64                { return e.revision }
func (e stubKeyValueEntry) Created() time.Time              { return e.created }
func (e stubKeyValueEntry) Delta() uint64                   { return e.delta }
func (e stubKeyValueEntry) Operation() jetstream.KeyValueOp { return e.op }

type stubKeyWatcher struct {
	updates  chan jetstream.KeyValueEntry
	stopErr  error
	stopCall int
	mu       sync.Mutex
}

func (w *stubKeyWatcher) Updates() <-chan jetstream.KeyValueEntry {
	return w.updates
}

func (w *stubKeyWatcher) Stop() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.stopCall++
	if w.stopErr != nil {
		return w.stopErr
	}
	return nil
}

func validDesiredConfigRecordForTests() DesiredConfigRecord {
	return DesiredConfigRecord{
		Version:   "1.0",
		RPCID:     "rpc-1",
		Target:    "vyos",
		UUID:      "cfg-1",
		Payload:   json.RawMessage(`{"hostname":"edge-1"}`),
		Timestamp: time.Unix(1700000000, 0).UTC(),
	}
}

/*
TC-KV-STORE-001
Type: Negative
Title: NewStore rejects nil runtime provider
Summary:
Verifies that store constructor rejects missing runtime provider dependency
with a typed validation error.

Validates:
  - nil runtime returns CodeValidation
  - error op is new_kv_store
*/
func TestNewStoreRejectsNilRuntimeProvider(t *testing.T) {
	store, err := NewStore(nil, nil)
	if store != nil {
		t.Fatalf("expected nil store, got %#v", store)
	}
	requireKVRuntimeError(t, err, runtimeerr.CodeValidation, "new_kv_store", "runtime provider is required")
}

/*
TC-KV-STORE-002
Type: Positive
Title: validateDesiredConfigRecord accepts a valid record
Summary:
Verifies that desired-config record validator accepts a complete valid record
with required identifiers timestamp and JSON payload.

Validates:
  - valid desired-config record passes validation
*/
func TestValidateDesiredConfigRecordAcceptsValidRecord(t *testing.T) {
	if err := validateDesiredConfigRecord(validDesiredConfigRecordForTests()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

/*
TC-KV-STORE-003
Type: Negative
Title: validateDesiredConfigRecord rejects missing required fields
Summary:
Verifies that desired-config record validator rejects missing required fields
and invalid payload values.

Validates:
  - missing required identifiers are rejected
  - zero timestamp and invalid payload are rejected
*/
func TestValidateDesiredConfigRecordRejectsMissingFields(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*DesiredConfigRecord)
		msgPart string
	}{
		{name: "missing version", mutate: func(r *DesiredConfigRecord) { r.Version = "" }, msgPart: "version is required"},
		{name: "missing rpc_id", mutate: func(r *DesiredConfigRecord) { r.RPCID = "" }, msgPart: "rpc_id is required"},
		{name: "missing target", mutate: func(r *DesiredConfigRecord) { r.Target = "" }, msgPart: "target is required"},
		{name: "missing uuid", mutate: func(r *DesiredConfigRecord) { r.UUID = "" }, msgPart: "uuid is required"},
		{name: "zero timestamp", mutate: func(r *DesiredConfigRecord) { r.Timestamp = time.Time{} }, msgPart: "timestamp is required"},
		{name: "empty payload", mutate: func(r *DesiredConfigRecord) { r.Payload = nil }, msgPart: "payload is required"},
		{name: "invalid payload json", mutate: func(r *DesiredConfigRecord) { r.Payload = json.RawMessage(`{"hostname":`) }, msgPart: "payload must contain valid JSON"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := validDesiredConfigRecordForTests()
			tc.mutate(&rec)

			err := validateDesiredConfigRecord(rec)
			requireKVRuntimeError(t, err, runtimeerr.CodeValidation, "validate_desired_config_record", tc.msgPart)
		})
	}
}

/*
TC-KV-STORE-004
Type: Negative
Title: StoreDesiredConfig fails validation before runtime lookup
Summary:
Verifies that store path validates input first and does not ask runtime for a
KV handle when the record is invalid.

Validates:
  - invalid record returns CodeValidation
  - runtime KeyValue lookup is skipped
*/
func TestStoreDesiredConfigFailsValidationBeforeRuntimeLookup(t *testing.T) {
	runtime := &stubRuntimeProvider{}
	store, err := NewStore(runtime, nil)
	if err != nil {
		t.Fatalf("expected nil constructor error, got %v", err)
	}

	rec := validDesiredConfigRecordForTests()
	rec.UUID = ""

	stored, err := store.StoreDesiredConfig(context.Background(), rec)
	if stored != nil {
		t.Fatalf("expected nil stored result, got %#v", stored)
	}
	requireKVRuntimeError(t, err, runtimeerr.CodeValidation, "validate_desired_config_record", "uuid is required")
	if runtime.keyValueCalls != 0 {
		t.Fatalf("expected runtime KeyValue lookup to be skipped, got %d calls", runtime.keyValueCalls)
	}
}

/*
TC-KV-STORE-005
Type: Negative
Title: StoreDesiredConfig propagates runtime KeyValue failure
Summary:
Verifies that store path returns runtime KeyValue lookup failures directly when
runtime is disconnected.

Validates:
  - KeyValue failure returns CodeDisconnected
  - Put operation is not attempted
*/
func TestStoreDesiredConfigPropagatesRuntimeKeyValueFailure(t *testing.T) {
	runtime := &stubRuntimeProvider{
		kvErr: &runtimeerr.Error{Code: runtimeerr.CodeDisconnected, Op: "key_value", Message: "runtime not connected"},
	}
	store, err := NewStore(runtime, nil)
	if err != nil {
		t.Fatalf("expected nil constructor error, got %v", err)
	}

	stored, err := store.StoreDesiredConfig(context.Background(), validDesiredConfigRecordForTests())
	if stored != nil {
		t.Fatalf("expected nil stored result, got %#v", stored)
	}
	requireKVRuntimeError(t, err, runtimeerr.CodeDisconnected, "key_value", "runtime not connected")
}

/*
TC-KV-STORE-006
Type: Negative
Title: StoreDesiredConfig wraps Put failure as KV store error
Summary:
Verifies that store path wraps KV Put errors using the typed KV store failure
code and op.

Validates:
  - Put failure returns CodeKVStoreFailed
  - store_desired_config op is preserved
*/
func TestStoreDesiredConfigWrapsPutFailure(t *testing.T) {
	kvHandle := &stubKeyValue{
		putFn: func(context.Context, string, []byte) (uint64, error) {
			return 0, errors.New("put failed")
		},
	}
	runtime := &stubRuntimeProvider{kv: kvHandle}
	store, err := NewStore(runtime, nil)
	if err != nil {
		t.Fatalf("expected nil constructor error, got %v", err)
	}

	stored, err := store.StoreDesiredConfig(context.Background(), validDesiredConfigRecordForTests())
	if stored != nil {
		t.Fatalf("expected nil stored result, got %#v", stored)
	}
	requireKVRuntimeError(t, err, runtimeerr.CodeKVStoreFailed, "store_desired_config", "failed to write desired config to KV")
	if kvHandle.putCalls != 1 {
		t.Fatalf("expected Put to be called once, got %d", kvHandle.putCalls)
	}
}

/*
TC-KV-STORE-007
Type: Positive
Title: StoreDesiredConfig returns metadata from post-read entry
Summary:
Verifies that store path uses post-read entry metadata revision and created
timestamp when metadata read succeeds.

Validates:
  - returned bucket key revision and created_at are populated
  - revision and created_at use post-read entry values
*/
func TestStoreDesiredConfigReturnsMetadataFromPostReadEntry(t *testing.T) {
	rec := validDesiredConfigRecordForTests()
	postReadCreated := rec.Timestamp.Add(5 * time.Second)
	kvHandle := &stubKeyValue{
		putFn: func(context.Context, string, []byte) (uint64, error) {
			return 9, nil
		},
		getFn: func(context.Context, string) (jetstream.KeyValueEntry, error) {
			return stubKeyValueEntry{
				bucket:   "cfg_desired",
				key:      "desired.vyos",
				value:    []byte(`{"version":"1.0"}`),
				revision: 17,
				created:  postReadCreated,
			}, nil
		},
	}
	runtime := &stubRuntimeProvider{kv: kvHandle, bucket: "cfg_desired", keyPattern: "desired.%s"}
	store, err := NewStore(runtime, nil)
	if err != nil {
		t.Fatalf("expected nil constructor error, got %v", err)
	}

	stored, err := store.StoreDesiredConfig(context.Background(), rec)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if stored == nil {
		t.Fatal("expected non-nil stored result")
	}
	if stored.Bucket != "cfg_desired" {
		t.Fatalf("expected bucket %q, got %q", "cfg_desired", stored.Bucket)
	}
	if stored.Key != "desired.vyos" {
		t.Fatalf("expected key %q, got %q", "desired.vyos", stored.Key)
	}
	if stored.Revision != 17 {
		t.Fatalf("expected revision %d, got %d", 17, stored.Revision)
	}
	if !stored.CreatedAt.Equal(postReadCreated) {
		t.Fatalf("expected CreatedAt %v, got %v", postReadCreated, stored.CreatedAt)
	}
}

/*
TC-KV-STORE-008
Type: Positive
Title: StoreDesiredConfig falls back to record timestamp when post-read created is zero
Summary:
Verifies that store path falls back to input record timestamp when post-read
metadata entry has zero Created value.

Validates:
  - CreatedAt falls back to record timestamp when entry.Created is zero
*/
func TestStoreDesiredConfigFallsBackToRecordTimestampWhenPostReadCreatedIsZero(t *testing.T) {
	rec := validDesiredConfigRecordForTests()
	kvHandle := &stubKeyValue{
		putFn: func(context.Context, string, []byte) (uint64, error) {
			return 5, nil
		},
		getFn: func(context.Context, string) (jetstream.KeyValueEntry, error) {
			return stubKeyValueEntry{bucket: "cfg_desired", key: "desired.vyos", revision: 5, created: time.Time{}}, nil
		},
	}
	runtime := &stubRuntimeProvider{kv: kvHandle}
	store, err := NewStore(runtime, nil)
	if err != nil {
		t.Fatalf("expected nil constructor error, got %v", err)
	}

	stored, err := store.StoreDesiredConfig(context.Background(), rec)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !stored.CreatedAt.Equal(rec.Timestamp) {
		t.Fatalf("expected CreatedAt fallback to record timestamp %v, got %v", rec.Timestamp, stored.CreatedAt)
	}
}

/*
TC-KV-STORE-009
Type: Positive
Title: StoreDesiredConfig reports post-read metadata failure but still succeeds
Summary:
Verifies that post-write metadata lookup failure does not fail store operation
and is reported asynchronously through error sink.

Validates:
  - store still returns a successful StoredDesiredConfig
  - error sink receives post-read metadata failure
*/
func TestStoreDesiredConfigReportsPostReadFailureButStillSucceeds(t *testing.T) {
	rec := validDesiredConfigRecordForTests()
	kvHandle := &stubKeyValue{
		putFn: func(context.Context, string, []byte) (uint64, error) {
			return 11, nil
		},
		getFn: func(context.Context, string) (jetstream.KeyValueEntry, error) {
			return nil, errors.New("metadata read failed")
		},
	}

	var sinkErr error
	store, err := NewStore(&stubRuntimeProvider{kv: kvHandle}, func(err error) {
		sinkErr = err
	})
	if err != nil {
		t.Fatalf("expected nil constructor error, got %v", err)
	}

	stored, err := store.StoreDesiredConfig(context.Background(), rec)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if stored == nil {
		t.Fatal("expected non-nil stored result")
	}
	if sinkErr == nil {
		t.Fatal("expected post-read failure to be reported to error sink")
	}
	requireKVRuntimeError(t, sinkErr, runtimeerr.CodeKVReadFailed, "store_desired_config_post_read", "stored config metadata lookup failed")
}

/*
TC-KV-STORE-010
Type: Negative
Title: LoadDesiredConfig returns config-not-found for missing key
Summary:
Verifies that load path maps not-found lookup errors to typed config-not-found
error semantics.

Validates:
  - key-not-found returns CodeConfigNotFound
  - load_desired_config op is preserved
*/
func TestLoadDesiredConfigReturnsConfigNotFoundForMissingKey(t *testing.T) {
	kvHandle := &stubKeyValue{
		getFn: func(context.Context, string) (jetstream.KeyValueEntry, error) {
			return nil, nats.ErrKeyNotFound
		},
	}
	store, err := NewStore(&stubRuntimeProvider{kv: kvHandle}, nil)
	if err != nil {
		t.Fatalf("expected nil constructor error, got %v", err)
	}

	stored, err := store.LoadDesiredConfig(context.Background(), "vyos")
	if stored != nil {
		t.Fatalf("expected nil stored result, got %#v", stored)
	}
	requireKVRuntimeError(t, err, runtimeerr.CodeConfigNotFound, "load_desired_config", "desired config not found")
}

/*
TC-KV-STORE-011
Type: Negative
Title: LoadDesiredConfig wraps generic Get failures as KV read errors
Summary:
Verifies that load path wraps non-not-found KV get failures with the typed KV
read error model.

Validates:
  - generic Get failure returns CodeKVReadFailed
  - load_desired_config op is preserved
*/
func TestLoadDesiredConfigWrapsGenericGetFailure(t *testing.T) {
	kvHandle := &stubKeyValue{
		getFn: func(context.Context, string) (jetstream.KeyValueEntry, error) {
			return nil, errors.New("read timeout")
		},
	}
	store, err := NewStore(&stubRuntimeProvider{kv: kvHandle}, nil)
	if err != nil {
		t.Fatalf("expected nil constructor error, got %v", err)
	}

	stored, err := store.LoadDesiredConfig(context.Background(), "vyos")
	if stored != nil {
		t.Fatalf("expected nil stored result, got %#v", stored)
	}
	requireKVRuntimeError(t, err, runtimeerr.CodeKVReadFailed, "load_desired_config", "failed to read desired config from KV")
}

/*
TC-KV-STORE-012
Type: Negative
Title: LoadDesiredConfig surfaces decode failures for malformed payloads
Summary:
Verifies that load path surfaces decode failures when KV entry payload is not
valid desired-config JSON.

Validates:
  - malformed JSON payload returns CodeDecodeFailed
*/
func TestLoadDesiredConfigSurfacesDecodeFailure(t *testing.T) {
	kvHandle := &stubKeyValue{
		getFn: func(context.Context, string) (jetstream.KeyValueEntry, error) {
			return stubKeyValueEntry{bucket: "cfg_desired", key: "desired.vyos", revision: 1, value: []byte(`{"version":`)}, nil
		},
	}
	store, err := NewStore(&stubRuntimeProvider{kv: kvHandle}, nil)
	if err != nil {
		t.Fatalf("expected nil constructor error, got %v", err)
	}

	stored, err := store.LoadDesiredConfig(context.Background(), "vyos")
	if stored != nil {
		t.Fatalf("expected nil stored result, got %#v", stored)
	}
	requireKVRuntimeError(t, err, runtimeerr.CodeDecodeFailed, "load_desired_config", "failed to decode desired config")
}

/*
TC-KV-STORE-013
Type: Positive
Title: LoadDesiredConfig returns decoded record and entry metadata
Summary:
Verifies that load path decodes desired-config payload and returns key metadata
from KV entry.

Validates:
  - decoded record fields are preserved
  - bucket key revision and created_at metadata are returned
*/
func TestLoadDesiredConfigReturnsDecodedRecordAndMetadata(t *testing.T) {
	rec := validDesiredConfigRecordForTests()
	payload, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("failed to marshal test record: %v", err)
	}
	createdAt := rec.Timestamp.Add(3 * time.Second)

	kvHandle := &stubKeyValue{
		getFn: func(context.Context, string) (jetstream.KeyValueEntry, error) {
			return stubKeyValueEntry{
				bucket:   "cfg_desired",
				key:      "desired.vyos",
				revision: 41,
				created:  createdAt,
				value:    payload,
			}, nil
		},
	}
	store, err := NewStore(&stubRuntimeProvider{kv: kvHandle}, nil)
	if err != nil {
		t.Fatalf("expected nil constructor error, got %v", err)
	}

	stored, err := store.LoadDesiredConfig(context.Background(), "vyos")
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if stored == nil {
		t.Fatal("expected non-nil stored result")
	}
	if stored.Record.UUID != rec.UUID || stored.Record.RPCID != rec.RPCID || stored.Record.Target != rec.Target {
		t.Fatalf("expected decoded record identifiers %+v, got %+v", rec, stored.Record)
	}
	if stored.Bucket != "cfg_desired" || stored.Key != "desired.vyos" || stored.Revision != 41 {
		t.Fatalf("unexpected metadata bucket=%q key=%q revision=%d", stored.Bucket, stored.Key, stored.Revision)
	}
	if !stored.CreatedAt.Equal(createdAt) {
		t.Fatalf("expected CreatedAt %v, got %v", createdAt, stored.CreatedAt)
	}
}

/*
TC-KV-STORE-014
Type: Negative
Title: decodeDesiredConfigRecord rejects empty and malformed payloads
Summary:
Verifies that decode helper rejects missing bytes and malformed JSON payload
content with typed errors.

Validates:
  - empty payload returns CodeValidation
  - malformed payload returns CodeDecodeFailed
*/
func TestDecodeDesiredConfigRecordRejectsEmptyAndMalformedPayloads(t *testing.T) {
	_, err := decodeDesiredConfigRecord(nil)
	requireKVRuntimeError(t, err, runtimeerr.CodeValidation, "load_desired_config", "payload is required")

	_, err = decodeDesiredConfigRecord([]byte(`{"version":`))
	requireKVRuntimeError(t, err, runtimeerr.CodeDecodeFailed, "load_desired_config", "failed to decode desired config")
}

/*
TC-KV-STORE-015
Type: Negative
Title: encodeDesiredConfigRecord rejects invalid desired-config records
Summary:
Verifies that encode helper validates records before JSON encoding and rejects
invalid desired-config inputs.

Validates:
  - invalid record returns CodeValidation
  - validate_desired_config_record op is preserved
*/
func TestEncodeDesiredConfigRecordRejectsInvalidRecord(t *testing.T) {
	rec := validDesiredConfigRecordForTests()
	rec.Target = ""

	payload, err := encodeDesiredConfigRecord(rec)
	if payload != nil {
		t.Fatalf("expected nil payload, got %q", string(payload))
	}
	requireKVRuntimeError(t, err, runtimeerr.CodeValidation, "validate_desired_config_record", "target is required")
}

/*
TC-KV-STORE-016
Type: Positive
Title: isConfigNotFound recognizes supported not-found and deleted forms
Summary:
Verifies that not-found helper recognizes typed key-not-found and key-deleted
errors along with message-based compatibility forms.

Validates:
  - typed not-found and deleted errors are recognized
  - message-based not-found and deleted forms are recognized
  - unrelated errors are not recognized
*/
func TestIsConfigNotFoundRecognizesSupportedForms(t *testing.T) {
	if !isConfigNotFound(nats.ErrKeyNotFound) {
		t.Fatal("expected nats.ErrKeyNotFound to be recognized")
	}
	if !isConfigNotFound(nats.ErrKeyDeleted) {
		t.Fatal("expected nats.ErrKeyDeleted to be recognized")
	}
	if !isConfigNotFound(jetstream.ErrKeyNotFound) {
		t.Fatal("expected jetstream.ErrKeyNotFound to be recognized")
	}
	if !isConfigNotFound(jetstream.ErrKeyDeleted) {
		t.Fatal("expected jetstream.ErrKeyDeleted to be recognized")
	}
	if !isConfigNotFound(errors.New("key not found")) {
		t.Fatal("expected message-based key not found to be recognized")
	}
	if !isConfigNotFound(errors.New("key was deleted")) {
		t.Fatal("expected message-based key deleted to be recognized")
	}
	if isConfigNotFound(errors.New("permission denied")) {
		t.Fatal("expected unrelated error to be rejected")
	}
}

/*
TC-KV-STORE-017
Type: Positive
Title: withTimeout preserves existing deadline and adds new deadline when needed
Summary:
Verifies that timeout helper keeps existing deadlines and adds configured
timeouts only for contexts without deadlines.

Validates:
  - existing deadline is preserved
  - new deadline is added when missing
*/
func TestWithTimeoutPreservesOrAddsDeadline(t *testing.T) {
	ctxWithDeadline, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	wantDeadline, _ := ctxWithDeadline.Deadline()

	preservedCtx, preservedCancel := withTimeout(ctxWithDeadline, 10*time.Millisecond)
	defer preservedCancel()
	gotDeadline, ok := preservedCtx.Deadline()
	if !ok {
		t.Fatal("expected deadline on preserved context")
	}
	if !gotDeadline.Equal(wantDeadline) {
		t.Fatalf("expected preserved deadline %v, got %v", wantDeadline, gotDeadline)
	}

	start := time.Now()
	addedCtx, addedCancel := withTimeout(context.Background(), 30*time.Millisecond)
	defer addedCancel()
	addedDeadline, ok := addedCtx.Deadline()
	if !ok {
		t.Fatal("expected timeout helper to add deadline")
	}
	if addedDeadline.Before(start.Add(20*time.Millisecond)) || addedDeadline.After(start.Add(100*time.Millisecond)) {
		t.Fatalf("expected added deadline near 30ms from start, got %v", addedDeadline.Sub(start))
	}
}
