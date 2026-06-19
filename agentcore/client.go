package agentcore

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Telecominfraproject/olg-nats-agent-core/internal/kv"
	"github.com/Telecominfraproject/olg-nats-agent-core/internal/registry"
	"github.com/Telecominfraproject/olg-nats-agent-core/internal/runtimeerr"
	"github.com/Telecominfraproject/olg-nats-agent-core/internal/session"
	"github.com/Telecominfraproject/olg-nats-agent-core/internal/subjects"
	"github.com/Telecominfraproject/olg-nats-agent-core/internal/transport"
)

// ConfigureHandler handles configure notifications for a target.
type ConfigureHandler func(context.Context, ConfigureNotification) error

// ActionHandler handles action commands for a target and action name.
type ActionHandler func(context.Context, ActionCommand) error

// ResultHandler handles result messages for a target.
type ResultHandler func(context.Context, ResultEnvelope) error

// StatusHandler handles status messages for a target.
type StatusHandler func(context.Context, StatusEnvelope) error

// DesiredConfigWatchHandler handles desired-config watch updates.
type DesiredConfigWatchHandler func(context.Context, StoredDesiredConfig) error

// StopFunc stops a watch registration created by a public API.
type StopFunc func() error

// SubscriptionOption configures a public subscription registration.
type SubscriptionOption func(*SubscriptionOptions)

// SubscriptionOptions contains public subscription registration settings.
type SubscriptionOptions struct {
	QueueGroup string
}

type clientOptions struct {
	logger    Logger
	metrics   Metrics
	now       func() time.Time
	errorSink func(error)
}

// Option applies an optional public client setting during construction.
type Option func(*clientOptions) error

// WithLogger injects a logger into the client.
func WithLogger(l Logger) Option {
	return func(opts *clientOptions) error {
		opts.logger = l
		return nil
	}
}

// WithMetrics injects metrics hooks into the client.
func WithMetrics(m Metrics) Option {
	return func(opts *clientOptions) error {
		opts.metrics = m
		return nil
	}
}

// WithClock overrides the clock used by bootstrap defaults.
func WithClock(now func() time.Time) Option {
	return func(opts *clientOptions) error {
		if now == nil {
			return &Error{Code: CodeValidation, Op: "with_clock", Message: "clock function is nil"}
		}
		opts.now = now
		return nil
	}
}

// WithErrorSink injects a best-effort async error sink hook.
func WithErrorSink(fn func(error)) Option {
	return func(opts *clientOptions) error {
		opts.errorSink = fn
		return nil
	}
}

type activeWatch struct {
	id      uint64
	target  string
	handler DesiredConfigWatchHandler
	stopFn  StopFunc
}

// Client is the public facade used by agent processes.
type Client struct {
	mu      sync.RWMutex
	cfg     Config
	options clientOptions

	session *session.Manager
	kv      *kv.Store

	subMu         sync.Mutex
	subscriptions *registry.Registry
	subjects      *subjects.Builder
	publisher     publisher
	handlerCtx    context.Context
	handlerCancel context.CancelFunc

	nextWatchID   uint64
	watchMu       sync.Mutex
	activeWatches map[uint64]*activeWatch

	callbacksEnabled atomic.Bool

	startSessionFn               func(context.Context) error
	activateAllSubscriptionsFn   func(string) error
	deactivateAllSubscriptionsFn func(string) error
	storeDesiredConfigFn         func(context.Context, DesiredConfigRecord) (*StoredDesiredConfig, error)
	closeSessionFn               func(context.Context) error
}

type publisher interface {
	Publish(ctx context.Context, op, kind, subject string, payload []byte) error
}

// New validates public options and constructs a bootstrap client facade.
func New(cfg Config, opts ...Option) (*Client, error) {
	options := clientOptions{
		now: time.Now,
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&options); err != nil {
			return nil, err
		}
	}

	if options.errorSink != nil {
		origSink := options.errorSink
		options.errorSink = func(err error) {
			origSink(toPublicError(err))
		}
	}

	if options.logger == nil {
		options.logger = cfg.Observe.Logger
	}
	if options.metrics == nil {
		options.metrics = cfg.Observe.Metrics
	}

	subPatterns, err := subjects.PatternsFromConfig(subjects.Config{
		ConfigurePattern: cfg.Subjects.ConfigurePattern,
		ActionPattern:    cfg.Subjects.ActionPattern,
		ResultPattern:    cfg.Subjects.ResultPattern,
		StatusPattern:    cfg.Subjects.StatusPattern,
		HealthPattern:    cfg.Subjects.HealthPattern,
	})
	if err != nil {
		return nil, toPublicError(err)
	}

	subjectBuilder, err := subjects.NewBuilder(subPatterns)
	if err != nil {
		return nil, toPublicError(err)
	}

	runtime, err := session.NewManager(toSessionConfig(cfg), session.Hooks{
		Logger:    options.logger,
		Metrics:   options.metrics,
		ErrorSink: options.errorSink,
	})
	if err != nil {
		return nil, toPublicError(err)
	}

	store, err := kv.NewStore(runtime, options.errorSink)
	if err != nil {
		return nil, toPublicError(err)
	}
	publisher, err := transport.NewPublisher(
		runtime,
		func() time.Duration {
			return runtime.EffectiveConfig().Timeouts.PublishTimeout
		},
		func() int {
			return runtime.EffectiveConfig().Retry.PublishAttempts
		},
		func() time.Duration {
			return runtime.EffectiveConfig().Retry.PublishBackoff
		},
		options.metrics,
	)
	if err != nil {
		return nil, toPublicError(err)
	}

	client := &Client{
		cfg:           cfg,
		options:       options,
		session:       runtime,
		kv:            store,
		subscriptions: registry.New(),
		subjects:      subjectBuilder,
		publisher:     publisher,
		activeWatches: make(map[uint64]*activeWatch),
	}
	client.syncSubscriptionHealth()
	runtime.SetReconnectHandler(client.onSessionReconnected)
	runtime.SetClosedHandler(client.onSessionClosed)

	return client, nil
}

// Config returns the bootstrap configuration snapshot.
func (c *Client) Config() Config {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cfg
}

// Start begins the client lifecycle.
func (c *Client) Start(ctx context.Context) error {
	if err := c.startSession(ctx); err != nil {
		return err
	}

	if err := c.activateAllSubscriptions("start"); err != nil {
		c.callbacksEnabled.Store(false)
		c.cancelHandlerContext()
		if cleanupErr := c.deactivateAllSubscriptionsWithOp("start_activation_cleanup"); cleanupErr != nil {
			c.logWarn("failed to cleanup subscriptions after start activation failure", "error", cleanupErr)
			if c.options.errorSink != nil {
				c.options.errorSink(cleanupErr)
			}
		}
		return err
	}

	c.ensureHandlerContext()
	c.callbacksEnabled.Store(true)
	return nil
}

// Close ends the client lifecycle with watch cleanup and connection drain.
func (c *Client) Close(ctx context.Context) error {
	c.callbacksEnabled.Store(false)
	c.cancelHandlerContext()
	subErr := c.deactivateAllSubscriptionsWithOp("close")
	watchErr := c.stopAllWatches()
	sessionErr := toPublicError(c.closeSession(ctx))

	if subErr != nil || watchErr != nil || sessionErr != nil {
		joined := errors.Join(subErr, watchErr, sessionErr)
		return &Error{
			Code:      CodeShutdown,
			Op:        "close",
			Message:   "client close operation encountered errors",
			Retryable: false,
			Err:       joined,
		}
	}
	return nil
}

func (c *Client) startSession(ctx context.Context) error {
	if c.startSessionFn != nil {
		return c.startSessionFn(ctx)
	}
	return toPublicError(c.session.Start(ctx))
}

func (c *Client) closeSession(ctx context.Context) error {
	if c.closeSessionFn != nil {
		return c.closeSessionFn(ctx)
	}
	return c.session.Close(ctx)
}

func (c *Client) activateAllSubscriptions(op string) error {
	if c.activateAllSubscriptionsFn != nil {
		return c.activateAllSubscriptionsFn(op)
	}
	return c.activateAllRegisteredSubscriptions(op)
}

func (c *Client) deactivateAllSubscriptionsWithOp(op string) error {
	if c.deactivateAllSubscriptionsFn != nil {
		return c.deactivateAllSubscriptionsFn(op)
	}
	return c.deactivateAllSubscriptions(op)
}

func (c *Client) ensureHandlerContext() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.handlerCtx != nil {
		select {
		case <-c.handlerCtx.Done():
			// Existing lifecycle context is canceled and must be replaced.
		default:
			// Existing lifecycle context is still active; keep it.
			return
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.handlerCtx = ctx
	c.handlerCancel = cancel
}

func (c *Client) cancelHandlerContext() {
	c.mu.Lock()
	cancel := c.handlerCancel
	c.handlerCtx = nil
	c.handlerCancel = nil
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}
}

func (c *Client) handlerContext() context.Context {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.handlerCtx != nil {
		return c.handlerCtx
	}
	return context.Background()
}

// Health returns the latest public health snapshot.
func (c *Client) Health() HealthSnapshot {
	if c.session == nil {
		return HealthSnapshot{State: StateNew}
	}
	return fromSessionHealth(c.session.HealthSnapshot())
}

// SubmitConfigure stores desired configuration in KV and publishes a configure notification.
// The operation is store-then-notify and is not atomic across KV and NATS publish;
// if notification publish fails after KV storage succeeds, the stored desired config remains.
func (c *Client) SubmitConfigure(ctx context.Context, cmd ConfigureCommand) (*SubmissionAck, error) {
	const op = "submit_configure"

	if err := validateOperationContext(op, ctx); err != nil {
		return nil, err
	}
	if err := validateConfigureCommand(op, cmd); err != nil {
		return nil, err
	}
	stored, err := c.StoreDesiredConfig(ctx, DesiredConfigRecord{
		Version:   cmd.Version,
		RPCID:     cmd.RPCID,
		Target:    cmd.Target,
		UUID:      cmd.UUID,
		Payload:   cmd.Payload,
		Timestamp: cmd.Timestamp,
	})
	if err != nil {
		return nil, err
	}
	if stored == nil {
		return nil, &Error{
			Code:      CodeKVStoreFailed,
			Op:        "submit_configure_store_desired",
			Message:   "desired config store returned nil result",
			Retryable: true,
		}
	}

	subject, err := c.subjects.ConfigureSubject(cmd.Target)
	if err != nil {
		return nil, toPublicError(err)
	}

	notification := ConfigureNotification{
		Version:     cmd.Version,
		RPCID:       cmd.RPCID,
		Target:      cmd.Target,
		CommandType: "configure",
		UUID:        cmd.UUID,
		KVBucket:    stored.Bucket,
		KVKey:       stored.Key,
		Timestamp:   c.options.now().UTC(),
	}
	if err := validateConfigureNotification(op, notification); err != nil {
		return nil, err
	}

	payload, err := json.Marshal(notification)
	if err != nil {
		return nil, &Error{
			Code:      CodeEncodeFailed,
			Op:        "submit_configure_encode_notification",
			Subject:   subject,
			Message:   "failed to encode configure notification",
			Retryable: false,
			Err:       err,
		}
	}

	if err := c.publisher.Publish(ctx, "submit_configure_publish_notification", string(registry.KindConfigure), subject, payload); err != nil {
		return nil, toPublicError(err)
	}

	return &SubmissionAck{
		Accepted:   true,
		RPCID:      cmd.RPCID,
		Target:     cmd.Target,
		Subject:    subject,
		AcceptedAt: notification.Timestamp,
		KVBucket:   stored.Bucket,
		KVKey:      stored.Key,
		KVRevision: stored.Revision,
	}, nil
}

// SubmitAction publishes an action command to the target action subject.
func (c *Client) SubmitAction(ctx context.Context, cmd ActionCommand) (*SubmissionAck, error) {
	const op = "submit_action"

	if err := validateOperationContext(op, ctx); err != nil {
		return nil, err
	}
	if err := validateActionCommand(op, cmd); err != nil {
		return nil, err
	}
	subject, err := c.subjects.ActionSubject(cmd.Target, cmd.Action)
	if err != nil {
		return nil, toPublicError(err)
	}

	payload, err := json.Marshal(cmd)
	if err != nil {
		return nil, &Error{
			Code:      CodeEncodeFailed,
			Op:        "submit_action_encode",
			Subject:   subject,
			Message:   "failed to encode action command",
			Retryable: false,
			Err:       err,
		}
	}

	if err := c.publisher.Publish(ctx, "submit_action_publish", string(registry.KindAction), subject, payload); err != nil {
		return nil, toPublicError(err)
	}

	return &SubmissionAck{
		Accepted:   true,
		RPCID:      cmd.RPCID,
		Target:     cmd.Target,
		Subject:    subject,
		AcceptedAt: c.options.now().UTC(),
	}, nil
}

// PublishResult publishes a result envelope to the target result subject.
func (c *Client) PublishResult(ctx context.Context, msg ResultEnvelope) error {
	const op = "publish_result"

	if err := validateOperationContext(op, ctx); err != nil {
		return err
	}
	if err := validateResultEnvelope(op, msg); err != nil {
		return err
	}
	subject, err := c.subjects.ResultSubject(msg.Target)
	if err != nil {
		return toPublicError(err)
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return &Error{
			Code:      CodeEncodeFailed,
			Op:        "publish_result_encode",
			Subject:   subject,
			Message:   "failed to encode result envelope",
			Retryable: false,
			Err:       err,
		}
	}

	if err := c.publisher.Publish(ctx, op, string(registry.KindResult), subject, payload); err != nil {
		return toPublicError(err)
	}
	return nil
}

// PublishStatus publishes a status envelope to the target status subject.
func (c *Client) PublishStatus(ctx context.Context, msg StatusEnvelope) error {
	const op = "publish_status"

	if err := validateOperationContext(op, ctx); err != nil {
		return err
	}
	if err := validateStatusEnvelope(op, msg); err != nil {
		return err
	}
	subject, err := c.subjects.StatusSubject(msg.Target)
	if err != nil {
		return toPublicError(err)
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return &Error{
			Code:      CodeEncodeFailed,
			Op:        "publish_status_encode",
			Subject:   subject,
			Message:   "failed to encode status envelope",
			Retryable: false,
			Err:       err,
		}
	}

	if err := c.publisher.Publish(ctx, op, string(registry.KindStatus), subject, payload); err != nil {
		return toPublicError(err)
	}
	return nil
}

// StoreDesiredConfig writes desired configuration to JetStream KV.
func (c *Client) StoreDesiredConfig(ctx context.Context, rec DesiredConfigRecord) (*StoredDesiredConfig, error) {
	if c.storeDesiredConfigFn != nil {
		return c.storeDesiredConfigFn(ctx, rec)
	}
	stored, err := c.kv.StoreDesiredConfig(ctx, toKVRecord(rec))
	if err != nil {
		return nil, toPublicError(err)
	}
	return fromKVStored(stored), nil
}

// LoadDesiredConfig loads desired configuration from JetStream KV.
func (c *Client) LoadDesiredConfig(ctx context.Context, target string) (*StoredDesiredConfig, error) {
	stored, err := c.kv.LoadDesiredConfig(ctx, target)
	if err != nil {
		return nil, toPublicError(err)
	}
	return fromKVStored(stored), nil
}

// WatchDesiredConfig registers a desired-config watch scoped to a single target.
func (c *Client) WatchDesiredConfig(ctx context.Context, target string, handler DesiredConfigWatchHandler) (StopFunc, error) {
	if handler == nil {
		return nil, &Error{
			Code:      CodeValidation,
			Op:        "watch_desired_config",
			Message:   "watch handler is required",
			Retryable: false,
		}
	}

	c.watchMu.Lock()
	id := c.nextWatchID + 1
	c.nextWatchID = id

	intent := &activeWatch{
		id:      id,
		target:  target,
		handler: handler,
	}
	c.activeWatches[id] = intent
	c.watchMu.Unlock()

	buildWatch := func() (StopFunc, error) {
		stop, err := c.kv.WatchDesiredConfig(ctx, target, func(watchCtx context.Context, stored kv.StoredDesiredConfig) error {
			return handler(watchCtx, StoredDesiredConfig{
				Record: DesiredConfigRecord{
					Version:   stored.Record.Version,
					RPCID:     stored.Record.RPCID,
					Target:    stored.Record.Target,
					UUID:      stored.Record.UUID,
					Payload:   json.RawMessage(stored.Record.Payload),
					Timestamp: stored.Record.Timestamp,
				},
				Bucket:    stored.Bucket,
				Key:       stored.Key,
				Revision:  stored.Revision,
				CreatedAt: stored.CreatedAt,
			})
		})
		if err != nil {
			return nil, err
		}
		return StopFunc(stop), nil
	}

	stop, err := buildWatch()
	if err != nil {
		c.watchMu.Lock()
		delete(c.activeWatches, id)
		c.watchMu.Unlock()
		return nil, toPublicError(err)
	}

	c.watchMu.Lock()
	intent.stopFn = stop
	c.watchMu.Unlock()

	var once sync.Once
	return func() error {
		var stopErr error
		once.Do(func() {
			c.watchMu.Lock()
			current := c.activeWatches[id]
			delete(c.activeWatches, id)
			c.watchMu.Unlock()

			if current != nil && current.stopFn != nil {
				stopErr = current.stopFn()
			}
		})
		return stopErr
	}, nil
}

// StartupReconcile loads latest desired state during recovery.
func (c *Client) StartupReconcile(ctx context.Context, target string) (*StoredDesiredConfig, error) {
	return c.LoadDesiredConfig(ctx, target)
}

// RegisterConfigureHandler registers a configure notification handler.
func (c *Client) RegisterConfigureHandler(target string, handler ConfigureHandler, opts ...SubscriptionOption) error {
	return c.registerConfigureHandler(target, handler, opts...)
}

// RegisterActionHandler registers a target/action handler.
func (c *Client) RegisterActionHandler(target, action string, handler ActionHandler, opts ...SubscriptionOption) error {
	return c.registerActionHandler(target, action, handler, opts...)
}

// RegisterResultHandler registers a result handler.
func (c *Client) RegisterResultHandler(target string, handler ResultHandler, opts ...SubscriptionOption) error {
	return c.registerResultHandler(target, handler, opts...)
}

// RegisterStatusHandler registers a status handler.
func (c *Client) RegisterStatusHandler(target string, handler StatusHandler, opts ...SubscriptionOption) error {
	return c.registerStatusHandler(target, handler, opts...)
}

func (c *Client) restoreAllActiveWatches() error {
	c.watchMu.Lock()
	defer c.watchMu.Unlock()

	var joined error
	for _, intent := range c.activeWatches {
		c.logInfo("restoring KV watch", "target", intent.target)
		if intent.stopFn != nil {
			_ = intent.stopFn()
		}

		handler := intent.handler
		stop, err := c.kv.WatchDesiredConfig(context.Background(), intent.target, func(watchCtx context.Context, stored kv.StoredDesiredConfig) error {
			return handler(watchCtx, StoredDesiredConfig{
				Record: DesiredConfigRecord{
					Version:   stored.Record.Version,
					RPCID:     stored.Record.RPCID,
					Target:    stored.Record.Target,
					UUID:      stored.Record.UUID,
					Payload:   json.RawMessage(stored.Record.Payload),
					Timestamp: stored.Record.Timestamp,
				},
				Bucket:    stored.Bucket,
				Key:       stored.Key,
				Revision:  stored.Revision,
				CreatedAt: stored.CreatedAt,
			})
		})
		if err != nil {
			joined = errors.Join(joined, err)
			c.logError("failed to restore KV watch", "target", intent.target, "error", err)
			if c.options.errorSink != nil {
				c.options.errorSink(err)
			}
			continue
		}
		intent.stopFn = StopFunc(stop)
	}
	return joined
}

func (c *Client) stopAllWatches() error {
	c.watchMu.Lock()
	stops := make([]StopFunc, 0, len(c.activeWatches))
	for _, w := range c.activeWatches {
		if w.stopFn != nil {
			stops = append(stops, w.stopFn)
		}
	}
	c.activeWatches = make(map[uint64]*activeWatch)
	c.watchMu.Unlock()

	var joined error
	for _, stop := range stops {
		if stop == nil {
			continue
		}
		if err := stop(); err != nil {
			if joined == nil {
				joined = err
			} else {
				joined = errors.Join(joined, err)
			}
		}
	}

	return joined
}

func toSessionConfig(cfg Config) session.Config {
	return session.Config{
		AgentName: cfg.AgentName,
		NATS: session.NATSConfig{
			Servers:              append([]string(nil), cfg.NATS.Servers...),
			ClientName:           cfg.NATS.ClientName,
			CredentialsFile:      cfg.NATS.CredentialsFile,
			NKeySeedFile:         cfg.NATS.NKeySeedFile,
			UserJWTFile:          cfg.NATS.UserJWTFile,
			Username:             cfg.NATS.Username,
			Password:             cfg.NATS.Password,
			Token:                cfg.NATS.Token,
			ConnectTimeout:       cfg.NATS.ConnectTimeout,
			RetryOnFailedConnect: cfg.NATS.RetryOnFailedConnect,
			MaxReconnects:        cfg.NATS.MaxReconnects,
			ReconnectWait:        cfg.NATS.ReconnectWait,
			ReconnectBufSize:     cfg.NATS.ReconnectBufSize,
			TLS:                  toSessionTLS(cfg.NATS.TLS),
		},
		JetStream: session.JetStreamConfig{
			Domain:         cfg.JetStream.Domain,
			APIPrefix:      cfg.JetStream.APIPrefix,
			DefaultTimeout: cfg.JetStream.DefaultTimeout,
		},
		KV: session.KVConfig{
			Bucket:           cfg.KV.Bucket,
			KeyPattern:       cfg.KV.KeyPattern,
			AutoCreateBucket: cfg.KV.AutoCreateBucket,
			History:          cfg.KV.History,
			TTL:              cfg.KV.TTL,
			MaxValueSize:     cfg.KV.MaxValueSize,
			Storage:          cfg.KV.Storage,
			Replicas:         cfg.KV.Replicas,
		},
		Timeouts: session.TimeoutConfig{
			PublishTimeout:   cfg.Timeouts.PublishTimeout,
			SubscribeTimeout: cfg.Timeouts.SubscribeTimeout,
			KVTimeout:        cfg.Timeouts.KVTimeout,
			ShutdownTimeout:  cfg.Timeouts.ShutdownTimeout,
			HandlerWarnAfter: cfg.Timeouts.HandlerWarnAfter,
		},
		Retry: session.RetryConfig{
			PublishAttempts: cfg.Retry.PublishAttempts,
			PublishBackoff:  cfg.Retry.PublishBackoff,
		},
	}
}

func toSessionTLS(cfg *TLSConfig) *session.TLSConfig {
	if cfg == nil {
		return nil
	}
	return &session.TLSConfig{
		Enabled:            cfg.Enabled,
		InsecureSkipVerify: cfg.InsecureSkipVerify,
		CAFile:             cfg.CAFile,
		CertFile:           cfg.CertFile,
		KeyFile:            cfg.KeyFile,
		ServerName:         cfg.ServerName,
	}
}

func toKVRecord(rec DesiredConfigRecord) kv.DesiredConfigRecord {
	return kv.DesiredConfigRecord{
		Version:   rec.Version,
		RPCID:     rec.RPCID,
		Target:    rec.Target,
		UUID:      rec.UUID,
		Payload:   json.RawMessage(rec.Payload),
		Timestamp: rec.Timestamp,
	}
}

func fromKVStored(stored *kv.StoredDesiredConfig) *StoredDesiredConfig {
	if stored == nil {
		return nil
	}
	return &StoredDesiredConfig{
		Record: DesiredConfigRecord{
			Version:   stored.Record.Version,
			RPCID:     stored.Record.RPCID,
			Target:    stored.Record.Target,
			UUID:      stored.Record.UUID,
			Payload:   json.RawMessage(stored.Record.Payload),
			Timestamp: stored.Record.Timestamp,
		},
		Bucket:    stored.Bucket,
		Key:       stored.Key,
		Revision:  stored.Revision,
		CreatedAt: stored.CreatedAt,
	}
}

func fromSessionHealth(snapshot session.HealthSnapshot) HealthSnapshot {
	return HealthSnapshot{
		State:                   ConnectionState(snapshot.State),
		ConnectedURL:            snapshot.ConnectedURL,
		JetStreamReady:          snapshot.JetStreamReady,
		KVReady:                 snapshot.KVReady,
		RegisteredSubscriptions: snapshot.RegisteredSubscriptions,
		ActiveSubscriptions:     snapshot.ActiveSubscriptions,
		LastError:               snapshot.LastError,
	}
}

func toPublicError(err error) error {
	if err == nil {
		return nil
	}

	var internal *runtimeerr.Error
	if !errors.As(err, &internal) {
		return err
	}

	return &Error{
		Code:      Code(internal.Code),
		Op:        internal.Op,
		Subject:   internal.Subject,
		Key:       internal.Key,
		Message:   internal.Message,
		Retryable: internal.Retryable,
		Err:       internal.Err,
	}
}

func validateConfigureCommand(op string, cmd ConfigureCommand) error {
	if err := requiredString(op, "version", cmd.Version); err != nil {
		return err
	}
	if err := requiredString(op, "rpc_id", cmd.RPCID); err != nil {
		return err
	}
	if err := requiredString(op, "target", cmd.Target); err != nil {
		return err
	}
	if err := requiredString(op, "uuid", cmd.UUID); err != nil {
		return err
	}
	if err := requiredTimestamp(op, "timestamp", cmd.Timestamp); err != nil {
		return err
	}
	return requiredJSON(op, "payload", cmd.Payload)
}

func validateOperationContext(op string, ctx context.Context) error {
	if ctx == nil {
		return validationError(op, "context is required")
	}
	if err := ctx.Err(); err != nil {
		return &Error{
			Code:      CodeValidation,
			Op:        op,
			Message:   "context is not usable",
			Retryable: false,
			Err:       err,
		}
	}
	return nil
}
