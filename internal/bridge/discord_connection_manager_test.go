package bridge

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	discord "github.com/stieneee/mumble-discord-bridge/internal/discord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiscord_RetryBudgetEndsInFailed(t *testing.T) {
	client := &mockDiscordClientForConn{}
	mgr, _, _ := newTestDiscordManager(client)
	mgr.config = &ConnectionManagerConfig{MaxRetries: 2, BaseRetryDelay: time.Millisecond, MaxRetryDelay: 2 * time.Millisecond, RetryMultiplier: 2}
	attempts := 0
	mgr.connectFunc = func() error {
		attempts++
		return errors.New("connect failed")
	}
	done := make(chan struct{})
	go func() {
		mgr.mainConnectionLoop(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Discord retry budget did not terminate")
	}
	assert.Equal(t, 3, attempts)
	assert.Equal(t, ConnectionFailed, mgr.GetStatus())
}

func TestDiscord_RetryBudgetResetsAfterSuccess(t *testing.T) {
	client := &mockDiscordClientForConn{}
	mgr, _, _ := newTestDiscordManager(client)
	mgr.config = &ConnectionManagerConfig{MaxRetries: 1, BaseRetryDelay: time.Millisecond, MaxRetryDelay: time.Millisecond, RetryMultiplier: 2}
	attempts := 0
	mgr.connectFunc = func() error {
		attempts++
		if attempts == 1 || attempts == 3 {
			return errors.New("transient")
		}
		mgr.SetStatus(ConnectionConnected, nil)
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	monitorCalls := 0
	mgr.monitorFunc = func(ctx context.Context) {
		monitorCalls++
		if monitorCalls == 2 {
			cancel()
			<-ctx.Done()
		}
	}
	done := make(chan struct{})
	go func() {
		mgr.mainConnectionLoop(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("Discord reset test did not terminate")
	}
	assert.Equal(t, 4, attempts, "each post-success episode must receive a fresh retry budget")
	assert.Equal(t, 2, monitorCalls)
}

// ---------------------------------------------------------------------------
// Mock types for Discord client and voice connection
// ---------------------------------------------------------------------------

// mockDiscordClientForConn implements discord.Client for connection manager tests.
type mockDiscordClientForConn struct {
	ready     bool
	mu        sync.Mutex
	voiceConn *mockVoiceConn
}

func (m *mockDiscordClientForConn) Connect(_ context.Context) error { return nil }

func (m *mockDiscordClientForConn) Disconnect(_ context.Context) error { return nil }

func (m *mockDiscordClientForConn) SendMessage(_, _ string) error { return nil }

func (m *mockDiscordClientForConn) GetUser(_ string) (*discord.User, error) {
	return &discord.User{ID: "mock-user"}, nil
}

func (m *mockDiscordClientForConn) CreateDM(_ string) (string, error) { return "dm-channel", nil }

func (m *mockDiscordClientForConn) GetGuild(_ string) (*discord.Guild, error) {
	return &discord.Guild{ID: "mock-guild"}, nil
}

func (m *mockDiscordClientForConn) GetBotUserID() string { return "bot-user-id" }

func (m *mockDiscordClientForConn) AddEventHandler(_ discord.EventHandler) func() { return func() {} }

func (m *mockDiscordClientForConn) IsReady() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ready
}

func (m *mockDiscordClientForConn) setReady(ready bool) {
	m.mu.Lock()
	m.ready = ready
	m.mu.Unlock()
}

func (m *mockDiscordClientForConn) CreateVoiceConnection(_ string) (discord.VoiceConnection, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.voiceConn, nil
}

// mockVoiceConn implements discord.VoiceConnection for connection manager tests.
type mockVoiceConn struct {
	ready         bool
	gatewayReady  bool
	opened        bool
	closed        bool
	closeCalls    int
	mu            sync.Mutex
	openErr       error
	openedChannel string
}

func (m *mockVoiceConn) Open(_ context.Context, channelID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.opened = true
	m.openedChannel = channelID
	return m.openErr
}

func (m *mockVoiceConn) getOpenedChannel() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.openedChannel
}

func (m *mockVoiceConn) Close(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	m.closeCalls++
	return nil
}

func (m *mockVoiceConn) SendOpus(_ []byte) error { return nil }

func (m *mockVoiceConn) ReceiveOpus() (*discord.AudioPacket, error) {
	return nil, nil
}

func (m *mockVoiceConn) SetSpeaking(_ context.Context, _ bool) error { return nil }

func (m *mockVoiceConn) IsReady() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ready
}

func (m *mockVoiceConn) IsGatewayReady() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.gatewayReady
}

func (m *mockVoiceConn) setReady(ready bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ready = ready
	m.gatewayReady = ready
}

func (m *mockVoiceConn) setGatewayReady(ready bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gatewayReady = ready
}

func (m *mockVoiceConn) UserIDBySSRC(_ uint32) string { return "" }

// ---------------------------------------------------------------------------
// Helper to create a standard test manager
// ---------------------------------------------------------------------------

func newTestDiscordManager(client *mockDiscordClientForConn) (*DiscordVoiceConnectionManager, *MockLogger, *MockBridgeEventEmitter) {
	l := NewMockLogger()
	e := NewMockBridgeEventEmitter()
	mgr := NewDiscordVoiceConnectionManager(client, "test-guild", "test-channel", l, e)
	return mgr, l, e
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestDiscord_ClientNotReadyWaits verifies that the manager does not reach
// Connected status when the Discord client never becomes ready.
func TestDiscord_ClientNotReadyWaits(t *testing.T) {
	client := &mockDiscordClientForConn{
		ready:     false,
		voiceConn: &mockVoiceConn{},
	}
	mgr, _, _ := newTestDiscordManager(client)

	ctx, cancel := context.WithCancel(context.Background())

	err := mgr.Start(ctx)
	require.NoError(t, err)

	// Give the loop a moment to attempt connection, then cancel.
	time.Sleep(200 * time.Millisecond)
	cancel()

	// Allow goroutines to wind down.
	time.Sleep(100 * time.Millisecond)

	status := mgr.GetStatus()
	assert.NotEqual(t, ConnectionConnected, status,
		"Manager should not be Connected when client.IsReady() is false")

	require.NoError(t, mgr.Stop())
}

// TestDiscord_ConcurrentGetVoiceConnection ensures 100 goroutines can safely
// call GetVoiceConnection concurrently without data races or panics.
func TestDiscord_ConcurrentGetVoiceConnection(t *testing.T) {
	client := &mockDiscordClientForConn{
		ready:     false,
		voiceConn: &mockVoiceConn{},
	}
	mgr, _, _ := newTestDiscordManager(client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr.InitContext(ctx)

	runConcurrentlyWithTimeout(t, 5*time.Second, 100, func(_ int) {
		_ = mgr.GetVoiceConnection()
	})

	require.NoError(t, mgr.Stop())
}

// TestDiscord_UpdateChannel verifies that 50 goroutines can call UpdateChannel
// concurrently without data races.
func TestDiscord_UpdateChannel(t *testing.T) {
	client := &mockDiscordClientForConn{
		ready:     false,
		voiceConn: &mockVoiceConn{},
	}
	mgr, _, _ := newTestDiscordManager(client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr.InitContext(ctx)

	runConcurrentlyWithTimeout(t, 5*time.Second, 50, func(i int) {
		mgr.UpdateChannel(fmt.Sprintf("channel-%d", i))
	})

	require.NoError(t, mgr.Stop())
}

func TestDiscord_ConnectUsesSingleChannelSnapshotDuringConcurrentUpdate(t *testing.T) {
	voiceConn := &mockVoiceConn{}
	client := &mockDiscordClientForConn{voiceConn: voiceConn}
	mgr, log, _ := newTestDiscordManager(client)
	mgr.InitContext(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- mgr.connectOnce()
	}()

	require.Eventually(t, func() bool {
		return log.ContainsMessage("Channel=test-channel")
	}, time.Second, time.Millisecond, "connect did not capture and log its initial channel")

	runConcurrentlyWithTimeout(t, time.Second, 50, func(i int) {
		mgr.UpdateChannel(fmt.Sprintf("updated-channel-%d", i))
	})
	mgr.UpdateChannel("new-target")
	client.setReady(true)

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("connectOnce did not finish")
	}
	require.Equal(t, "test-channel", voiceConn.getOpenedChannel(), "log and Open must use one locked channel snapshot")
	require.NoError(t, mgr.Stop())
}

func TestDiscord_FailedOpenClosesCreatedAbstraction(t *testing.T) {
	voiceConn := &mockVoiceConn{openErr: errors.New("offline open failure")}
	client := &mockDiscordClientForConn{ready: true, voiceConn: voiceConn}
	mgr, _, _ := newTestDiscordManager(client)
	mgr.InitContext(context.Background())

	require.ErrorContains(t, mgr.connectOnce(), "failed to join voice channel")
	voiceConn.mu.Lock()
	closeCalls := voiceConn.closeCalls
	voiceConn.mu.Unlock()
	require.Equal(t, 1, closeCalls, "manager must defensively close a failed abstraction")
	require.Nil(t, mgr.GetVoiceConnection())
	require.NoError(t, mgr.Stop())
}

// TestDiscord_StopIdempotent ensures calling Stop() multiple times after cancel
// does not panic or return errors.
func TestDiscord_StopIdempotent(t *testing.T) {
	client := &mockDiscordClientForConn{
		ready:     false,
		voiceConn: &mockVoiceConn{},
	}
	mgr, _, _ := newTestDiscordManager(client)

	ctx, cancel := context.WithCancel(context.Background())
	mgr.InitContext(ctx)
	cancel()

	for i := 0; i < 5; i++ {
		err := mgr.Stop()
		require.NoError(t, err, "Stop call %d should not error", i)
	}
}

// TestDiscord_GetVoiceConnection_NilWhenNotConnected verifies that
// GetVoiceConnection returns nil before any connection is established.
func TestDiscord_GetVoiceConnection_NilWhenNotConnected(t *testing.T) {
	client := &mockDiscordClientForConn{
		ready:     false,
		voiceConn: &mockVoiceConn{},
	}
	mgr, _, _ := newTestDiscordManager(client)

	// Do not start or init context — no connection has been made.
	vc := mgr.GetVoiceConnection()
	assert.Nil(t, vc, "Expected nil VoiceConnection before starting")
}

// TestDiscord_GetVoiceConnection_ChecksReady returns nil unless the underlying
// voice connection and its gateway are both ready.
func TestDiscord_GetVoiceConnection_ChecksReady(t *testing.T) {
	vc := &mockVoiceConn{ready: false}
	client := &mockDiscordClientForConn{
		ready:     true,
		voiceConn: vc,
	}
	mgr, _, _ := newTestDiscordManager(client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr.InitContext(ctx)

	// Inject the voice connection directly (package-level access).
	mgr.connMutex.Lock()
	mgr.voiceConn = vc
	mgr.connMutex.Unlock()

	// Not ready yet.
	assert.Nil(t, mgr.GetVoiceConnection(),
		"Expected nil when voiceConn.IsReady() is false")

	// Local ready state alone is insufficient when the gateway is stale.
	vc.setReady(true)
	vc.setGatewayReady(false)
	assert.Nil(t, mgr.GetVoiceConnection(),
		"Expected nil when voiceConn.IsReady() is true but its gateway is not ready")

	// A fully ready connection is returned unchanged.
	vc.setGatewayReady(true)
	assert.Same(t, vc, mgr.GetVoiceConnection(),
		"Expected the same voice connection when local and gateway readiness are true")

	require.NoError(t, mgr.Stop())
}

// TestDiscord_IsConnectionHealthy_NilConn verifies isConnectionHealthy returns
// false when voiceConn is nil.
func TestDiscord_IsConnectionHealthy_NilConn(t *testing.T) {
	client := &mockDiscordClientForConn{
		ready:     false,
		voiceConn: &mockVoiceConn{},
	}
	mgr, _, _ := newTestDiscordManager(client)

	// voiceConn starts as nil in the manager.
	assert.False(t, mgr.isConnectionHealthy(),
		"Expected unhealthy when voiceConn is nil")
}

// TestDiscord_RapidStartStop runs 10 create/start/cancel/stop cycles to ensure
// no goroutine leaks or panics.
func TestDiscord_RapidStartStop(t *testing.T) {
	for i := 0; i < 10; i++ {
		client := &mockDiscordClientForConn{
			ready:     false,
			voiceConn: &mockVoiceConn{},
		}
		mgr, _, _ := newTestDiscordManager(client)

		ctx, cancel := context.WithCancel(context.Background())

		err := mgr.Start(ctx)
		require.NoError(t, err, "Start failed on cycle %d", i)

		// Give the goroutine time to enter the connection loop and register
		// its select on closedChan before we tear it down.
		time.Sleep(50 * time.Millisecond)

		cancel()
		err = mgr.Stop()
		require.NoError(t, err, "Stop failed on cycle %d", i)

		// Allow goroutines to fully wind down before next cycle.
		time.Sleep(20 * time.Millisecond)
	}
}

// TestDiscord_ConcurrentStatusReads ensures 100 goroutines can concurrently
// read GetStatus() and IsConnected() without data races.
func TestDiscord_ConcurrentStatusReads(t *testing.T) {
	client := &mockDiscordClientForConn{
		ready:     false,
		voiceConn: &mockVoiceConn{},
	}
	mgr, _, _ := newTestDiscordManager(client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr.InitContext(ctx)

	runConcurrentlyWithTimeout(t, 5*time.Second, 100, func(_ int) {
		_ = mgr.GetStatus()
		_ = mgr.IsConnected()
	})

	require.NoError(t, mgr.Stop())
}

// TestDiscord_EventHandlerRegistration exercises the InitContext + Stop
// lifecycle to verify no panics during basic event handler setup.
func TestDiscord_EventHandlerRegistration(t *testing.T) {
	client := &mockDiscordClientForConn{
		ready:     false,
		voiceConn: &mockVoiceConn{},
	}
	mgr, _, _ := newTestDiscordManager(client)

	ctx, cancel := context.WithCancel(context.Background())
	mgr.InitContext(ctx)

	// Verify that we can read status after init.
	status := mgr.GetStatus()
	assert.Equal(t, ConnectionDisconnected, status)

	cancel()
	err := mgr.Stop()
	require.NoError(t, err)
}

// TestDiscord_IsConnectionHealthy_WithNilConnection is an explicit test that
// isConnectionHealthy returns false when voiceConn is nil (complementary to
// TestDiscord_IsConnectionHealthy_NilConn with a different setup path).
func TestDiscord_IsConnectionHealthy_WithNilConnection(t *testing.T) {
	client := &mockDiscordClientForConn{
		ready:     true,
		voiceConn: &mockVoiceConn{},
	}
	mgr, _, _ := newTestDiscordManager(client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr.InitContext(ctx)

	// Explicitly ensure voiceConn is nil.
	mgr.connMutex.Lock()
	mgr.voiceConn = nil
	mgr.connMutex.Unlock()

	assert.False(t, mgr.isConnectionHealthy(),
		"Expected false when voiceConn is explicitly nil")

	require.NoError(t, mgr.Stop())
}

// TestDiscord_IsConnectionHealthy_WithNotReady verifies isConnectionHealthy
// returns false when voiceConn exists but IsReady() returns false.
func TestDiscord_IsConnectionHealthy_WithNotReady(t *testing.T) {
	vc := &mockVoiceConn{ready: false}
	client := &mockDiscordClientForConn{
		ready:     true,
		voiceConn: vc,
	}
	mgr, _, _ := newTestDiscordManager(client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr.InitContext(ctx)

	// Inject voice connection that is not ready.
	mgr.connMutex.Lock()
	mgr.voiceConn = vc
	mgr.connMutex.Unlock()

	assert.False(t, mgr.isConnectionHealthy(),
		"Expected false when voiceConn.IsReady() is false")

	require.NoError(t, mgr.Stop())
}

// TestDiscord_IsConnectionHealthy_WithReady verifies isConnectionHealthy
// returns true when voiceConn exists and IsReady() returns true.
func TestDiscord_IsConnectionHealthy_WithReady(t *testing.T) {
	vc := &mockVoiceConn{ready: true, gatewayReady: true}
	client := &mockDiscordClientForConn{
		ready:     true,
		voiceConn: vc,
	}
	mgr, _, _ := newTestDiscordManager(client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr.InitContext(ctx)

	// Inject a ready voice connection.
	mgr.connMutex.Lock()
	mgr.voiceConn = vc
	mgr.connMutex.Unlock()

	assert.True(t, mgr.isConnectionHealthy(),
		"Expected true when voiceConn.IsReady() is true")

	require.NoError(t, mgr.Stop())
}

// TestDiscord_IsConnectionHealthy_StaleGateway verifies that isConnectionHealthy
// returns false when voiceConn.IsReady() is true but the voice gateway is not
// ready (stale session — local state says "ready" but gateway is broken).
func TestDiscord_IsConnectionHealthy_StaleGateway(t *testing.T) {
	vc := &mockVoiceConn{ready: true, gatewayReady: false}
	client := &mockDiscordClientForConn{
		ready:     true,
		voiceConn: vc,
	}
	mgr, _, _ := newTestDiscordManager(client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr.InitContext(ctx)

	// Inject voice connection that thinks it's ready but has a broken gateway.
	mgr.connMutex.Lock()
	mgr.voiceConn = vc
	mgr.connMutex.Unlock()

	assert.False(t, mgr.isConnectionHealthy(),
		"Expected unhealthy when gateway is not ready despite local ready=true")

	// Now mark gateway as ready — health should recover.
	vc.setGatewayReady(true)
	assert.True(t, mgr.isConnectionHealthy(),
		"Expected healthy after gateway becomes ready")

	require.NoError(t, mgr.Stop())
}

// TestDiscord_ConcurrentHealthChecks runs 100 goroutines reading health while
// 10 goroutines toggle the ready flag, checking for data races.
func TestDiscord_ConcurrentHealthChecks(t *testing.T) {
	vc := &mockVoiceConn{ready: false}
	client := &mockDiscordClientForConn{
		ready:     true,
		voiceConn: vc,
	}
	mgr, _, _ := newTestDiscordManager(client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr.InitContext(ctx)

	// Inject voice connection.
	mgr.connMutex.Lock()
	mgr.voiceConn = vc
	mgr.connMutex.Unlock()

	var wg sync.WaitGroup

	// 100 health-check readers.
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = mgr.isConnectionHealthy()
				_ = mgr.GetVoiceConnection()
			}
		}()
	}

	// 10 ready-flag togglers.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				vc.setReady(j%2 == 0)
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success.
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for concurrent health checks to complete")
	}

	require.NoError(t, mgr.Stop())
}

// TestDiscord_DisconnectInternalClearsConnection verifies that calling
// disconnectInternal() sets voiceConn to nil.
func TestDiscord_DisconnectInternalClearsConnection(t *testing.T) {
	vc := &mockVoiceConn{ready: true, gatewayReady: true}
	client := &mockDiscordClientForConn{
		ready:     true,
		voiceConn: vc,
	}
	mgr, _, _ := newTestDiscordManager(client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr.InitContext(ctx)

	// Inject voice connection.
	mgr.connMutex.Lock()
	mgr.voiceConn = vc
	mgr.connMutex.Unlock()

	// Confirm it is set.
	assert.True(t, mgr.isConnectionHealthy())

	// Disconnect.
	mgr.disconnectInternal()

	// voiceConn should now be nil.
	mgr.connMutex.RLock()
	isNil := mgr.voiceConn == nil
	mgr.connMutex.RUnlock()

	assert.True(t, isNil, "Expected voiceConn to be nil after disconnectInternal()")

	// Close should have been called on the mock.
	vc.mu.Lock()
	wasClosed := vc.closed
	vc.mu.Unlock()
	assert.True(t, wasClosed, "Expected Close() to have been called on voiceConn")

	require.NoError(t, mgr.Stop())
}

// TestDiscord_WaitForClientReady_AlreadyReady verifies that waitForClientReady
// returns nil immediately when the client is already ready.
func TestDiscord_WaitForClientReady_AlreadyReady(t *testing.T) {
	client := &mockDiscordClientForConn{
		ready:     true,
		voiceConn: &mockVoiceConn{},
	}
	mgr, _, _ := newTestDiscordManager(client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr.InitContext(ctx)

	start := time.Now()
	err := mgr.waitForClientReady(5 * time.Second)
	elapsed := time.Since(start)

	require.NoError(t, err, "Expected no error when client is already ready")
	assert.Less(t, elapsed, 500*time.Millisecond,
		"Expected waitForClientReady to return quickly when already ready")

	require.NoError(t, mgr.Stop())
}

// TestDiscord_SetStatusAfterStop ensures that calling SetStatus after Stop does
// not panic.
func TestDiscord_SetStatusAfterStop(t *testing.T) {
	client := &mockDiscordClientForConn{
		ready:     false,
		voiceConn: &mockVoiceConn{},
	}
	mgr, _, _ := newTestDiscordManager(client)

	ctx, cancel := context.WithCancel(context.Background())
	mgr.InitContext(ctx)
	cancel()

	err := mgr.Stop()
	require.NoError(t, err)

	// SetStatus after Stop should not panic.
	assert.NotPanics(t, func() {
		mgr.SetStatus(ConnectionConnected, nil)
		mgr.SetStatus(ConnectionFailed, fmt.Errorf("post-stop error"))
	})
}

// TestDiscord_EventEmission verifies that status changes emit events through
// the BridgeEventEmitter.
func TestDiscord_EventEmission(t *testing.T) {
	client := &mockDiscordClientForConn{
		ready:     false,
		voiceConn: &mockVoiceConn{},
	}
	mgr, _, emitter := newTestDiscordManager(client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr.InitContext(ctx)

	// Trigger several status changes.
	mgr.SetStatus(ConnectionConnecting, nil)
	mgr.SetStatus(ConnectionConnected, nil)
	mgr.SetStatus(ConnectionReconnecting, nil)
	mgr.SetStatus(ConnectionFailed, fmt.Errorf("test failure"))

	events := emitter.GetEvents()
	assert.Len(t, events, 4, "Expected 4 events for 4 distinct status changes")

	// All events should be tagged with the "discord" service name.
	for _, ev := range events {
		assert.Equal(t, "discord", ev.Service,
			"Expected service name 'discord' in emitted event")
	}

	require.NoError(t, mgr.Stop())
}

// TestDiscord_MonitorConnection_ContextCancel verifies that monitorConnection
// exits promptly when the context is canceled.
func TestDiscord_MonitorConnection_ContextCancel(t *testing.T) {
	vc := &mockVoiceConn{ready: true, gatewayReady: true}
	client := &mockDiscordClientForConn{
		ready:     true,
		voiceConn: vc,
	}
	mgr, _, _ := newTestDiscordManager(client)

	ctx, cancel := context.WithCancel(context.Background())
	mgr.InitContext(ctx)

	// Inject a ready voice connection so monitor has something to watch.
	mgr.connMutex.Lock()
	mgr.voiceConn = vc
	mgr.connMutex.Unlock()

	done := make(chan struct{})
	go func() {
		mgr.monitorConnection(ctx)
		close(done)
	}()

	// Cancel the context; monitorConnection should exit.
	cancel()

	select {
	case <-done:
		// Success.
	case <-time.After(5 * time.Second):
		t.Fatal("monitorConnection did not exit after context cancellation")
	}

	require.NoError(t, mgr.Stop())
}

// TestDiscord_ConnectOnce_ClientNotReady verifies that connectOnce returns an
// error when the Discord client never becomes ready.
func TestDiscord_ConnectOnce_ClientNotReady(t *testing.T) {
	client := &mockDiscordClientForConn{
		ready:     false,
		voiceConn: &mockVoiceConn{},
	}
	mgr, _, _ := newTestDiscordManager(client)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	mgr.InitContext(ctx)

	// connectOnce waits up to 10s for client ready; override by using a context
	// with a tight deadline so we don't wait the full timeout. We call
	// connectOnce directly and expect an error.
	var connectErr error
	done := make(chan struct{})
	go func() {
		connectErr = mgr.connectOnce()
		close(done)
	}()

	select {
	case <-done:
		require.Error(t, connectErr, "Expected error from connectOnce when client is not ready")
		assert.Contains(t, connectErr.Error(), "not ready",
			"Error should mention client not being ready")
	case <-time.After(30 * time.Second):
		t.Fatal("connectOnce did not return in time")
	}

	status := mgr.GetStatus()
	assert.Equal(t, ConnectionConnecting, status,
		"a single connectOnce failure must not consume the loop's terminal retry decision")

	require.NoError(t, mgr.Stop())
}

// ---------------------------------------------------------------------------
// Ensure the mocks satisfy their respective interfaces at compile time.
// ---------------------------------------------------------------------------

var (
	_ discord.Client          = (*mockDiscordClientForConn)(nil)
	_ discord.VoiceConnection = (*mockVoiceConn)(nil)
)
