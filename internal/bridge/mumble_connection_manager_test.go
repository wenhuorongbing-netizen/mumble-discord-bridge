package bridge

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stieneee/gumble/gumble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMumble_RetryBudgetEndsInFailed(t *testing.T) {
	manager := NewMumbleConnectionManager("unused", &gumble.Config{}, nil, NewMockLogger(), NewMockBridgeEventEmitter())
	manager.managerConfig = &ConnectionManagerConfig{MaxRetries: 2, BaseRetryDelay: time.Millisecond, MaxRetryDelay: 2 * time.Millisecond, RetryMultiplier: 2}
	attempts := 0
	manager.connectFunc = func(context.Context, uint64) error {
		attempts++
		return errors.New("connect failed")
	}
	done := make(chan struct{})
	go func() {
		manager.connectionLoop(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Mumble retry budget did not terminate")
	}
	assert.Equal(t, 3, attempts)
	assert.Equal(t, ConnectionFailed, manager.GetStatus())
}

func TestMumble_RetryBudgetResetsAfterSuccess(t *testing.T) {
	manager := NewMumbleConnectionManager("unused", &gumble.Config{}, nil, NewMockLogger(), NewMockBridgeEventEmitter())
	manager.managerConfig = &ConnectionManagerConfig{MaxRetries: 1, BaseRetryDelay: time.Millisecond, MaxRetryDelay: time.Millisecond, RetryMultiplier: 2}
	attempts := 0
	connected := make(chan struct{}, 2)
	manager.connectFunc = func(context.Context, uint64) error {
		attempts++
		if attempts == 1 || attempts == 3 {
			return errors.New("transient")
		}
		connected <- struct{}{}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		manager.connectionLoop(ctx)
		close(done)
	}()

	select {
	case <-connected:
	case <-time.After(time.Second):
		t.Fatal("first Mumble success not reached")
	}
	manager.OnDisconnect(&gumble.DisconnectEvent{Type: gumble.DisconnectError, String: "test outage"})
	select {
	case <-connected:
	case <-time.After(time.Second):
		t.Fatal("second Mumble success not reached")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Mumble reset test did not terminate")
	}
	assert.Equal(t, 4, attempts, "each post-success episode must receive a fresh retry budget")
}

func TestMumble_ConfiguredChannelFailureNeverConnects(t *testing.T) {
	emitter := NewMockBridgeEventEmitter()
	manager := NewMumbleConnectionManager("unused", &gumble.Config{}, nil, NewMockLogger(), emitter)
	manager.SetTargetChannel([]string{"typo-channel"})
	manager.managerConfig = &ConnectionManagerConfig{MaxRetries: 0, BaseRetryDelay: time.Millisecond, MaxRetryDelay: time.Millisecond, RetryMultiplier: 2}
	manager.connectFunc = func(context.Context, uint64) error { return errors.New("configured Mumble channel not found") }
	manager.connectionLoop(context.Background())

	assert.Equal(t, ConnectionFailed, manager.GetStatus())
	for _, event := range emitter.GetEvents() {
		assert.False(t, event.Connected, "missing configured channel must never emit Connected")
	}
}

func TestMumble_EmptyChannelExplicitlyTargetsRoot(t *testing.T) {
	manager := NewMumbleConnectionManager("unused", &gumble.Config{}, nil, NewMockLogger(), nil)
	manager.SetTargetChannel(nil)
	manager.configMutex.RLock()
	defer manager.configMutex.RUnlock()
	assert.Empty(t, manager.targetChannel)
}

func TestMumble_TargetChannelConfigurationIsCopied(t *testing.T) {
	manager := NewMumbleConnectionManager("unused", &gumble.Config{}, nil, NewMockLogger(), nil)
	path := []string{"parent", "voice"}
	manager.SetTargetChannel(path)
	path[1] = "mutated"
	manager.configMutex.RLock()
	defer manager.configMutex.RUnlock()
	assert.Equal(t, []string{"parent", "voice"}, manager.targetChannel)
}

func TestMumble_StopClosedDisconnectChannelBoundary(t *testing.T) {
	for iteration := 0; iteration < 100; iteration++ {
		manager := NewMumbleConnectionManager("unused", &gumble.Config{}, nil, NewMockLogger(), nil)
		manager.connectFunc = func(context.Context, uint64) error { return nil }
		loopCtx, cancelLoop := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			manager.connectionLoop(loopCtx)
			close(done)
		}()

		select {
		case event := <-manager.GetEventChannel():
			require.Equal(t, ConnectionConnected, event.Status)
		case <-time.After(time.Second):
			cancelLoop()
			t.Fatalf("iteration %d did not reach disconnect wait", iteration)
		}

		require.NoError(t, manager.Stop())
		select {
		case <-done:
		case <-time.After(time.Second):
			cancelLoop()
			t.Fatalf("iteration %d did not exit after disconnect channel close", iteration)
		}
		cancelLoop()
	}
}

// TestMumble_DisconnectKickedStopsReconnection verifies kicked status stops reconnection
func TestMumble_DisconnectKickedStopsReconnection(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	config := &gumble.Config{
		Username: "test-bot",
	}

	manager := NewMumbleConnectionManager("localhost:64738", config, nil, logger, emitter)

	// Use InitContext to test handleDisconnectEvent without racing with connection loop
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager.InitContext(ctx)

	// Simulate kicked event
	kickedEvent := &gumble.DisconnectEvent{
		Type:   gumble.DisconnectKicked,
		String: "You were kicked",
	}
	manager.handleDisconnectEvent(kickedEvent)

	// Status should be ConnectionFailed
	assert.Equal(t, ConnectionFailed, manager.GetStatus())

	// Cleanup
	err := manager.Stop()
	require.NoError(t, err)
}

// TestMumble_DisconnectBannedStopsReconnection verifies banned status stops reconnection
func TestMumble_DisconnectBannedStopsReconnection(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	config := &gumble.Config{
		Username: "test-bot",
	}

	manager := NewMumbleConnectionManager("localhost:64738", config, nil, logger, emitter)

	// Use InitContext to test handleDisconnectEvent without racing with connection loop
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager.InitContext(ctx)

	// Simulate banned event
	bannedEvent := &gumble.DisconnectEvent{
		Type:   gumble.DisconnectBanned,
		String: "You are banned",
	}
	manager.handleDisconnectEvent(bannedEvent)

	// Status should be ConnectionFailed
	assert.Equal(t, ConnectionFailed, manager.GetStatus())

	err := manager.Stop()
	require.NoError(t, err)
}

// TestMumble_DisconnectErrorReconnects verifies error triggers reconnection
func TestMumble_DisconnectErrorReconnects(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	config := &gumble.Config{
		Username: "test-bot",
	}

	manager := NewMumbleConnectionManager("localhost:64738", config, nil, logger, emitter)

	// Use InitContext to test handleDisconnectEvent without racing with connection loop
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager.InitContext(ctx)

	// Simulate error disconnect event
	errorEvent := &gumble.DisconnectEvent{
		Type:   gumble.DisconnectError,
		String: "Connection error",
	}
	manager.handleDisconnectEvent(errorEvent)

	// Status should be ConnectionReconnecting (not Failed)
	assert.Equal(t, ConnectionReconnecting, manager.GetStatus())

	err := manager.Stop()
	require.NoError(t, err)
}

// TestMumble_DisconnectEventAfterStop ensures no panic sending to closed channel
func TestMumble_DisconnectEventAfterStop(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	config := &gumble.Config{
		Username: "test-bot",
	}

	manager := NewMumbleConnectionManager("localhost:64738", config, nil, logger, emitter)

	// Use InitContext to avoid connection loop racing with Stop
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager.InitContext(ctx)

	// Stop the manager
	err := manager.Stop()
	require.NoError(t, err)

	// Now try to send disconnect event - should not panic
	assert.NotPanics(t, func() {
		manager.OnDisconnect(&gumble.DisconnectEvent{
			Type:   gumble.DisconnectError,
			String: "Post-stop event",
		})
	})
}

// TestMumble_ConcurrentDisconnectEvents tests multiple disconnect events while shutting down
func TestMumble_ConcurrentDisconnectEvents(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	config := &gumble.Config{
		Username: "test-bot",
	}

	manager := NewMumbleConnectionManager("localhost:64738", config, nil, logger, emitter)

	// Use InitContext to avoid connection loop racing with Stop
	ctx, cancel := context.WithCancel(context.Background())

	manager.InitContext(ctx)

	// Start concurrent disconnect events
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			manager.OnDisconnect(&gumble.DisconnectEvent{
				Type:   gumble.DisconnectError,
				String: "Concurrent disconnect",
			})
		}()
	}

	// Stop while events are being sent
	cancel()
	err := manager.Stop()
	require.NoError(t, err)

	// Wait for all goroutines to finish
	wg.Wait()
}

// TestMumble_RapidReconnection tests multiple connect/disconnect cycles
func TestMumble_RapidReconnection(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	config := &gumble.Config{
		Username: "test-bot",
	}

	// Run 10 rapid start/stop cycles
	for i := 0; i < 10; i++ {
		manager := NewMumbleConnectionManager("localhost:64738", config, nil, logger, emitter)

		// Use InitContext to avoid connection loop racing with Stop
		ctx, cancel := context.WithCancel(context.Background())

		manager.InitContext(ctx)

		// Brief operation
		time.Sleep(10 * time.Millisecond)

		cancel()
		err := manager.Stop()
		require.NoError(t, err, "Failed to stop on cycle %d", i)
	}
}

// TestMumble_StopIdempotent ensures multiple Stop calls are safe
func TestMumble_StopIdempotent(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	config := &gumble.Config{
		Username: "test-bot",
	}

	manager := NewMumbleConnectionManager("localhost:64738", config, nil, logger, emitter)

	// Use InitContext to avoid connection loop racing with Stop
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager.InitContext(ctx)

	// Multiple stops should be safe
	for i := 0; i < 5; i++ {
		err := manager.Stop()
		require.NoError(t, err, "Stop call %d failed", i)
	}
}

// TestMumble_GetClientThreadSafe tests concurrent GetClient calls
func TestMumble_GetClientThreadSafe(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	config := &gumble.Config{
		Username: "test-bot",
	}

	manager := NewMumbleConnectionManager("localhost:64738", config, nil, logger, emitter)

	// Use InitContext to avoid connection loop racing with Stop
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager.InitContext(ctx)

	// Concurrent GetClient calls
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = manager.GetClient()
		}()
	}

	wg.Wait()

	err := manager.Stop()
	require.NoError(t, err)
}

// TestMumble_UpdateAddressThreadSafe tests concurrent address updates
func TestMumble_UpdateAddressThreadSafe(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	config := &gumble.Config{
		Username: "test-bot",
	}

	manager := NewMumbleConnectionManager("localhost:64738", config, nil, logger, emitter)

	// Use InitContext to avoid connection loop racing with Stop
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager.InitContext(ctx)

	// Concurrent address updates
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			err := manager.UpdateAddress("localhost:" + string(rune(64738+idx)))
			_ = err
			_ = manager.GetAddress()
		}(i)
	}

	wg.Wait()

	err := manager.Stop()
	require.NoError(t, err)
}

// TestMumble_HandleDisconnectEventTypes tests all disconnect event types
func TestMumble_HandleDisconnectEventTypes(t *testing.T) {
	testCases := []struct {
		name           string
		disconnectType gumble.DisconnectType
		expectedStatus ConnectionStatus
	}{
		{"Error", gumble.DisconnectError, ConnectionReconnecting},
		{"Kicked", gumble.DisconnectKicked, ConnectionFailed},
		{"Banned", gumble.DisconnectBanned, ConnectionFailed},
		{"User", gumble.DisconnectUser, ConnectionReconnecting},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			logger := NewMockLogger()
			emitter := NewMockBridgeEventEmitter()

			config := &gumble.Config{
				Username: "test-bot",
			}

			manager := NewMumbleConnectionManager("localhost:64738", config, nil, logger, emitter)

			// Use InitContext to test handleDisconnectEvent without racing with connection loop
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			manager.InitContext(ctx)

			// Handle disconnect event
			event := &gumble.DisconnectEvent{
				Type:   tc.disconnectType,
				String: "Test disconnect",
			}
			manager.handleDisconnectEvent(event)

			assert.Equal(t, tc.expectedStatus, manager.GetStatus())

			err := manager.Stop()
			require.NoError(t, err)
		})
	}
}

// TestMumble_ConfigUpdateThreadSafe tests concurrent config updates
func TestMumble_ConfigUpdateThreadSafe(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	config := &gumble.Config{
		Username: "test-bot",
	}

	manager := NewMumbleConnectionManager("localhost:64738", config, nil, logger, emitter)

	// Use InitContext to avoid connection loop racing with Stop
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager.InitContext(ctx)

	// Concurrent config updates and reads
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func(idx int) {
			defer wg.Done()
			newConfig := &gumble.Config{
				Username: "test-bot-" + string(rune('0'+idx)),
			}
			_ = manager.UpdateConfig(newConfig) //nolint:errcheck // test setup
		}(i)
		go func() {
			defer wg.Done()
			_ = manager.GetConfig()
		}()
	}

	wg.Wait()

	err := manager.Stop()
	require.NoError(t, err)
}

// TestMumble_EventListenerMethods tests the gumble.EventListener implementation
func TestMumble_EventListenerMethods(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	config := &gumble.Config{
		Username: "test-bot",
	}

	manager := NewMumbleConnectionManager("localhost:64738", config, nil, logger, emitter)

	// All these methods should not panic when called
	assert.NotPanics(t, func() {
		manager.OnConnect(&gumble.ConnectEvent{})
		manager.OnTextMessage(&gumble.TextMessageEvent{})
		manager.OnUserChange(&gumble.UserChangeEvent{})
		manager.OnChannelChange(&gumble.ChannelChangeEvent{})
		manager.OnPermissionDenied(&gumble.PermissionDeniedEvent{})
		manager.OnUserList(&gumble.UserListEvent{})
		manager.OnACL(&gumble.ACLEvent{})
		manager.OnBanList(&gumble.BanListEvent{})
		manager.OnContextActionChange(&gumble.ContextActionChangeEvent{})
		manager.OnServerConfig(&gumble.ServerConfigEvent{})
	})
}

// TestMumble_DisconnectChannelBuffer tests the buffered disconnect channel
func TestMumble_DisconnectChannelBuffer(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	config := &gumble.Config{
		Username: "test-bot",
	}

	manager := NewMumbleConnectionManager("localhost:64738", config, nil, logger, emitter)

	// Use InitContext to avoid connection loop racing with Stop
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager.InitContext(ctx)

	// Send multiple disconnect events rapidly - should not block
	for i := 0; i < 10; i++ {
		manager.OnDisconnect(&gumble.DisconnectEvent{
			Type:   gumble.DisconnectError,
			String: "Rapid event",
		})
	}

	// Should complete without blocking
	err := manager.Stop()
	require.NoError(t, err)
}

// TestMumble_SendAudioNilClient tests manager-owned handoff with no client.
func TestMumble_SendAudioNilClient(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	config := &gumble.Config{
		Username: "test-bot",
	}

	manager := NewMumbleConnectionManager("localhost:64738", config, nil, logger, emitter)

	assert.False(t, manager.SendAudio(context.Background(), gumble.AudioBuffer{1}, time.Millisecond))
}

// TestMumble_StatusTransitionsUnderLoad tests status changes under concurrent load
func TestMumble_StatusTransitionsUnderLoad(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	config := &gumble.Config{
		Username: "test-bot",
	}

	manager := NewMumbleConnectionManager("localhost:64738", config, nil, logger, emitter)

	// Use InitContext to avoid connection loop racing with Stop
	ctx, cancel := context.WithCancel(context.Background())

	manager.InitContext(ctx)

	var reads, writes int32
	var wg sync.WaitGroup

	// Concurrent status reads
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = manager.GetStatus()
				_ = manager.IsConnected()
				atomic.AddInt32(&reads, 1)
			}
		}()
	}

	// Concurrent status changes via disconnect events
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				manager.OnDisconnect(&gumble.DisconnectEvent{
					Type:   gumble.DisconnectError,
					String: "Load test",
				})
				atomic.AddInt32(&writes, 1)
			}
		}()
	}

	wg.Wait()
	cancel()
	err := manager.Stop()
	require.NoError(t, err)

	t.Logf("Completed %d reads and %d writes", atomic.LoadInt32(&reads), atomic.LoadInt32(&writes))
}

func TestMumble_ProductionDialAttemptHasFiniteTimeout(t *testing.T) {
	manager := NewMumbleConnectionManager("unused", &gumble.Config{}, nil, NewMockLogger(), nil)
	manager.InitContext(context.Background())
	manager.runMutex.Lock()
	manager.running = true
	manager.runMutex.Unlock()
	manager.connectTimeout = 37 * time.Millisecond
	manager.dialFunc = func(dialer *net.Dialer, _ string, _ *gumble.Config, _ *tls.Config) (*gumble.Client, error) {
		assert.Equal(t, 37*time.Millisecond, dialer.Timeout)
		return nil, errors.New("offline dial failure")
	}

	require.ErrorContains(t, manager.connect(manager.ctx, manager.generation), "failed to connect")
	require.NoError(t, manager.Stop())
}

func TestMumble_ChannelSyncAttemptHonorsCanceledContext(t *testing.T) {
	manager := NewMumbleConnectionManager("unused", &gumble.Config{}, nil, NewMockLogger(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	manager.InitContext(ctx)
	cancel()
	client := &gumble.Client{Channels: gumble.Channels{0: &gumble.Channel{ID: 0}}}

	require.ErrorIs(t, manager.moveToTargetChannel(client), context.Canceled)
	require.NoError(t, manager.Stop())
}

func TestMumble_StopJoinsConnectionLoop(t *testing.T) {
	manager := NewMumbleConnectionManager("unused", &gumble.Config{}, nil, NewMockLogger(), nil)
	entered := make(chan struct{})
	exited := make(chan struct{})
	manager.connectFunc = func(context.Context, uint64) error {
		close(entered)
		<-manager.ctx.Done()
		close(exited)
		return context.Canceled
	}
	require.NoError(t, manager.Start(context.Background()))
	<-entered

	require.NoError(t, manager.Stop())
	select {
	case <-exited:
	default:
		t.Fatal("Stop returned before the connection loop's active attempt exited")
	}
}

func TestMumble_CanceledGenerationCannotPublishSuccess(t *testing.T) {
	manager := NewMumbleConnectionManager("unused", &gumble.Config{}, nil, NewMockLogger(), nil)
	entered := make(chan struct{})
	release := make(chan struct{})
	channel := &gumble.Channel{ID: 0}
	client := &gumble.Client{
		Channels: gumble.Channels{0: channel},
		Self:     &gumble.User{Channel: channel},
	}
	manager.dialFunc = func(*net.Dialer, string, *gumble.Config, *tls.Config) (*gumble.Client, error) {
		close(entered)
		<-release
		return client, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, manager.Start(ctx))
	<-entered
	cancel()
	close(release)
	require.NoError(t, manager.Stop())
	require.NotEqual(t, ConnectionConnected, manager.GetStatus())
	require.Nil(t, manager.GetClient())
	manager.audioMutex.Lock()
	require.Nil(t, manager.audioOutgoing)
	manager.audioMutex.Unlock()
}

func TestMumble_DisconnectEventsRequireCurrentActiveClient(t *testing.T) {
	manager := NewMumbleConnectionManager("unused", &gumble.Config{}, nil, NewMockLogger(), nil)
	current := &gumble.Client{}
	stale := &gumble.Client{}
	manager.runMutex.Lock()
	manager.running = true
	manager.runMutex.Unlock()
	manager.clientMutex.Lock()
	manager.client = current
	manager.clientMutex.Unlock()

	manager.OnDisconnect(&gumble.DisconnectEvent{Client: stale, Type: gumble.DisconnectError, String: "stale"})
	require.Empty(t, manager.disconnectCh)
	manager.OnDisconnect(&gumble.DisconnectEvent{Client: current, Type: gumble.DisconnectError, String: "current"})
	require.Len(t, manager.disconnectCh, 1)
	<-manager.disconnectCh
	manager.runMutex.Lock()
	manager.running = false
	manager.runMutex.Unlock()
	manager.OnDisconnect(&gumble.DisconnectEvent{Client: current, Type: gumble.DisconnectError, String: "stopped"})
	require.Empty(t, manager.disconnectCh)
	manager.clientMutex.Lock()
	manager.client = nil
	manager.clientMutex.Unlock()
	require.NoError(t, manager.Stop())
}

func TestMumble_ManagerOwnsOutgoingAcrossPipelinePauseAndRetirement(t *testing.T) {
	manager := NewMumbleConnectionManager("unused", &gumble.Config{}, nil, NewMockLogger(), nil)
	outgoing := make(chan gumble.AudioBuffer, 2)
	manager.audioMutex.Lock()
	manager.audioOutgoing = outgoing
	manager.audioMutex.Unlock()
	bridge := createTestBridgeState(nil)
	bridge.MumbleConnectionManager = manager
	duplex := NewMumbleDuplex(bridge.Logger, bridge)
	internal := make(chan gumble.AudioBuffer, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	before := testutil.ToFloat64(promSentMumblePackets)
	go func() {
		duplex.toMumbleSender(ctx, internal)
		close(done)
	}()
	internal <- gumble.AudioBuffer{1, 2, 3}
	require.Equal(t, gumble.AudioBuffer{1, 2, 3}, <-outgoing)
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(promSentMumblePackets) == before+1
	}, time.Second, time.Millisecond)
	cancel()
	<-done

	require.NotPanics(t, func() { outgoing <- gumble.AudioBuffer{4} }, "pipeline pause must not close persistent-client audio")
	<-outgoing
	manager.disconnectInternal()
	_, open := <-outgoing
	require.False(t, open, "client retirement must close AudioOutgoing")
	require.NotPanics(t, manager.disconnectInternal, "retirement must close AudioOutgoing exactly once")
}
