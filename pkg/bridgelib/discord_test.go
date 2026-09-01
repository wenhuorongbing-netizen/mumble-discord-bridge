package bridgelib

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	internalbridge "github.com/stieneee/mumble-discord-bridge/internal/bridge"
	"github.com/stieneee/mumble-discord-bridge/internal/discord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockBridgeLibClient implements discord.Client for SharedDiscordClient tests.
type mockBridgeLibClient struct {
	mu    sync.Mutex
	ready bool
}

type stopSignalTestLogger struct {
	*MockTestLogger
	once     sync.Once
	signaled chan struct{}
}

func (l *stopSignalTestLogger) Debug(component, message string) {
	l.MockTestLogger.Debug(component, message)
	if message == "Auto/mumble bridge stop signal sent" {
		l.once.Do(func() { close(l.signaled) })
	}
}

func (m *mockBridgeLibClient) Connect(_ context.Context) error { return nil }

func (m *mockBridgeLibClient) Disconnect(_ context.Context) error { return nil }

func (m *mockBridgeLibClient) SendMessage(_, _ string) error { return nil }

func (m *mockBridgeLibClient) GetUser(_ string) (*discord.User, error) {
	return &discord.User{}, nil
}

func (m *mockBridgeLibClient) CreateDM(_ string) (string, error) { return "", nil }

func (m *mockBridgeLibClient) GetGuild(_ string) (*discord.Guild, error) {
	return &discord.Guild{}, nil
}

func (m *mockBridgeLibClient) GetBotUserID() string { return "bot-user-id" }

func (m *mockBridgeLibClient) IsReady() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ready
}

func (m *mockBridgeLibClient) CreateVoiceConnection(_ string) (discord.VoiceConnection, error) {
	return nil, nil
}

func (m *mockBridgeLibClient) AddEventHandler(_ discord.EventHandler) func() { return func() {} }

// TestSharedDiscordClient_IsSessionHealthy verifies that IsSessionHealthy
// correctly reflects the readiness state of the underlying discord.Client.
func TestSharedDiscordClient_IsSessionHealthy(t *testing.T) {
	t.Run("returns false when client reports not ready", func(t *testing.T) {
		mock := &mockBridgeLibClient{ready: false}
		lgr := &MockTestLogger{}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sdc := &SharedDiscordClient{
			client: mock,
			logger: lgr,
			ctx:    ctx,
			cancel: cancel,
		}

		assert.False(t, sdc.IsSessionHealthy(), "expected unhealthy when client is not ready")
	})

	t.Run("returns true when client reports ready", func(t *testing.T) {
		mock := &mockBridgeLibClient{ready: true}
		lgr := &MockTestLogger{}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sdc := &SharedDiscordClient{
			client: mock,
			logger: lgr,
			ctx:    ctx,
			cancel: cancel,
		}

		assert.True(t, sdc.IsSessionHealthy(), "expected healthy when client is ready")
	})
}

// TestSharedDiscordClient_SessionMonitorLoop_ContextCancellation verifies that
// the session monitor loop exits cleanly when its context is canceled.
func TestSharedDiscordClient_SessionMonitorLoop_ContextCancellation(t *testing.T) {
	mock := &mockBridgeLibClient{ready: true}
	lgr := &MockTestLogger{}
	ctx, cancel := context.WithCancel(context.Background())

	sdc := &SharedDiscordClient{
		client: mock,
		logger: lgr,
		ctx:    ctx,
		cancel: cancel,
	}

	// Run the monitor loop in a goroutine and signal when it returns.
	done := make(chan struct{})
	go func() {
		sdc.sessionMonitorLoop()
		close(done)
	}()

	// Cancel the context to trigger loop exit.
	cancel()

	select {
	case <-done:
		// Loop exited cleanly -- success.
	case <-time.After(5 * time.Second):
		t.Fatal("sessionMonitorLoop did not exit within 5 seconds after context cancellation")
	}

	// Verify that the loop logged its exit message.
	entries := lgr.getEntries()
	found := false
	for _, e := range entries {
		if containsSubstring(e, "Session monitoring loop exiting") {
			found = true
			break
		}
	}
	require.True(t, found, "expected 'Session monitoring loop exiting' in log entries, got: %v", entries)
}

// TestSharedDiscordClient_SessionMonitorLoop_LogsUnhealthy verifies that the
// monitor loop runs without panicking when the client reports unhealthy, and
// exits cleanly on context cancellation.
func TestSharedDiscordClient_SessionMonitorLoop_LogsUnhealthy(t *testing.T) {
	mock := &mockBridgeLibClient{ready: false}
	lgr := &MockTestLogger{}
	ctx, cancel := context.WithCancel(context.Background())

	sdc := &SharedDiscordClient{
		client: mock,
		logger: lgr,
		ctx:    ctx,
		cancel: cancel,
	}

	done := make(chan struct{})
	go func() {
		sdc.sessionMonitorLoop()
		close(done)
	}()

	// Let the monitor tick at least once (ticker is 15s, so we can't wait
	// that long in a unit test). Instead, cancel promptly and just verify
	// the loop starts and exits without panicking.
	// We give it a short window so the goroutine has time to enter the select.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Loop exited -- no panic.
	case <-time.After(5 * time.Second):
		t.Fatal("sessionMonitorLoop did not exit within 5 seconds after context cancellation")
	}

	// The loop should have logged that it started and that it is exiting.
	entries := lgr.getEntries()
	foundStarted := false
	foundExiting := false
	for _, e := range entries {
		if containsSubstring(e, "Session monitoring loop started") {
			foundStarted = true
		}
		if containsSubstring(e, "Session monitoring loop exiting") {
			foundExiting = true
		}
	}
	assert.True(t, foundStarted, "expected 'Session monitoring loop started' log entry, got: %v", entries)
	assert.True(t, foundExiting, "expected 'Session monitoring loop exiting' log entry, got: %v", entries)
}

func TestBridgeInstance_StopJoinsModeBeforeStoppedPublicationAndHandlerRemoval(t *testing.T) {
	lgr := &MockTestLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	dispatcher := NewEventDispatcher("offline", 4, lgr)
	dispatcher.Start()
	var modeDone atomic.Bool
	var handlerRemoved atomic.Bool
	stoppedObserved := make(chan bool, 1)
	dispatcher.RegisterHandler(EventBridgeStopped, func(BridgeEvent) {
		stoppedObserved <- modeDone.Load() && handlerRemoved.Load()
	})
	instance := &BridgeInstance{
		State: &internalbridge.BridgeState{
			Mode:   internalbridge.BridgeModeConstant,
			Logger: lgr,
		},
		config:          &BridgeConfig{},
		logger:          lgr,
		eventDispatcher: dispatcher,
		ctx:             ctx,
		cancel:          cancel,
		removeEventHandler: func() {
			require.True(t, modeDone.Load(), "handler removal must follow mode cleanup")
			handlerRemoved.Store(true)
		},
	}
	instance.modeWg.Add(1)
	go func() {
		defer instance.modeWg.Done()
		<-ctx.Done()
		modeDone.Store(true)
	}()

	require.NoError(t, instance.Stop())
	require.True(t, instance.stopped)
	require.True(t, modeDone.Load())
	require.True(t, handlerRemoved.Load())
	select {
	case ordered := <-stoppedObserved:
		require.True(t, ordered, "stopped publication must follow cleanup and handler removal")
	case <-time.After(time.Second):
		t.Fatal("stopped event was not dispatched")
	}
}

func TestBridgeInstance_StopModeSignalIsRetainedAndIdempotent(t *testing.T) {
	for _, mode := range []internalbridge.BridgeMode{internalbridge.BridgeModeAuto, internalbridge.BridgeModeMumble} {
		t.Run(mode.String(), func(t *testing.T) {
			lgr := &stopSignalTestLogger{MockTestLogger: &MockTestLogger{}, signaled: make(chan struct{})}
			ctx, cancel := context.WithCancel(context.Background())
			modeStop := make(chan bool)
			instance := &BridgeInstance{
				State: &internalbridge.BridgeState{
					Mode:        mode,
					AutoChanDie: modeStop,
					Logger:      lgr,
				},
				config: &BridgeConfig{},
				logger: lgr,
				ctx:    ctx,
				cancel: cancel,
			}

			proceed := make(chan struct{})
			cleanup := make(chan struct{})
			observed := make(chan bool, 1)
			instance.modeWg.Add(1)
			go func() {
				defer instance.modeWg.Done()
				<-proceed // Deliberately busy while Stop signals.
				select {
				case _, ok := <-modeStop:
					observed <- !ok
				case <-cleanup:
				}
			}()

			firstDone := make(chan error, 1)
			go func() { firstDone <- instance.Stop() }()
			<-lgr.signaled // Stop has executed the production signal branch.

			retained := false
			select {
			case _, ok := <-modeStop:
				retained = !ok
			default:
			}
			if !retained {
				close(cleanup)
				close(proceed)
				<-firstDone
				t.Fatal("mode stop signal was lost while worker was busy")
			}

			const concurrentStops = 10
			concurrentDone := make(chan error, concurrentStops)
			for range concurrentStops {
				go func() { concurrentDone <- instance.Stop() }()
			}
			close(proceed)
			require.True(t, <-observed, "worker must observe close after becoming receptive")
			require.NoError(t, <-firstDone)
			for range concurrentStops {
				require.NoError(t, <-concurrentDone)
			}
			require.NoError(t, instance.Stop(), "repeated Stop must not close the channel twice")
		})
	}
}
