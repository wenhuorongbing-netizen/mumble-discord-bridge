package bridge

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBridge_DiscordUsersMapRace tests concurrent map access under mutex
func TestBridge_DiscordUsersMapRace(_ *testing.T) {
	bridge := createTestBridgeState(nil)

	var wg sync.WaitGroup

	// Concurrent writes
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			bridge.DiscordUsersMutex.Lock()
			bridge.DiscordUsers["user-"+string(rune('0'+idx%10))] = DiscordUser{
				username: "TestUser",
				seen:     true,
			}
			bridge.DiscordUsersMutex.Unlock()
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bridge.DiscordUsersMutex.Lock()
			for k := range bridge.DiscordUsers {
				_ = bridge.DiscordUsers[k].username
			}
			bridge.DiscordUsersMutex.Unlock()
		}()
	}

	// Concurrent deletes
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			bridge.DiscordUsersMutex.Lock()
			delete(bridge.DiscordUsers, "user-"+string(rune('0'+idx%10)))
			bridge.DiscordUsersMutex.Unlock()
		}(i)
	}

	wg.Wait()
}

// TestBridge_MumbleUsersMapRace tests concurrent Mumble user map access
func TestBridge_MumbleUsersMapRace(_ *testing.T) {
	bridge := createTestBridgeState(nil)

	var wg sync.WaitGroup

	// Concurrent writes
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			bridge.MumbleUsersMutex.Lock()
			bridge.MumbleUsers["user-"+string(rune('0'+idx%10))] = true
			bridge.MumbleUsersMutex.Unlock()
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bridge.MumbleUsersMutex.Lock()
			for k := range bridge.MumbleUsers {
				_ = bridge.MumbleUsers[k]
			}
			bridge.MumbleUsersMutex.Unlock()
		}()
	}

	wg.Wait()
}

// TestBridge_BridgeMutexLockOrder tests proper lock ordering
func TestBridge_BridgeMutexLockOrder(t *testing.T) {
	bridge := createTestBridgeState(nil)

	// Test proper lock order: BridgeMutex -> MumbleUsersMutex -> DiscordUsersMutex
	assertNoDeadlock(t, 5*time.Second, func() {
		var wg sync.WaitGroup

		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// Correct lock order
				bridge.BridgeMutex.Lock()
				bridge.MumbleUsersMutex.Lock()
				bridge.DiscordUsersMutex.Lock()

				// Do some work
				bridge.MumbleUsers["test"] = true
				bridge.DiscordUsers["test"] = DiscordUser{username: "test"}

				bridge.DiscordUsersMutex.Unlock()
				bridge.MumbleUsersMutex.Unlock()
				bridge.BridgeMutex.Unlock()
			}()
		}

		wg.Wait()
	})
}

// TestBridge_IsConnected tests thread-safe IsConnected
func TestBridge_IsConnected(t *testing.T) {
	bridge := createTestBridgeState(nil)

	// Initial state
	assert.False(t, bridge.IsConnected())

	// Set connected
	bridge.BridgeMutex.Lock()
	bridge.Connected = true
	bridge.BridgeMutex.Unlock()

	assert.True(t, bridge.IsConnected())

	// Concurrent access
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = bridge.IsConnected()
		}()
	}
	wg.Wait()
}

// TestBridge_GetConnectionStates tests thread-safe state retrieval
func TestBridge_GetConnectionStates(t *testing.T) {
	bridge := createTestBridgeState(nil)

	// Set some states
	bridge.BridgeMutex.Lock()
	bridge.DiscordConnected = true
	bridge.MumbleConnected = false
	bridge.Connected = true
	bridge.BridgeMutex.Unlock()

	discord, mumble, overall := bridge.GetConnectionStates()
	assert.True(t, discord)
	assert.False(t, mumble)
	assert.True(t, overall)
}

// TestBridge_UpdateOverallConnectionState tests state update logic
func TestBridge_UpdateOverallConnectionState(t *testing.T) {
	testCases := []struct {
		name         string
		mode         BridgeMode
		discord      bool
		mumble       bool
		expectedConn bool
	}{
		{"Constant - both connected", BridgeModeConstant, true, true, true},
		{"Constant - none connected", BridgeModeConstant, false, false, false},
		{"Auto - both connected", BridgeModeAuto, true, true, true},
		{"Auto - one connected", BridgeModeAuto, true, false, false},
		{"Auto - none connected", BridgeModeAuto, false, false, false},
		{"Manual - both connected", BridgeModeManual, true, true, true},
		{"Manual - one connected", BridgeModeManual, true, false, false},
		{"Manual - none connected", BridgeModeManual, false, false, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bridge := createTestBridgeState(nil)
			bridge.BridgeMutex.Lock()
			bridge.Mode = tc.mode
			bridge.DiscordConnected = tc.discord
			bridge.MumbleConnected = tc.mumble
			bridge.DiscordOutboundHealth = discordOutboundHealthy
			bridge.updateOverallConnectionState()
			result := bridge.Connected
			bridge.BridgeMutex.Unlock()

			assert.Equal(t, tc.expectedConn, result)
		})
	}
}

func TestBridge_OutboundHealthReadinessModel(t *testing.T) {
	bridge := createTestBridgeState(nil)
	bridge.BridgeMutex.Lock()
	bridge.DiscordConnected = true
	bridge.MumbleConnected = true
	bridge.updateOverallConnectionState()
	assert.False(t, bridge.Connected, "active startup is fail-closed until the first successful send")
	bridge.BridgeMutex.Unlock()

	bridge.setDiscordOutboundHealth(discordOutboundHealthy)
	assert.True(t, bridge.IsConnected())
	bridge.setDiscordOutboundHealth(discordOutboundUnhealthy)
	assert.False(t, bridge.IsConnected())
	bridge.setDiscordOutboundHealth(discordOutboundHealthy)
	assert.True(t, bridge.IsConnected())

	bridge.setDiscordOutboundHealth(discordOutboundInactive)
	assert.True(t, bridge.IsConnected(), "intentional no-audio presence must not be permanently unready")
}

func TestBridge_VoiceOnlyPrivacyPredicateIsNarrow(t *testing.T) {
	bridge := createTestBridgeState(nil)
	bridge.BridgeConfig.DiscordTextMode = "disabled"
	bridge.BridgeConfig.MumbleDisableText = true
	assert.True(t, bridge.voiceOnlyPrivacy())

	bridge.BridgeConfig.DiscordTextMode = "channel"
	assert.False(t, bridge.voiceOnlyPrivacy())
	bridge.BridgeConfig.DiscordTextMode = "disabled"
	bridge.BridgeConfig.DiscordCommand = true
	assert.False(t, bridge.voiceOnlyPrivacy())
	bridge.BridgeConfig.DiscordCommand = false
	bridge.BridgeConfig.ChatBridge = true
	assert.False(t, bridge.voiceOnlyPrivacy())
	bridge.BridgeConfig.ChatBridge = false
	bridge.BridgeConfig.MumbleDisableText = false
	assert.False(t, bridge.voiceOnlyPrivacy())
}

// TestBridge_EmitConnectionEvent tests event emission
func TestBridge_EmitConnectionEvent(t *testing.T) {
	logger := NewMockLogger()
	bridge := createTestBridgeState(logger)

	// Test Discord connection event
	bridge.EmitConnectionEvent("discord", 1, true, nil)

	bridge.BridgeMutex.Lock()
	assert.True(t, bridge.DiscordConnected)
	bridge.BridgeMutex.Unlock()

	// Test Mumble connection event
	bridge.EmitConnectionEvent("mumble", 1, true, nil)

	bridge.BridgeMutex.Lock()
	assert.True(t, bridge.MumbleConnected)
	bridge.BridgeMutex.Unlock()
}

// TestBridge_MetricsChangeCallback tests callback invocation
func TestBridge_MetricsChangeCallback(t *testing.T) {
	bridge := createTestBridgeState(nil)

	var callCount int32
	bridge.MetricsChangeCallback = func() {
		atomic.AddInt32(&callCount, 1)
	}

	// Trigger notification
	bridge.notifyMetricsChange()

	// Wait for goroutine
	time.Sleep(50 * time.Millisecond)

	assert.Greater(t, atomic.LoadInt32(&callCount), int32(0))
}

// TestBridge_ConcurrentConnectionStateUpdates tests concurrent state updates
func TestBridge_ConcurrentConnectionStateUpdates(_ *testing.T) {
	bridge := createTestBridgeState(nil)

	var wg sync.WaitGroup

	// Concurrent EmitConnectionEvent calls
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if idx%2 == 0 {
				bridge.EmitConnectionEvent("discord", idx%5, idx%2 == 0, nil)
			} else {
				bridge.EmitConnectionEvent("mumble", idx%5, idx%2 == 1, nil)
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = bridge.GetConnectionStates()
			_ = bridge.IsConnected()
		}()
	}

	wg.Wait()
}

// TestBridge_StopBridgeSignal tests BridgeDie channel behavior
func TestBridge_StopBridgeSignal(t *testing.T) {
	bridge := createTestBridgeState(nil)

	// Sending to BridgeDie should not block (buffered)
	select {
	case bridge.BridgeDie <- true:
		// Success
	default:
		t.Fatal("BridgeDie channel should be buffered")
	}

	// Second send should use select default
	select {
	case bridge.BridgeDie <- true:
		// Might succeed if first was consumed
	default:
		// Also fine - channel might be full
	}
}

// TestBridge_ModeString tests BridgeMode String method
func TestBridge_ModeString(t *testing.T) {
	assert.Equal(t, "auto", BridgeModeAuto.String())
	assert.Equal(t, "manual", BridgeModeManual.String())
	assert.Equal(t, "constant", BridgeModeConstant.String())
}

// TestBridge_ConcurrentModeAccess tests concurrent mode access
func TestBridge_ConcurrentModeAccess(_ *testing.T) {
	bridge := createTestBridgeState(nil)

	var wg sync.WaitGroup

	modes := []BridgeMode{BridgeModeAuto, BridgeModeManual, BridgeModeConstant}

	// Concurrent mode changes
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			bridge.BridgeMutex.Lock()
			bridge.Mode = modes[idx%len(modes)]
			bridge.BridgeMutex.Unlock()
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bridge.BridgeMutex.Lock()
			_ = bridge.Mode
			bridge.BridgeMutex.Unlock()
		}()
	}

	wg.Wait()
}

// TestBridge_UserCountTracking tests user count accuracy
func TestBridge_UserCountTracking(t *testing.T) {
	bridge := createTestBridgeState(nil)

	// Add users
	bridge.MumbleUsersMutex.Lock()
	bridge.MumbleUsers["user1"] = true
	bridge.MumbleUsers["user2"] = true
	bridge.MumbleUsers["user3"] = true
	bridge.MumbleUserCount = 3
	bridge.MumbleUsersMutex.Unlock()

	bridge.DiscordUsersMutex.Lock()
	bridge.DiscordUsers["discord1"] = DiscordUser{username: "user1"}
	bridge.DiscordUsers["discord2"] = DiscordUser{username: "user2"}
	bridge.DiscordUsersMutex.Unlock()

	// Verify counts
	bridge.MumbleUsersMutex.Lock()
	assert.Equal(t, 3, len(bridge.MumbleUsers))
	assert.Equal(t, 3, bridge.MumbleUserCount)
	bridge.MumbleUsersMutex.Unlock()

	bridge.DiscordUsersMutex.Lock()
	assert.Equal(t, 2, len(bridge.DiscordUsers))
	bridge.DiscordUsersMutex.Unlock()
}

// TestBridge_WaitExitUsage tests WaitGroup usage
func TestBridge_WaitExitUsage(t *testing.T) {
	bridge := createTestBridgeState(nil)

	// Add to wait group
	bridge.WaitExit.Add(3)

	var completed int32

	// Complete work
	for i := 0; i < 3; i++ {
		go func() {
			defer bridge.WaitExit.Done()
			atomic.AddInt32(&completed, 1)
		}()
	}

	// Wait should complete
	done := make(chan struct{})
	go func() {
		bridge.WaitExit.Wait()
		close(done)
	}()

	select {
	case <-done:
		assert.Equal(t, int32(3), atomic.LoadInt32(&completed))
	case <-time.After(2 * time.Second):
		t.Fatal("WaitExit.Wait() timed out")
	}
}

// TestBridge_ConnectionManagerNilSafe tests nil manager handling
func TestBridge_ConnectionManagerNilSafe(t *testing.T) {
	bridge := createTestBridgeState(nil)

	// Managers should be nil initially
	assert.Nil(t, bridge.DiscordVoiceConnectionManager)
	assert.Nil(t, bridge.MumbleConnectionManager)

	// EmitConnectionEvent should still work
	assert.NotPanics(t, func() {
		bridge.EmitConnectionEvent("discord", 0, false, nil)
		bridge.EmitConnectionEvent("mumble", 0, false, nil)
	})
}

// TestBridge_ContextCancellation tests context-based shutdown
func TestBridge_ContextCancellation(t *testing.T) {
	bridge := createTestBridgeState(nil)

	ctx, cancel := context.WithCancel(context.Background())
	bridge.connectionCtx = ctx
	bridge.connectionCancel = cancel

	// Cancel should work
	cancel()

	// Context should be done
	select {
	case <-bridge.connectionCtx.Done():
		// Success
	default:
		t.Fatal("Context should be canceled")
	}
}

// TestBridge_StartTimeSetting tests StartTime field
func TestBridge_StartTimeSetting(t *testing.T) {
	bridge := createTestBridgeState(nil)

	// Initially zero
	assert.True(t, bridge.StartTime.IsZero())

	// Set start time
	now := time.Now()
	bridge.BridgeMutex.Lock()
	bridge.StartTime = now
	bridge.BridgeMutex.Unlock()

	bridge.BridgeMutex.Lock()
	assert.Equal(t, now, bridge.StartTime)
	bridge.BridgeMutex.Unlock()
}

// TestBridge_EmitUserEvent tests user event emission
func TestBridge_EmitUserEvent(t *testing.T) {
	bridge := createTestBridgeState(nil)

	// Without BridgeInstance, should not panic
	assert.NotPanics(t, func() {
		bridge.EmitUserEvent("discord", 0, "testuser", nil)
		bridge.EmitUserEvent("mumble", 1, "testuser", nil)
	})
}

// TestBridge_DiscordUserStruct tests DiscordUser struct fields
func TestBridge_DiscordUserStruct(t *testing.T) {
	user := DiscordUser{
		username: "TestUser",
		seen:     true,
		dmID:     "",
	}

	assert.Equal(t, "TestUser", user.username)
	assert.True(t, user.seen)
	assert.Empty(t, user.dmID)
}

// TestBridgeState_StopDiscordVoice tests that StopDiscordVoice properly cleans up
// the Discord voice connection without affecting Mumble. This is critical for
// constant mode reconnection cycles to prevent the old voice websocket from
// causing a visible "rejoin" when a new connection is established.
func TestBridgeState_StopDiscordVoice(t *testing.T) {
	bridge := createTestBridgeState(nil)

	// Create a context that can be canceled
	ctx, cancel := context.WithCancel(context.Background())
	bridge.connectionCtx = ctx
	bridge.connectionCancel = cancel

	// Simulate a connection monitoring goroutine that exits when context is canceled
	monitorExited := atomic.Bool{}
	bridge.connectionWg.Add(1)
	go func() {
		defer bridge.connectionWg.Done()
		<-bridge.connectionCtx.Done()
		monitorExited.Store(true)
	}()

	// Create a real DiscordVoiceConnectionManager (nil client is fine for Stop)
	bridge.DiscordVoiceConnectionManager = NewDiscordVoiceConnectionManager(
		nil, "test-guild", "test-channel", bridge.Logger, nil,
	)

	// Call StopDiscordVoice
	bridge.StopDiscordVoice()

	// Verify context was canceled
	select {
	case <-bridge.connectionCtx.Done():
		// Expected
	default:
		t.Fatal("connectionCtx should be canceled")
	}

	// Verify the monitoring goroutine exited
	assert.True(t, monitorExited.Load(), "monitoring goroutine should have exited")
}

func TestBridge_StartBridgeFailureClearsActiveAndReadiness(t *testing.T) {
	for _, stage := range []string{"initialize", "start"} {
		t.Run(stage, func(t *testing.T) {
			state := createTestBridgeState(nil)
			if stage == "initialize" {
				state.initializeManagers = func() error { return errors.New("offline initialization failure") }
			} else {
				state.initializeManagers = func() error { return nil }
				state.startManagers = func() error { return errors.New("offline start failure") }
				state.stopManagers = func() {}
			}
			state.StartBridge()

			state.BridgeMutex.Lock()
			defer state.BridgeMutex.Unlock()
			require.False(t, state.BridgeActive)
			require.False(t, state.Connected)
			require.False(t, state.DiscordConnected)
			require.False(t, state.MumbleConnected)
			require.Nil(t, state.bridgeCancel)
			require.Nil(t, state.bridgeDone)
			require.True(t, state.StartTime.IsZero())
		})
	}
}

func TestBridge_StopBeforePublicationConsumesOnlyRegisteredIntent(t *testing.T) {
	state := createTestBridgeState(nil)
	var initializeCalls atomic.Int32
	state.initializeManagers = func() error {
		initializeCalls.Add(1)
		return errors.New("offline initialization failure")
	}

	state.RegisterBridgeStartIntent()
	state.StopBridge()
	state.BridgeMutex.Lock()
	require.True(t, state.stopPending)
	require.Equal(t, 1, state.startIntents)
	state.BridgeMutex.Unlock()

	state.StartBridgeFromIntent()
	require.Zero(t, initializeCalls.Load(), "pending stop must consume the registered start before publication")
	state.BridgeMutex.Lock()
	require.False(t, state.stopPending)
	require.Zero(t, state.startIntents)
	state.BridgeMutex.Unlock()

	state.StartBridge()
	require.Equal(t, int32(1), initializeCalls.Load(), "consumed stop must not poison a later legitimate start")
	state.StopBridge() // idle stop with no intent must also remain non-poisoning
	state.StartBridge()
	require.Equal(t, int32(2), initializeCalls.Load())
}

func TestBridge_CanceledStartIntentClearsPendingStop(t *testing.T) {
	state := createTestBridgeState(nil)
	state.RegisterBridgeStartIntent()
	state.StopBridge()
	state.CancelBridgeStartIntent()

	state.BridgeMutex.Lock()
	require.False(t, state.stopPending)
	require.Zero(t, state.startIntents)
	state.BridgeMutex.Unlock()
}

func TestBridge_StopPublicationInterleavingsJoinFullCleanup(t *testing.T) {
	for iteration := 0; iteration < 100; iteration++ {
		state := createTestBridgeState(nil)
		state.BridgeConfig.MumbleConfig = nil
		enteredStart := make(chan struct{})
		releaseStart := make(chan struct{})
		state.initializeManagers = func() error { return nil }
		state.stopManagers = func() {}
		state.startManagers = func() error {
			close(enteredStart)
			<-releaseStart
			return nil
		}
		state.RegisterBridgeStartIntent()
		go state.StartBridgeFromIntent()
		<-enteredStart
		state.BridgeMutex.Lock()
		sessionCtx := state.bridgeCtx
		state.BridgeMutex.Unlock()
		require.NotNil(t, sessionCtx)
		stopDone := make(chan struct{})
		go func() {
			state.StopBridge()
			close(stopDone)
		}()
		<-sessionCtx.Done()
		close(releaseStart)

		select {
		case <-stopDone:
		case <-time.After(time.Second):
			t.Fatalf("iteration %d: StopBridge did not join full session cleanup", iteration)
		}
		require.True(t, state.lock.TryLock(), "iteration %d: session lock finalizer was not complete", iteration)
		state.lock.Unlock()
		state.BridgeMutex.Lock()
		require.False(t, state.BridgeActive)
		require.False(t, state.Connected)
		require.Nil(t, state.bridgeDone)
		state.BridgeMutex.Unlock()
		require.NotNil(t, state.WaitExit)
		state.WaitExit.Wait()
		require.NotNil(t, state.DiscordStream)
		require.Nil(t, state.DiscordStream.cleanupCancel)
		require.NotNil(t, state.MumbleStream)
		state.MumbleStream.mutex.Lock()
		require.Empty(t, state.MumbleStream.streams)
		state.MumbleStream.mutex.Unlock()
	}
}

func TestBridge_DiscordDirectMessageCountSnapshotIsSynchronized(t *testing.T) {
	state := createTestBridgeState(nil)
	state.BridgeConfig.DiscordTextMode = "user"
	state.DiscordClient = &mockDiscordClient{}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for worker := 0; worker < 20; worker++ {
		wg.Add(2)
		go func(id int) {
			defer wg.Done()
			<-start
			for iteration := 0; iteration < 100; iteration++ {
				key := fmt.Sprintf("user-%d-%d", id, iteration)
				state.DiscordUsersMutex.Lock()
				state.DiscordUsers[key] = DiscordUser{username: key, dmID: "dm-" + key}
				delete(state.DiscordUsers, fmt.Sprintf("user-%d-%d", id, iteration-1))
				state.DiscordUsersMutex.Unlock()
			}
		}(worker)
		go func() {
			defer wg.Done()
			<-start
			for iteration := 0; iteration < 100; iteration++ {
				state.discordSendMessage("offline")
			}
		}()
	}
	close(start)
	wg.Wait()
}

func TestBridge_TeardownFailsReadinessAndResetsSessionGauges(t *testing.T) {
	state := createTestBridgeState(nil)
	state.BridgeMutex.Lock()
	state.Connected = true
	state.DiscordConnected = true
	state.MumbleConnected = true
	state.DiscordOutboundHealth = discordOutboundHealthy
	state.StartTime = time.Now()
	state.BridgeMutex.Unlock()
	promDiscordConnectionStatus.Set(float64(ConnectionConnected))
	promMumbleConnectionStatus.Set(float64(ConnectionConnected))
	promDiscordConnectionUptime.Set(12)
	promMumbleConnectionUptime.Set(12)
	promMumbleBufferedPackets.Set(3)
	promMumbleArraySize.Set(3)
	promMumbleStreaming.Set(2)
	promMumbleMaxStreamDepth.Set(4)
	promDiscordArraySize.Set(3)
	promDiscordStreaming.Set(2)
	promToDiscordJitterBuffer.Set(2)
	promRtpTimestampDrift.Set(1)

	state.failReadinessAndResetMetrics()
	require.False(t, state.IsConnected())
	require.Zero(t, testutil.ToFloat64(promDiscordConnectionStatus))
	require.Zero(t, testutil.ToFloat64(promMumbleConnectionStatus))
	require.Zero(t, testutil.ToFloat64(promDiscordConnectionUptime))
	require.Zero(t, testutil.ToFloat64(promMumbleConnectionUptime))
	require.Zero(t, testutil.ToFloat64(promMumbleBufferedPackets))
	require.Zero(t, testutil.ToFloat64(promMumbleArraySize))
	require.Zero(t, testutil.ToFloat64(promMumbleStreaming))
	require.Zero(t, testutil.ToFloat64(promMumbleMaxStreamDepth))
	require.Zero(t, testutil.ToFloat64(promDiscordArraySize))
	require.Zero(t, testutil.ToFloat64(promDiscordStreaming))
	require.Zero(t, testutil.ToFloat64(promToDiscordJitterBuffer))
	require.Zero(t, testutil.ToFloat64(promRtpTimestampDrift))
}
