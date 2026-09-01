package bridge

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/stieneee/gumble/gumble"
	"github.com/stieneee/mumble-discord-bridge/pkg/logger"
)

// MumbleConnectionManager manages Mumble connections with automatic reconnection
type MumbleConnectionManager struct {
	*BaseConnectionManager
	client        *gumble.Client
	config        *gumble.Config
	address       string
	tlsConfig     *tls.Config
	clientMutex   sync.RWMutex
	audioMutex    sync.Mutex                   // Serializes AudioOutgoing sends and its sole close owner.
	configMutex   sync.RWMutex                 // Protects address, config, tlsConfig, and targetChannel
	disconnectCh  chan *gumble.DisconnectEvent // Channel to signal disconnection events
	disconnectMux sync.Mutex                   // Serializes disconnect event publication

	// Cached audio channel — created and closed exactly once by this manager per client.
	audioOutgoing  chan<- gumble.AudioBuffer // Protected by audioMutex
	managerConfig  *ConnectionManagerConfig
	connectFunc    func(context.Context, uint64) error
	targetChannel  []string
	connectTimeout time.Duration
	dialFunc       func(*net.Dialer, string, *gumble.Config, *tls.Config) (*gumble.Client, error)

	runMutex   sync.Mutex // Lock order: runMutex -> clientMutex -> audioMutex.
	generation uint64
	running    bool
	loopWg     sync.WaitGroup
	stopSignal sync.Once
	stopCh     chan struct{}
}

// NewMumbleConnectionManager creates a new Mumble connection manager
func NewMumbleConnectionManager(address string, config *gumble.Config, tlsConfig *tls.Config, logger logger.Logger, eventEmitter BridgeEventEmitter) *MumbleConnectionManager {
	base := NewBaseConnectionManager(logger, "mumble", eventEmitter)

	manager := &MumbleConnectionManager{
		BaseConnectionManager: base,
		address:               address,
		config:                config,
		tlsConfig:             tlsConfig,
		disconnectCh:          make(chan *gumble.DisconnectEvent, 1),
		managerConfig:         DefaultConnectionManagerConfig(),
		connectTimeout:        30 * time.Second,
		dialFunc:              gumble.DialWithDialer,
		stopCh:                make(chan struct{}),
	}
	manager.connectFunc = manager.connect

	// Attach this connection manager as an event listener to the gumble config
	if config != nil {
		config.Attach(manager)
	}

	return manager
}

// Start begins the Mumble connection process with automatic reconnection
func (m *MumbleConnectionManager) Start(ctx context.Context) error {
	m.logger.Info("MUMBLE_CONN", "Starting Mumble connection manager")

	// Initialize context for proper cancellation chain
	m.InitContext(ctx)
	m.runMutex.Lock()
	m.generation++
	m.running = true
	m.runMutex.Unlock()

	// Start connection management goroutine
	m.loopWg.Add(1)
	go func() {
		defer m.loopWg.Done()
		m.connectionLoop(m.ctx)
	}()

	return nil
}

// connectionLoop manages the connection lifecycle with reconnection logic
func (m *MumbleConnectionManager) connectionLoop(ctx context.Context) {
	defer m.disconnectInternal()
	m.runMutex.Lock()
	if !m.running {
		m.generation++
		m.running = true
	}
	generation := m.generation
	m.runMutex.Unlock()
	defer func() {
		m.runMutex.Lock()
		if m.generation == generation {
			m.running = false
		}
		m.runMutex.Unlock()
	}()

	retries := 0
	for {
		// Check if we're in a permanent failure state (kicked/banned)
		if m.GetStatus() == ConnectionFailed {
			m.logger.Info("MUMBLE_CONN", "Connection in permanent failure state, stopping reconnection attempts")

			return
		}

		select {
		case <-ctx.Done():
			m.logger.Info("MUMBLE_CONN", "Connection loop canceled")

			return
		case <-m.stopCh:
			return
		default:
		}

		// Attempt connection
		if err := m.connectFunc(ctx, generation); err != nil {
			m.logger.Error("MUMBLE_CONN", fmt.Sprintf("Connection failed: %v", err))
			if retries >= m.managerConfig.MaxRetries {
				m.SetStatus(ConnectionFailed, err)
				return
			}
			m.SetStatus(ConnectionReconnecting, err)
			delay := retryDelay(m.managerConfig, retries)
			retries++
			if !waitRetry(ctx, delay) {
				return
			}
			continue
		}

		// Connection successful - check if context is still active
		m.runMutex.Lock()
		if !m.running || m.generation != generation || ctx.Err() != nil {
			m.runMutex.Unlock()
			return
		}
		retries = 0
		m.SetStatus(ConnectionConnected, nil)
		m.runMutex.Unlock()
		m.logger.Info("MUMBLE_CONN", "Mumble connection established")

		// Wait for disconnect event
		select {
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		case disconnectEvent, ok := <-m.disconnectCh:
			if !ok {
				return
			}
			if disconnectEvent == nil {
				m.logger.Warn("MUMBLE_CONN", "Ignoring nil disconnect event")
				continue
			}
			m.handleDisconnectEvent(disconnectEvent)
			// Check if this was a permanent failure (kicked/banned)
			if m.GetStatus() == ConnectionFailed {
				m.logger.Info("MUMBLE_CONN", "Permanent failure, exiting connection loop")

				return
			}
		}
	}
}

// connect establishes a Mumble connection
func (m *MumbleConnectionManager) connect(ctx context.Context, generation uint64) error {
	m.SetStatus(ConnectionConnecting, nil)

	// Read configuration under lock
	m.configMutex.RLock()
	address := m.address
	config := m.config
	tlsConfig := m.tlsConfig
	m.configMutex.RUnlock()

	// Log connection attempt with redacted sensitive info
	configDebug := m.getRedactedConfigInfo()
	tlsDebug := m.getRedactedTLSInfo()
	m.logger.Debug("MUMBLE_CONN", fmt.Sprintf("Connecting to Mumble: Address=%s, Config=%+v, TLS=%+v",
		address, configDebug, tlsDebug))

	// Disconnect any existing connection
	m.disconnectInternal()

	// Attempt Mumble connection
	dialer := &net.Dialer{Timeout: m.connectTimeout}
	client, err := m.dialFunc(dialer, address, config, tlsConfig)
	if err != nil {
		m.logger.Error("MUMBLE_CONN", fmt.Sprintf("Failed to dial Mumble server %s: %v", address, err))

		return fmt.Errorf("failed to connect to Mumble server: %w", err)
	}
	if err := m.moveToTargetChannel(client); err != nil {
		_ = client.Disconnect()
		return err
	}

	// Publish only into the still-active manager generation. Stop takes runMutex
	// before cancellation, so a late successful dial cannot resurrect a client.
	m.runMutex.Lock()
	if !m.running || m.generation != generation || ctx.Err() != nil {
		m.runMutex.Unlock()
		_ = client.Disconnect()
		return context.Canceled
	}
	m.clientMutex.Lock()
	m.client = client
	m.clientMutex.Unlock()
	m.audioMutex.Lock()
	m.audioOutgoing = client.AudioOutgoing()
	m.audioMutex.Unlock()
	m.runMutex.Unlock()

	m.logger.Debug("MUMBLE_CONN", fmt.Sprintf("Mumble connection established successfully to %s, client state: %d",
		address, client.State()))

	return nil
}

// SetTargetChannel configures the exact channel path; an empty path explicitly means root.
func (m *MumbleConnectionManager) SetTargetChannel(channel []string) {
	m.configMutex.Lock()
	m.targetChannel = append([]string(nil), channel...)
	m.configMutex.Unlock()
}

func (m *MumbleConnectionManager) moveToTargetChannel(client *gumble.Client) error {
	m.configMutex.RLock()
	path := append([]string(nil), m.targetChannel...)
	m.configMutex.RUnlock()

	var target *gumble.Channel
	client.Do(func() {
		if len(path) == 0 {
			target = client.Channels[0]
		} else {
			target = client.Channels.Find(path...)
		}
		if target != nil && client.Self != nil && (client.Self.Channel == nil || client.Self.Channel.ID != target.ID) {
			client.Self.Move(target)
		}
	})
	if target == nil {
		return fmt.Errorf("configured Mumble channel not found")
	}

	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	var done <-chan struct{}
	if m.ctx != nil {
		done = m.ctx.Done()
	}
	for {
		moved := false
		client.Do(func() {
			moved = client.Self != nil && client.Self.Channel != nil && client.Self.Channel.ID == target.ID
		})
		if moved {
			return nil
		}
		select {
		case <-done:
			return m.ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("Mumble channel move was not confirmed")
		case <-ticker.C:
		}
	}
}

// disconnectInternal retires the current client and its manager-owned audio
// channel. audioMutex prevents a close from racing with SendAudio.
func (m *MumbleConnectionManager) disconnectInternal() {
	m.clientMutex.Lock()
	client := m.client
	m.client = nil
	m.clientMutex.Unlock()

	m.audioMutex.Lock()
	if m.audioOutgoing != nil {
		close(m.audioOutgoing)
		m.audioOutgoing = nil
	}
	m.audioMutex.Unlock()

	if client != nil {
		m.logger.Debug("MUMBLE_CONN", "Disconnecting from Mumble")
		if err := client.Disconnect(); err != nil {
			m.logger.Error("MUMBLE_CONN", fmt.Sprintf("Error disconnecting from Mumble: %v", err))
		}
	}
}

// handleDisconnectEvent processes different types of disconnect events
func (m *MumbleConnectionManager) handleDisconnectEvent(event *gumble.DisconnectEvent) {
	if event == nil {
		m.logger.Warn("MUMBLE_CONN", "Ignoring nil disconnect event")
		return
	}

	switch event.Type {
	case gumble.DisconnectError:
		m.SetStatus(ConnectionReconnecting, fmt.Errorf("connection error: %s", event.String))
		m.logger.Warn("MUMBLE_CONN", fmt.Sprintf("Connection lost due to error: %s, attempting reconnection", event.String))
	case gumble.DisconnectKicked:
		m.SetStatus(ConnectionFailed, fmt.Errorf("kicked from server: %s", event.String))
		m.logger.Error("MUMBLE_CONN", fmt.Sprintf("Kicked from server: %s", event.String))
	case gumble.DisconnectBanned:
		m.SetStatus(ConnectionFailed, fmt.Errorf("banned from server: %s", event.String))
		m.logger.Error("MUMBLE_CONN", fmt.Sprintf("Banned from server: %s", event.String))
	case gumble.DisconnectUser:
		m.SetStatus(ConnectionReconnecting, nil)
		m.logger.Info("MUMBLE_CONN", "User-initiated disconnect, attempting reconnection")
	default:
		m.SetStatus(ConnectionReconnecting, fmt.Errorf("unknown disconnect: %s", event.String))
		m.logger.Warn("MUMBLE_CONN", fmt.Sprintf("Unknown disconnect type: %s, attempting reconnection", event.String))
	}
}

// Stop gracefully stops the Mumble connection manager
func (m *MumbleConnectionManager) Stop() error {
	m.logger.Info("MUMBLE_CONN", "Stopping Mumble connection manager")

	m.runMutex.Lock()
	m.running = false
	m.runMutex.Unlock()
	m.stopSignal.Do(func() { close(m.stopCh) })

	// Stop the base connection manager (cancels context)
	if err := m.BaseConnectionManager.Stop(); err != nil {
		m.logger.Error("MUMBLE_CONN", fmt.Sprintf("Error stopping base connection manager: %v", err))
	}
	m.loopWg.Wait()

	// Disconnect from Mumble
	m.disconnectInternal()

	return nil
}

// GetClient returns the current Mumble client (thread-safe)
func (m *MumbleConnectionManager) GetClient() *gumble.Client {
	m.clientMutex.RLock()
	defer m.clientMutex.RUnlock()

	return m.client
}

// EventListener implementation for gumble events
// We only care about Connect and Disconnect events for connection management

// OnConnect handles gumble connection events
func (m *MumbleConnectionManager) OnConnect(_ *gumble.ConnectEvent) {
	m.logger.Info("MUMBLE_CONN", "Connection event received")
	// Connection events are already handled by the connection loop
}

// OnDisconnect handles gumble disconnection events and signals the connection loop
func (m *MumbleConnectionManager) OnDisconnect(e *gumble.DisconnectEvent) {
	if e == nil {
		return
	}
	m.logger.Warn("MUMBLE_CONN", fmt.Sprintf("Disconnect event received: %s", e.String))

	// Signal the connection loop about the disconnection
	m.disconnectMux.Lock()
	defer m.disconnectMux.Unlock()

	m.runMutex.Lock()
	running := m.running
	m.runMutex.Unlock()
	m.clientMutex.RLock()
	current := m.client
	m.clientMutex.RUnlock()
	if !running || (e.Client != nil && e.Client != current) {
		m.logger.Debug("MUMBLE_CONN", "Ignoring stale or stopped-generation disconnect event")
		return
	}

	select {
	case m.disconnectCh <- e:
		// Successfully sent disconnect signal
	default:
		// Channel is full, no need to send another event
		m.logger.Debug("MUMBLE_CONN", "Disconnect channel full, skipping event")
	}
}

// Required EventListener interface methods (unused for connection management)

// OnTextMessage implements gumble.EventListener interface (unused)
func (m *MumbleConnectionManager) OnTextMessage(_ *gumble.TextMessageEvent) {}

// OnUserChange implements gumble.EventListener interface (unused)
func (m *MumbleConnectionManager) OnUserChange(_ *gumble.UserChangeEvent) {}

// OnChannelChange implements gumble.EventListener interface (unused)
func (m *MumbleConnectionManager) OnChannelChange(_ *gumble.ChannelChangeEvent) {}

// OnPermissionDenied implements gumble.EventListener interface (unused)
func (m *MumbleConnectionManager) OnPermissionDenied(_ *gumble.PermissionDeniedEvent) {}

// OnUserList implements gumble.EventListener interface (unused)
func (m *MumbleConnectionManager) OnUserList(_ *gumble.UserListEvent) {}

// OnACL implements gumble.EventListener interface (unused)
func (m *MumbleConnectionManager) OnACL(_ *gumble.ACLEvent) {}

// OnBanList implements gumble.EventListener interface (unused)
func (m *MumbleConnectionManager) OnBanList(_ *gumble.BanListEvent) {}

// OnContextActionChange implements gumble.EventListener interface (unused)
func (m *MumbleConnectionManager) OnContextActionChange(_ *gumble.ContextActionChangeEvent) {}

// OnServerConfig implements gumble.EventListener interface (unused)
func (m *MumbleConnectionManager) OnServerConfig(_ *gumble.ServerConfigEvent) {}

// SendAudio synchronizes the final handoff with client retirement. The manager
// remains the sole owner allowed to close AudioOutgoing.
func (m *MumbleConnectionManager) SendAudio(ctx context.Context, packet gumble.AudioBuffer, timeout time.Duration) bool {
	m.audioMutex.Lock()
	defer m.audioMutex.Unlock()
	if m.audioOutgoing == nil {
		return false
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case m.audioOutgoing <- packet:
		return true
	case <-ctx.Done():
		return false
	case <-timer.C:
		return false
	}
}

// GetSelfName safely returns the client's own name
func (m *MumbleConnectionManager) GetSelfName() string {
	m.clientMutex.RLock()
	client := m.client
	m.clientMutex.RUnlock()

	if client == nil {
		return ""
	}

	var name string
	client.Do(func() {
		if client.Self != nil {
			name = client.Self.Name
		}
	})

	return name
}

// GetChannelUsers safely returns the users in the client's current channel
func (m *MumbleConnectionManager) GetChannelUsers() []*gumble.User {
	m.clientMutex.RLock()
	client := m.client
	m.clientMutex.RUnlock()

	if client == nil {
		return []*gumble.User{}
	}

	var usersCopy []*gumble.User
	client.Do(func() {
		if client.Self != nil && client.Self.Channel != nil {
			// Create a proper copy of the users to avoid concurrent access issues
			for _, user := range client.Self.Channel.Users {
				if user != nil {
					usersCopy = append(usersCopy, user)
				}
			}
		}
	})

	return usersCopy
}

// Note: Audio listeners should be attached to the config before connection,
// not to the connection manager, to ensure they're active when client connects

// UpdateConfig updates the Mumble configuration (requires reconnection for most changes)
func (m *MumbleConnectionManager) UpdateConfig(newConfig *gumble.Config) error {
	m.logger.Info("MUMBLE_CONN", "Updating Mumble configuration")

	m.configMutex.Lock()
	m.config = newConfig
	m.configMutex.Unlock()

	// If currently connected, disconnect to trigger reconnection
	if m.IsConnected() {
		m.logger.Info("MUMBLE_CONN", "Disconnecting to apply config change")
		m.disconnectInternal()
	}

	return nil
}

// UpdateAddress updates the Mumble server address (requires reconnection)
func (m *MumbleConnectionManager) UpdateAddress(address string) error {
	m.configMutex.Lock()
	if m.address == address {
		m.configMutex.Unlock()

		return nil
	}

	m.logger.Info("MUMBLE_CONN", fmt.Sprintf("Changing address from %s to %s", m.address, address))
	m.address = address
	m.configMutex.Unlock()

	// If currently connected, disconnect to trigger reconnection
	if m.IsConnected() {
		m.logger.Info("MUMBLE_CONN", "Disconnecting to apply address change")
		m.disconnectInternal()
	}

	return nil
}

// GetAddress returns the Mumble server address
func (m *MumbleConnectionManager) GetAddress() string {
	m.configMutex.RLock()
	defer m.configMutex.RUnlock()

	return m.address
}

// GetConfig returns the Mumble configuration
func (m *MumbleConnectionManager) GetConfig() *gumble.Config {
	m.configMutex.RLock()
	defer m.configMutex.RUnlock()

	return m.config
}

// getRedactedConfigInfo returns config info with sensitive fields redacted for logging
func (m *MumbleConnectionManager) getRedactedConfigInfo() map[string]any {
	m.configMutex.RLock()
	config := m.config
	m.configMutex.RUnlock()

	if config == nil {
		return map[string]any{"config": "nil"}
	}

	return map[string]any{
		"Username":       config.Username,
		"Password":       fmt.Sprintf("[REDACTED - %d chars]", len(config.Password)),
		"Tokens":         fmt.Sprintf("[%d tokens]", len(config.Tokens)),
		"AudioInterval":  config.AudioInterval.String(),
		"AudioDataBytes": config.AudioDataBytes,
		"AudioFrameSize": config.AudioFrameSize(),
		"ClientType":     config.ClientType,
	}
}

// getRedactedTLSInfo returns TLS config info with sensitive fields redacted for logging
func (m *MumbleConnectionManager) getRedactedTLSInfo() map[string]any {
	m.configMutex.RLock()
	tlsConfig := m.tlsConfig
	m.configMutex.RUnlock()

	if tlsConfig == nil {
		return map[string]any{"tls": "nil"}
	}

	return map[string]any{
		"InsecureSkipVerify": tlsConfig.InsecureSkipVerify,
		"ServerName":         tlsConfig.ServerName,
		"MinVersion":         tlsConfig.MinVersion,
		"MaxVersion":         tlsConfig.MaxVersion,
		"CipherSuites":       "[REDACTED]",
		"Certificates":       fmt.Sprintf("[%d certificates]", len(tlsConfig.Certificates)),
		"RootCAs":            fmt.Sprintf("[%v]", tlsConfig.RootCAs != nil),
		"ClientCAs":          fmt.Sprintf("[%v]", tlsConfig.ClientCAs != nil),
	}
}
