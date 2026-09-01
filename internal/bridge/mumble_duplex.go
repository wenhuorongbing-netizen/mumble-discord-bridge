package bridge

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/stieneee/gumble/gumble"
	_ "github.com/stieneee/gumble/opus" // Register opus codec
	"github.com/stieneee/mumble-discord-bridge/pkg/logger"
)

// Constants for Mumble audio handling
const (
	mumbleStreamBufferSize = 24  // Buffer size for individual audio streams (240ms, sufficient for TCP jitter)
	mumbleAudioChunkSize   = 480 // Samples per 10ms audio chunk
	mumbleMaxBufferDepth   = 6   // Max per-stream chunks before skipping old ones (60ms; prevents clock drift accumulation)
)

func mixPCM(sources ...[]int16) []int16 {
	length := 0
	for _, source := range sources {
		if len(source) > length {
			length = len(source)
		}
	}
	mixed := make([]int16, length)
	for index := range mixed {
		var sum int32
		for _, source := range sources {
			if index < len(source) {
				sum += int32(source[index])
			}
		}
		if sum > 32767 {
			sum = 32767
		} else if sum < -32768 {
			sum = -32768
		}
		mixed[index] = int16(sum)
	}
	return mixed
}

// MumbleDuplex - listener and outgoing
type MumbleDuplex struct {
	mutex   sync.Mutex
	streams []chan gumble.AudioBuffer
	logger  logger.Logger
	bridge  *BridgeState // Reference to bridge for connection manager access
	// Track stream cleanup for reconnections - using map for O(1) removal
	streamCleanupCallbacks map[chan gumble.AudioBuffer]func() // Protected by mutex
}

// NewMumbleDuplex creates a new Mumble audio duplex handler.
func NewMumbleDuplex(log logger.Logger, bridge *BridgeState) *MumbleDuplex {
	return &MumbleDuplex{
		streams:                make([]chan gumble.AudioBuffer, 0),
		logger:                 log,
		bridge:                 bridge,
		streamCleanupCallbacks: make(map[chan gumble.AudioBuffer]func()),
	}
}

// CleanupStreams forcibly closes all active audio streams (used during reconnections)
func (m *MumbleDuplex) CleanupStreams() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.logger.Info("MUMBLE_STREAM", fmt.Sprintf("Cleaning up %d active audio streams", len(m.streams)))

	// Call all cleanup callbacks to close streams gracefully
	for _, callback := range m.streamCleanupCallbacks {
		func() {
			defer func() {
				if r := recover(); r != nil {
					m.logger.Warn("MUMBLE_STREAM", fmt.Sprintf("Panic during stream cleanup: %v", r))
				}
			}()
			callback()
		}()
	}

	// Clear all streams and callbacks
	m.streams = make([]chan gumble.AudioBuffer, 0)
	m.streamCleanupCallbacks = make(map[chan gumble.AudioBuffer]func())

	promMumbleArraySize.Set(0)
	m.logger.Info("MUMBLE_STREAM", "Audio stream cleanup completed")
}

// OnAudioStream - Spawn routines to handle incoming packets with improved cleanup
func (m *MumbleDuplex) OnAudioStream(e *gumble.AudioStreamEvent) {
	stream := make(chan gumble.AudioBuffer, mumbleStreamBufferSize)
	streamClosed := false
	streamMutex := sync.Mutex{}

	m.mutex.Lock()
	m.streams = append(m.streams, stream)
	streamCount := len(m.streams)
	// Add cleanup callback for this stream using map for O(1) access
	done := make(chan struct{})
	m.streamCleanupCallbacks[stream] = func() {
		streamMutex.Lock()
		defer streamMutex.Unlock()
		if !streamClosed {
			close(stream)
			close(done)
			streamClosed = true
			if m.bridge != nil && m.bridge.voiceOnlyPrivacy() {
				m.logger.Debug("MUMBLE_STREAM", "Forcibly closed Mumble stream during cleanup")
			} else {
				m.logger.Debug("MUMBLE_STREAM", fmt.Sprintf("Forcibly closed stream for user: %s during cleanup", e.User.Name))
			}
		}
	}
	m.mutex.Unlock()

	promMumbleArraySize.Set(float64(streamCount))

	go func() {
		name := e.User.Name
		privacy := m.bridge != nil && m.bridge.voiceOnlyPrivacy()
		if privacy {
			m.logger.Info("MUMBLE_STREAM", "New Mumble audio stream")
		} else {
			m.logger.Info("MUMBLE_STREAM", fmt.Sprintf("New mumble audio stream: %s", name))
		}
		defer func() {
			if r := recover(); r != nil {
				if privacy {
					m.logger.Error("MUMBLE_STREAM", fmt.Sprintf("Panic in Mumble audio stream: %v", r))
				} else {
					m.logger.Error("MUMBLE_STREAM", fmt.Sprintf("Panic in audio stream for %s: %v", name, r))
				}
			}
		}()

	loop:
		for {
			select {
			case p, ok := <-e.C:
				if !ok {
					// gumble closed the audio stream channel
					break loop
				}

				// Hold lock once per packet instead of per chunk to reduce overhead
				streamMutex.Lock()
				if streamClosed {
					streamMutex.Unlock()

					break loop
				}

				// Process all audio chunks while holding lock
				for i := 0; i < len(p.AudioBuffer)/mumbleAudioChunkSize; i++ {
					start := mumbleAudioChunkSize * i
					end := mumbleAudioChunkSize * (i + 1)
					// Non-blocking send to avoid hanging on closed stream
					select {
					case stream <- p.AudioBuffer[start:end]:
					default:
						// Stream buffer full, drop packet
						if privacy {
							m.logger.Debug("MUMBLE_STREAM", "Mumble stream buffer full, dropping packet")
						} else {
							m.logger.Debug("MUMBLE_STREAM", fmt.Sprintf("Stream buffer full for %s, dropping packet", name))
						}
					}
				}
				streamMutex.Unlock()

				promReceivedMumblePackets.Inc()

			case <-done:
				break loop
			}
		}

		if privacy {
			m.logger.Info("MUMBLE_STREAM", "Mumble audio stream ended")
		} else {
			m.logger.Info("MUMBLE_STREAM", fmt.Sprintf("Mumble audio stream ended: %s", name))
		}

		// Cleanup stream from arrays
		m.mutex.Lock()
		defer m.mutex.Unlock()

		// Close stream safely
		streamMutex.Lock()
		if !streamClosed {
			close(stream)
			streamClosed = true
		}
		streamMutex.Unlock()

		// Remove stream from array
		for i := 0; i < len(m.streams); i++ {
			if m.streams[i] == stream {
				m.streams = append(m.streams[:i], m.streams[i+1:]...)

				break
			}
		}

		// Remove cleanup callback using map for O(1) removal
		delete(m.streamCleanupCallbacks, stream)

		promMumbleArraySize.Set(float64(len(m.streams)))
	}()
}

// MixOneChunk reads one 10ms chunk from all active Mumble audio streams and mixes them.
// Returns the mixed audio buffer and the number of active streams, or nil if no streams have data.
// Thread-safe: acquires mutex internally.
func (m *MumbleDuplex) MixOneChunk() (mixed []int16, streamingCount int) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	var sources [][]int16
	var maxDepth int

	for i := range m.streams {
		bufLen := len(m.streams[i])
		if bufLen > maxDepth {
			maxDepth = bufLen
		}
		if bufLen > 0 {
			streamingCount++
			// Skip old chunks when buffer is too deep. This happens when the Mumble
			// server's clock runs faster than ours — audio accumulates faster than
			// we consume it. Without this, delay grows monotonically over time.
			if bufLen > mumbleMaxBufferDepth {
				skip := bufLen - mumbleMaxBufferDepth
				for range skip {
					<-m.streams[i]
				}
				promMumbleChunksSkipped.Add(float64(skip))
			}
			audioData := <-m.streams[i]
			sources = append(sources, audioData)
		}
	}

	promMumbleMaxStreamDepth.Set(float64(maxDepth))

	if streamingCount == 0 {
		return nil, 0
	}

	return mixPCM(sources...), streamingCount
}

// toMumbleSender sends audio through the manager-owned final handoff.
func (m *MumbleDuplex) toMumbleSender(ctx context.Context, internalChan <-chan gumble.AudioBuffer) {
	const sendTimeout = 20 * time.Millisecond

	for {
		select {
		case <-ctx.Done():
			return

		case packet, ok := <-internalChan:
			if !ok {
				m.logger.Info("MUMBLE_FORWARDER", "Internal audio channel closed, stopping toMumbleSender")

				return
			}
			promMumbleBufferedPackets.Set(float64(len(internalChan)))

			manager := m.bridge.MumbleConnectionManager
			if manager == nil {
				promPacketsSunk.WithLabelValues("mumble", "inbound").Inc()
				continue
			}
			if manager.SendAudio(ctx, packet, sendTimeout) {
				promSentMumblePackets.Inc()
			} else if ctx.Err() == nil {
				promMumbleSendTimeouts.Inc()
				promToMumbleDropped.Inc()
			}
		}
	}
}
