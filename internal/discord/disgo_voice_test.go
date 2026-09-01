package discord

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/disgoorg/disgo/gateway"
	"github.com/disgoorg/disgo/voice"
	"github.com/disgoorg/snowflake/v2"
	"github.com/stretchr/testify/require"
)

type failedOpenVoiceConn struct {
	closed  atomic.Int32
	onClose func()
}

func (*failedOpenVoiceConn) Gateway() voice.Gateway                                 { return nil }
func (*failedOpenVoiceConn) UDP() voice.UDPConn                                     { return nil }
func (*failedOpenVoiceConn) ChannelID() *snowflake.ID                               { return nil }
func (*failedOpenVoiceConn) GuildID() snowflake.ID                                  { return 1 }
func (*failedOpenVoiceConn) UserIDBySSRC(uint32) snowflake.ID                       { return 0 }
func (*failedOpenVoiceConn) SetSpeaking(context.Context, voice.SpeakingFlags) error { return nil }
func (*failedOpenVoiceConn) SetOpusFrameProvider(voice.OpusFrameProvider)           {}
func (*failedOpenVoiceConn) SetOpusFrameReceiver(voice.OpusFrameReceiver)           {}
func (*failedOpenVoiceConn) SetEventHandlerFunc(voice.EventHandlerFunc)             {}
func (*failedOpenVoiceConn) Open(context.Context, snowflake.ID, bool, bool) error {
	return errors.New("offline open failure")
}
func (c *failedOpenVoiceConn) Close(context.Context) {
	c.closed.Add(1)
	if c.onClose != nil {
		c.onClose()
	}
}
func (*failedOpenVoiceConn) HandleVoiceStateUpdate(gateway.EventVoiceStateUpdate)   {}
func (*failedOpenVoiceConn) HandleVoiceServerUpdate(gateway.EventVoiceServerUpdate) {}

func TestDisgoVoiceConnectionFailedOpenClosesExactObjectWithoutRemovingReplacement(t *testing.T) {
	replacement := &failedOpenVoiceConn{}
	var current voice.Conn
	conn := &failedOpenVoiceConn{onClose: func() { current = replacement }}
	current = conn
	vc := &DisgoVoiceConnection{
		guildID:    1,
		createConn: func() voice.Conn { return conn },
	}

	err := vc.Open(context.Background(), "2")
	require.ErrorContains(t, err, "failed to open voice connection")
	require.Equal(t, int32(1), conn.closed.Load())
	require.Same(t, replacement, current, "cleanup after exact Close must not remove a replacement")
	require.False(t, vc.IsReady())
}

func TestDisgoVoiceConnectionCloseIsIdempotentAndPreservesReplacement(t *testing.T) {
	replacement := &failedOpenVoiceConn{}
	var current voice.Conn
	conn := &failedOpenVoiceConn{onClose: func() { current = replacement }}
	current = conn
	vc := &DisgoVoiceConnection{guildID: 1, conn: conn, ready: true}

	require.NoError(t, vc.Close(context.Background()))
	require.NoError(t, vc.Close(context.Background()))
	require.Equal(t, int32(1), conn.closed.Load())
	require.Same(t, replacement, current, "normal Close must not perform manager-wide replacement removal")
	require.False(t, vc.IsReady())
}

var _ voice.Conn = (*failedOpenVoiceConn)(nil)
