package provider

import (
	"context"
)

// TranscriptEvent represents a partial or final transcription event sent back to LobeHub client.
type TranscriptEvent struct {
	SegmentID string
	Text      string
	IsFinal   bool
}

// ASRProvider defines the interface for streaming ASR backends (e.g. Volcengine, Qwen, FunASR).
type ASRProvider interface {
	// StartSession initializes the upstream streaming connection.
	StartSession(ctx context.Context, sessionID string) error

	// SendAudio sends a chunk of raw 16kHz 16-bit PCM mono audio.
	SendAudio(pcmChunk []byte) error

	// EndSession notifies upstream that audio stream is finished.
	EndSession() error

	// Close terminates the upstream connection immediately.
	Close() error

	// Events returns the channel of incoming transcription events.
	Events() <-chan TranscriptEvent

	// Errors returns the channel of fatal upstream errors.
	Errors() <-chan error
}
