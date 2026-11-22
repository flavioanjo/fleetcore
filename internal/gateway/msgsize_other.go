//go:build !windows

package gateway

// isMessageTooLarge is always false here: other platforms truncate an
// oversize datagram to the buffer, which the read loop detects by length.
func isMessageTooLarge(error) bool { return false }
