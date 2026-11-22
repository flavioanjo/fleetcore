package gateway

import (
	"errors"
	"syscall"
)

// isMessageTooLarge reports whether a UDP read failed because the datagram
// did not fit the buffer. Windows reports this as WSAEMSGSIZE instead of
// silently truncating.
func isMessageTooLarge(err error) bool {
	return errors.Is(err, syscall.Errno(10040)) // WSAEMSGSIZE
}
