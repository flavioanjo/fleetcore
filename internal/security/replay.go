package security

// WindowSize is the number of counters a ReplayWindow remembers below the
// highest accepted one.
const WindowSize = 1024

// ReplayWindow is a sliding anti-replay window over a per-session message
// counter (the IPsec/DTLS construction, RFC 4303 §3.4.3): it accepts each
// counter at most once, accepts counters up to WindowSize-1 below the
// highest seen (so reordered datagrams are not rejected), and rejects
// anything older. Counter 0 is never valid.
//
// The zero value is an empty window. It is not safe for concurrent use.
type ReplayWindow struct {
	top  uint64
	bits [WindowSize / 64]uint64
}

// Check reports whether ctr would be accepted, without recording it. Use
// Check before verifying a MAC and Accept only after, so forged packets
// cannot advance the window.
func (w *ReplayWindow) Check(ctr uint64) bool {
	switch {
	case ctr == 0:
		return false
	case ctr > w.top:
		return true
	case w.top-ctr >= WindowSize:
		return false
	}
	return !w.bit(ctr)
}

// Accept records ctr and reports whether it was fresh.
func (w *ReplayWindow) Accept(ctr uint64) bool {
	if !w.Check(ctr) {
		return false
	}
	if ctr > w.top {
		shift := ctr - w.top
		if shift >= WindowSize {
			w.bits = [WindowSize / 64]uint64{}
		} else {
			// Iterate over the bounded offset, never over the counter
			// itself: top+i <= ctr cannot overflow, while a loop on
			// "c <= ctr" never ends when ctr is math.MaxUint64.
			for i := uint64(1); i <= shift; i++ {
				w.clear(w.top + i)
			}
		}
		w.top = ctr
	}
	w.set(ctr)
	return true
}

// Top is the highest accepted counter (0 if none).
func (w *ReplayWindow) Top() uint64 { return w.top }

func (w *ReplayWindow) idx(c uint64) (int, uint64) {
	i := c % WindowSize
	return int(i / 64), 1 << (i % 64)
}
func (w *ReplayWindow) bit(c uint64) bool { i, m := w.idx(c); return w.bits[i]&m != 0 }
func (w *ReplayWindow) set(c uint64)      { i, m := w.idx(c); w.bits[i] |= m }
func (w *ReplayWindow) clear(c uint64)    { i, m := w.idx(c); w.bits[i] &^= m }
