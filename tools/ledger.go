package tools

import "sync"

// ledgerCap bounds the ids a ledger holds; past it new ones are not recorded,
// and those messages are acked with sock_ack instead of by a reply.
const ledgerCap = 10_000

// Ledger is the deliveries this session showed the model (pushed on the
// channel or returned by sock_fetch) and has not acked, by conversation. A
// reply in a conversation acks what it holds for that conversation.
type Ledger struct {
	// Forget, when set, is told of ids once they are acked.
	Forget func(ids ...string)

	mu   sync.Mutex
	conv map[string]map[string]struct{}
	of   map[string]string // message id to its conversation
}

// Record notes that the model was shown message id in conversation conv.
func (l *Ledger) Record(conv, id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conv == nil {
		l.conv, l.of = map[string]map[string]struct{}{}, map[string]string{}
	}
	if _, ok := l.of[id]; ok || len(l.of) >= ledgerCap {
		return
	}
	ids := l.conv[conv]
	if ids == nil {
		ids = map[string]struct{}{}
		l.conv[conv] = ids
	}
	ids[id] = struct{}{}
	l.of[id] = conv
}

// shown is what the model was shown in conv so far.
func (l *Ledger) shown(conv string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, 0, len(l.conv[conv]))
	for id := range l.conv[conv] {
		out = append(out, id)
	}
	return out
}

// acked drops ids, wherever they were recorded, and tells Forget.
func (l *Ledger) acked(ids []string) {
	l.mu.Lock()
	for _, id := range ids {
		conv, ok := l.of[id]
		if !ok {
			continue
		}
		delete(l.of, id)
		delete(l.conv[conv], id)
		if len(l.conv[conv]) == 0 {
			delete(l.conv, conv)
		}
	}
	l.mu.Unlock()
	if l.Forget != nil {
		l.Forget(ids...)
	}
}
