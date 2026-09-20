package coordinator

type roundKey struct {
	targetIndex int
	roundID     int
}

// CommitFunc is invoked once all RoundResult events of a round and the
// RoundCommitted marker have been received. results preserves publish order.
type CommitFunc func(targetIndex, roundID int, results []Event)

// RoundCommitter buffers per-round region results and hands them to the
// callback only after the matching RoundCommitted event. Both frontends use
// it, so simple and TUI apply identical "rounds completed" semantics.
type RoundCommitter struct {
	onCommit CommitFunc
	pending  map[roundKey][]Event
}

// NewRoundCommitter creates a RoundCommitter.
func NewRoundCommitter(onCommit CommitFunc) *RoundCommitter {
	return &RoundCommitter{
		onCommit: onCommit,
		pending:  make(map[roundKey][]Event),
	}
}

// Handle processes one event. It is not safe for concurrent use; drive it
// from a single goroutine.
func (r *RoundCommitter) Handle(event Event) {
	key := roundKey{
		targetIndex: event.TargetIndex,
		roundID:     event.RoundID,
	}

	switch event.Type {
	case RoundResult:
		r.pending[key] = append(r.pending[key], event)
	case RoundCommitted:
		results := r.pending[key]
		delete(r.pending, key)
		r.onCommit(event.TargetIndex, event.RoundID, results)
	}
}
