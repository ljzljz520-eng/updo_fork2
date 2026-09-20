package coordinator_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Owloops/updo/config"
	"github.com/Owloops/updo/coordinator"
	"github.com/Owloops/updo/net"
)

const testURL = "https://example.com"

var testRegions = []string{"us-east-1", "us-west-1", "eu-west-1"}

func makeTargets(count int, refresh time.Duration) []config.Target {
	targets := make([]config.Target, count)
	for i := range targets {
		targets[i] = config.Target{
			Name:            fmt.Sprintf("Target-%d", i+1),
			URL:             fmt.Sprintf("https://target-%d.example.com", i+1),
			RefreshInterval: int(refresh.Seconds()),
		}
	}
	return targets
}

// fakeProbe is an instrumented ProbeFunc.
type fakeProbe struct {
	mu sync.Mutex

	delays       map[int]time.Duration
	execFailures map[string]bool

	inflight map[string]bool
	overlap  map[string]bool

	activeProbes  int
	maxConcurrent int

	cancelsObserved int
	started         chan string
}

func newFakeProbe() *fakeProbe {
	return &fakeProbe{
		delays:       make(map[int]time.Duration),
		execFailures: make(map[string]bool),
		inflight:     make(map[string]bool),
		overlap:      make(map[string]bool),
	}
}

func (f *fakeProbe) withStarted(ch chan string) *fakeProbe {
	f.started = ch
	return f
}

func (f *fakeProbe) setDelay(targetIdx int, delay time.Duration) {
	f.delays[targetIdx] = delay
}

func (f *fakeProbe) failRegion(targetIdx int, region string) {
	f.execFailures[fmt.Sprintf("%d:%s", targetIdx, region)] = true
}

func (f *fakeProbe) probe(ctx context.Context, batch coordinator.ProbeBatch, region string) coordinator.RegionOutcome {
	key := fmt.Sprintf("%d:%s", batch.TargetIndex, region)

	f.mu.Lock()
	if f.inflight[key] {
		f.overlap[key] = true
	}
	f.inflight[key] = true
	f.activeProbes++
	if f.activeProbes > f.maxConcurrent {
		f.maxConcurrent = f.activeProbes
	}
	fail := f.execFailures[key]
	if f.started != nil && batch.TargetIndex == 1 {
		select {
		case f.started <- key:
		default:
		}
	}
	f.mu.Unlock()

	defer func() {
		f.mu.Lock()
		f.inflight[key] = false
		f.activeProbes--
		f.mu.Unlock()
	}()

	delay := f.delays[batch.TargetIndex]
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-ctx.Done():
		f.mu.Lock()
		f.cancelsObserved++
		f.mu.Unlock()
		return f.cancelledOutcome(batch, region)
	}

	url := fmt.Sprintf("https://target-%d.example.com", batch.TargetIndex)

	if fail {
		return coordinator.RegionOutcome{
			Region:  region,
			Outcome: coordinator.ExecutorFailure,
			Result:  net.WebsiteCheckResult{URL: url, IsUp: false, LastCheckTime: time.Now()},
			Err:     errors.New("lambda invocation failed"),
		}
	}

	return coordinator.RegionOutcome{
		Region:  region,
		Outcome: coordinator.Success,
		Result: net.WebsiteCheckResult{
			URL:           url,
			IsUp:          true,
			StatusCode:    httpStatusOK,
			ResponseTime:  5 * time.Millisecond,
			LastCheckTime: time.Now(),
		},
	}
}

func (f *fakeProbe) cancelledOutcome(batch coordinator.ProbeBatch, region string) coordinator.RegionOutcome {
	url := fmt.Sprintf("https://target-%d.example.com", batch.TargetIndex)
	if region == "" {
		return coordinator.RegionOutcome{
			Region:  region,
			Outcome: coordinator.ProbeFailure,
			Result:  net.WebsiteCheckResult{URL: url, IsUp: false, LastCheckTime: time.Now()},
		}
	}
	return coordinator.RegionOutcome{
		Region:  region,
		Outcome: coordinator.ExecutorFailure,
		Result:  net.WebsiteCheckResult{URL: url, IsUp: false, LastCheckTime: time.Now()},
		Err:     context.Canceled,
	}
}

func (f *fakeProbe) assertNoOverlap(t *testing.T) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for key := range f.overlap {
		t.Errorf("overlapping probes detected for %s", key)
	}
}

const httpStatusOK = 200

type collectedStats struct {
	resultCounts map[string]int
	resultOrder  []string
	commits      map[int]int
	totalResults int
	// resultsSeenInRound tracks how many region results were observed before
	// each commit.
	roundResults map[string]int
}

func newCollectedStats() *collectedStats {
	return &collectedStats{
		resultCounts: make(map[string]int),
		commits:      make(map[int]int),
		roundResults: make(map[string]int),
	}
}

func (s *collectedStats) collect(event coordinator.Event) {
	switch event.Type {
	case coordinator.RoundResult:
		key := fmt.Sprintf("%d:%s", event.TargetIndex, event.Region)
		s.resultCounts[key]++
		s.resultOrder = append(s.resultOrder, key)
		s.totalResults++
		roundKey := fmt.Sprintf("%d:%d", event.TargetIndex, event.RoundID)
		s.roundResults[roundKey]++
	case coordinator.RoundCommitted:
		s.commits[event.TargetIndex]++
	}
}

// TestCountRoundsPerTarget verifies acceptance item 1: 2 targets x 3 regions,
// --count 4 yields exactly four terminal results per target/region, the stream
// closes only after 24 results were committed, and RoundCommitter applies the
// same counting rule.
func TestCountRoundsPerTarget(t *testing.T) {
	probe := newFakeProbe()

	c := coordinator.New(coordinator.Config{
		Targets:       makeTargets(2, 1),
		Regions:       testRegions,
		Count:         4,
		ShutdownGrace: time.Second,
		Probe:         probe.probe,
	})
	c.Start(context.Background())

	collected := newCollectedStats()

	// Results handed to the commit callback are exactly the results a UI
	// would count.
	committedResults := 0
	commits := 0
	committedKeys := make(map[string]int)
	committer := coordinator.NewRoundCommitter(func(targetIdx, roundID int, events []coordinator.Event) {
		commits++
		for _, event := range events {
			committedResults++
			committedKeys[fmt.Sprintf("%d:%s", targetIdx, event.Region)]++
		}
	})

	for event := range c.Events() {
		collected.collect(event)
		committer.Handle(event)
	}

	if collected.totalResults != 24 {
		t.Errorf("total terminal results = %d, want 24", collected.totalResults)
	}

	for targetIdx := 0; targetIdx < 2; targetIdx++ {
		if collected.commits[targetIdx] != 4 {
			t.Errorf("target %d commits = %d, want 4", targetIdx, collected.commits[targetIdx])
		}
		for _, region := range testRegions {
			key := fmt.Sprintf("%d:%s", targetIdx, region)
			if collected.resultCounts[key] != 4 {
				t.Errorf("results for %s = %d, want 4", key, collected.resultCounts[key])
			}
		}
	}

	for round, count := range collected.roundResults {
		if count != len(testRegions) {
			t.Errorf("round %s committed with %d results, want %d",
				round, count, len(testRegions))
		}
	}

	if committedResults != 24 {
		t.Errorf("RoundCommitter delivered %d results, want 24", committedResults)
	}
	if commits != 8 {
		t.Errorf("RoundCommitter callbacks = %d, want 8", commits)
	}
	for targetIdx := 0; targetIdx < 2; targetIdx++ {
		for _, region := range testRegions {
			key := fmt.Sprintf("%d:%s", targetIdx, region)
			if committedKeys[key] != 4 {
				t.Errorf("committed results for %s = %d, want 4",
					key, committedKeys[key])
			}
		}
	}

	probe.assertNoOverlap(t)
}

// TestExecutorFailureCounts verifies acceptance item 2: a Lambda Invoke
// failure is a terminal executor-failure result counted in the availability
// denominator, while other regions keep being consumed and rounds never
// stall.
func TestExecutorFailureCounts(t *testing.T) {
	probe := newFakeProbe()
	probe.failRegion(0, "us-west-1")

	c := coordinator.New(coordinator.Config{
		Targets:       makeTargets(2, 1),
		Regions:       testRegions,
		Count:         4,
		ShutdownGrace: time.Second,
		Probe:         probe.probe,
	})
	c.Start(context.Background())

	collected := newCollectedStats()

	// Emulate the availability accounting the frontends do.
	checks := make(map[int]int)
	successes := make(map[int]int)
	var failedOutcomes []coordinator.Event
	committer := coordinator.NewRoundCommitter(func(targetIdx, roundID int, events []coordinator.Event) {
		for _, event := range events {
			checks[targetIdx]++
			if event.Result.IsUp {
				successes[targetIdx]++
			}
			if event.Outcome == coordinator.ExecutorFailure {
				failedOutcomes = append(failedOutcomes, event)
			}
		}
	})

	for event := range c.Events() {
		collected.collect(event)
		committer.Handle(event)
	}

	if collected.totalResults != 24 {
		t.Fatalf("total results = %d, want 24 (failed region must not stall rounds)",
			collected.totalResults)
	}

	failedKey := "0:us-west-1"
	if collected.resultCounts[failedKey] != 4 {
		t.Errorf("failed region results = %d, want 4", collected.resultCounts[failedKey])
	}

	if len(failedOutcomes) != 4 {
		t.Errorf("executor failure outcomes = %d, want 4", len(failedOutcomes))
	}
	for _, event := range failedOutcomes {
		if event.Err == nil {
			t.Error("executor failure outcome must carry Err")
		}
		if event.Result.IsUp {
			t.Error("executor failure result must report down")
		}
	}

	// Denominator: target 0 had 12 region checks, only 8 successful.
	if checks[0] != 12 {
		t.Errorf("target 0 checks (denominator) = %d, want 12", checks[0])
	}
	if successes[0] != 8 {
		t.Errorf("target 0 successes = %d, want 8", successes[0])
	}
	if checks[1] != 12 || successes[1] != 12 {
		t.Errorf("target 1 checks/successes = %d/%d, want 12/12",
			checks[1], successes[1])
	}
}

// TestCancelDuringSlowProbe verifies acceptance item 3: after the root
// context is cancelled while probes are in flight, the stream closes within
// the bounded grace, already committed results are delivered, probes observe
// cancellation, and no coordinator goroutine is leaked.
func TestCancelDuringSlowProbe(t *testing.T) {
	probe := newFakeProbe()
	probe.setDelay(0, 10*time.Millisecond) // fast target commits several rounds
	probe.setDelay(1, 2*time.Second)       // slow target, interrupted

	started := make(chan string, 6)
	probe.withStarted(started)

	ctx, cancel := context.WithCancel(context.Background())

	c := coordinator.New(coordinator.Config{
		Targets:       makeTargets(2, 1),
		Regions:       testRegions,
		Count:         0,
		ShutdownGrace: time.Second,
		Probe:         probe.probe,
	})

	goroutinesBefore := runtime.NumGoroutine()
	c.Start(ctx)

	// Wait until the slow target's probes are in flight.
	seen := make(map[string]bool)
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for len(seen) < len(testRegions) {
		select {
		case key := <-started:
			seen[key] = true
		case <-deadline.C:
			t.Fatalf("slow probes did not start, saw %v", seen)
		}
	}

	// Let the fast target commit a couple of rounds (10ms probe + small reset).
	time.Sleep(40 * time.Millisecond)

	cancel() // simulates Ctrl+C cancelling the root context

	closeStart := time.Now()
	committedRounds := make(map[int]int)
	drainTimer := time.NewTimer(2 * time.Second)
	defer drainTimer.Stop()
drainLoop:
	for {
		select {
		case event, ok := <-c.Events():
			if !ok {
				break drainLoop
			}
			if event.Type == coordinator.RoundCommitted {
				committedRounds[event.TargetIndex]++
			}
		case <-drainTimer.C:
			t.Fatal("event stream did not close within bounded shutdown")
		}
	}

	closeElapsed := time.Since(closeStart)
	if closeElapsed > 500*time.Millisecond {
		t.Errorf("stream closed after %v, want <= 500ms", closeElapsed)
	}

	if probe.cancelsObserved == 0 {
		t.Error("in-flight probes did not observe root context cancellation")
	}

	if committedRounds[0] < 2 {
		t.Errorf("fast target committed rounds delivered = %d, want >= 2",
			committedRounds[0])
	}

	// No goroutine leak: wait briefly for all coordinator goroutines to exit.
	leakDeadline := time.Now().Add(time.Second)
	for time.Now().Before(leakDeadline) {
		if runtime.NumGoroutine() <= goroutinesBefore+1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("goroutine leak: before=%d after=%d",
		goroutinesBefore, runtime.NumGoroutine())
}

// TestIndependentSchedules verifies acceptance item 4: different refresh
// intervals never overlap probes for the same target, a slow target does not
// block other targets, and region-level parallelism (default concurrency) is
// preserved.
func TestIndependentSchedules(t *testing.T) {
	probe := newFakeProbe()
	probe.setDelay(0, 60*time.Millisecond) // slow target
	probe.setDelay(1, time.Millisecond)    // fast target

	targets := makeTargets(2, 1)
	// Config granularity is whole seconds; zero is the smallest valid interval.
	targets[0].RefreshInterval = 0
	targets[1].RefreshInterval = 0

	c := coordinator.New(coordinator.Config{
		Targets:       targets,
		Regions:       testRegions,
		Count:         3,
		ShutdownGrace: time.Second,
		Probe:         probe.probe,
	})
	c.Start(context.Background())

	collected := newCollectedStats()
	commitTimes := make(map[int][]time.Time)
	committer := coordinator.NewRoundCommitter(func(targetIdx, roundID int, events []coordinator.Event) {
		commitTimes[targetIdx] = append(commitTimes[targetIdx], time.Now())
	})

	for event := range c.Events() {
		collected.collect(event)
		committer.Handle(event)
	}

	probe.assertNoOverlap(t)

	for targetIdx := 0; targetIdx < 2; targetIdx++ {
		if collected.commits[targetIdx] != 3 {
			t.Errorf("target %d commits = %d, want 3",
				targetIdx, collected.commits[targetIdx])
		}
	}

	if len(commitTimes[0]) != 3 || len(commitTimes[1]) != 3 {
		t.Fatalf("commit counts slow=%d fast=%d",
			len(commitTimes[0]), len(commitTimes[1]))
	}

	// The fast target must finish before the slow target even completes.
	fastDone := commitTimes[1][2]
	slowDone := commitTimes[0][2]
	if !fastDone.Before(slowDone) {
		t.Error("fast target was blocked by slow target")
	}
	if slowDone.Sub(fastDone) < 50*time.Millisecond {
		t.Error("scheduling difference too small to prove independence")
	}

	// Region probes within a round still run concurrently.
	probe.mu.Lock()
	maxConcurrent := probe.maxConcurrent
	probe.mu.Unlock()
	if maxConcurrent < len(testRegions) {
		t.Errorf("max concurrent region probes = %d, want >= %d",
			maxConcurrent, len(testRegions))
	}
}

// TestRoundCommitter verifies the shared transactional counting rule.
func TestRoundCommitter(t *testing.T) {
	var calls [][]coordinator.Event
	committer := coordinator.NewRoundCommitter(func(targetIdx, roundID int, events []coordinator.Event) {
		calls = append(calls, events)
	})

	resultEvent := func(targetIdx, roundID int, region string) coordinator.Event {
		return coordinator.Event{
			Type:       coordinator.RoundResult,
			ProbeBatch: coordinator.ProbeBatch{TargetIndex: targetIdx, RoundID: roundID},
			Region:     region,
		}
	}
	commitEvent := func(targetIdx, roundID int) coordinator.Event {
		return coordinator.Event{
			Type:       coordinator.RoundCommitted,
			ProbeBatch: coordinator.ProbeBatch{TargetIndex: targetIdx, RoundID: roundID},
		}
	}

	// target 0 round 0: full round commits.
	committer.Handle(resultEvent(0, 0, "us-east-1"))
	committer.Handle(resultEvent(0, 0, "us-west-1"))
	if len(calls) != 0 {
		t.Fatal("callback fired before RoundCommitted")
	}
	committer.Handle(commitEvent(0, 0))
	if len(calls) != 1 || len(calls[0]) != 2 {
		t.Fatalf("commit delivered %v, want one round with 2 results", calls)
	}
	if calls[0][0].Region != "us-east-1" || calls[0][1].Region != "us-west-1" {
		t.Error("commit did not preserve result order")
	}

	// target 0 round 1 partial; interleaved target 1 round 0 commits
	// independently.
	committer.Handle(resultEvent(0, 1, "eu-west-1"))
	committer.Handle(resultEvent(1, 0, "us-east-1"))
	committer.Handle(commitEvent(1, 0))
	if len(calls) != 2 || len(calls[1]) != 1 {
		t.Fatalf("interleaved commit delivered %v", calls)
	}

	// Late commit of target 0 round 1 flushes the buffered partial round.
	committer.Handle(commitEvent(0, 1))
	if len(calls) != 3 || len(calls[2]) != 1 {
		t.Fatalf("late commit delivered %v", calls)
	}
}

// TestSignalContext verifies SIGINT cancels the shared root context.
func TestSignalContext(t *testing.T) {
	ctx, cancel := coordinator.SignalContext(context.Background())
	defer cancel()

	if err := syscall.Kill(syscall.Getpid(), syscall.SIGINT); err != nil {
		t.Fatalf("failed to send SIGINT: %v", err)
	}

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("root context was not cancelled after SIGINT")
	}
}

// TestStartWithCancelledContext verifies shutdown works when the context was
// already cancelled before Start.
func TestStartWithCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c := coordinator.New(coordinator.Config{
		Targets:       makeTargets(1, 1),
		Regions:       testRegions,
		ShutdownGrace: 100 * time.Millisecond,
		Probe:         newFakeProbe().probe,
	})
	c.Start(ctx)

	select {
	case _, ok := <-c.Events():
		if ok {
			t.Error("expected closed stream with pre-cancelled context")
		}
	case <-time.After(time.Second):
		t.Fatal("stream did not close for pre-cancelled context")
	}
}
