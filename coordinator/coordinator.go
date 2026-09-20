// Package coordinator contains the run coordination logic shared by the
// simple (text) and TUI frontends.
//
// A run is scheduled per target as a sequence of ProbeBatch values, each
// carrying a RoundID and covering every configured region (or one local
// probe). Every region always reaches exactly one terminal Outcome
// (Success, ProbeFailure or ExecutorFailure); a round is committed only
// after all its region outcomes are received. The coordinator publishes a
// single event stream to both frontends and performs an orderly, bounded
// shutdown on normal completion or context cancellation.
package coordinator

import (
	"context"
	"sync"
	"time"

	"github.com/Owloops/updo/aws"
	"github.com/Owloops/updo/config"
	"github.com/Owloops/updo/net"
)

const (
	// defaultShutdownGrace bounds how long the coordinator waits for
	// in-flight batches after the run context is cancelled (signal exit).
	defaultShutdownGrace = 5 * time.Second

	// channelBufferMultiplier is applied to the total number of
	// target/region keys when sizing internal buffers.
	channelBufferMultiplier = 2
)

// Outcome is the terminal state of probing one region in one round.
type Outcome uint8

const (
	// Success means the probe executed and the target satisfied the check.
	Success Outcome = iota
	// ProbeFailure means the probe executed but the target failed the check
	// (non-success status code, assertion failure or request error).
	ProbeFailure
	// ExecutorFailure means the probe could not be executed (for example the
	// Lambda Invoke call failed). It still yields a terminal result so that a
	// round can never be left with a missing region.
	ExecutorFailure
)

// ProbeBatch is one scheduled round for a single target: every configured
// region (or one local probe when Regions is empty) shares the same RoundID.
type ProbeBatch struct {
	TargetIndex int
	RoundID     int
	Regions     []string
}

// RegionOutcome is the terminal result for one region within a ProbeBatch.
type RegionOutcome struct {
	Region  string
	Outcome Outcome
	// Result is always populated, including a synthetic down result for
	// ExecutorFailure, so consumers can feed outcomes to stats uniformly.
	Result net.WebsiteCheckResult
	Err    error
}

// ProbeFunc executes a single region probe (region == "" for local probes).
// Implementations must return promptly after ctx is cancelled.
type ProbeFunc func(ctx context.Context, batch ProbeBatch, region string) RegionOutcome

// EventType discriminates events published on the coordinator stream.
type EventType uint8

const (
	// RoundResult is one region's terminal outcome within a round.
	RoundResult EventType = iota
	// RoundCommitted marks that every region outcome of the round has been
	// published. Only committed rounds count towards --count.
	RoundCommitted
)

// Event is an item in the shared event stream consumed by simple and TUI.
type Event struct {
	Type EventType
	ProbeBatch
	Region  string
	Outcome Outcome
	Result  net.WebsiteCheckResult
	Err     error
}

// Config configures a RunCoordinator.
type Config struct {
	Targets []config.Target
	// Regions is the global region list used when a target does not define
	// its own Regions.
	Regions []string
	Profile string
	// Count is the number of rounds completed per target. 0 means run until
	// the context is cancelled.
	Count int
	// ShutdownGrace bounds in-flight shutdown. Defaults to 5 seconds.
	ShutdownGrace time.Duration
	// Probe overrides the default (real) probe executor; tests use this.
	Probe ProbeFunc
}

// RunCoordinator schedules one ProbeBatch at a time per target, tracks the
// per-target round budget and in-flight batches, and publishes a single
// event stream.
type RunCoordinator struct {
	cfg Config

	probe ProbeFunc

	ctx    context.Context
	cancel context.CancelFunc

	// outbox receives committed events from schedulers; it is never closed.
	outbox chan Event
	// events is the public stream; only dispatch sends to/closes it.
	events chan Event

	dispStop chan struct{}

	schedWG sync.WaitGroup
	dispWG  sync.WaitGroup
}

// New creates a RunCoordinator.
func New(cfg Config) *RunCoordinator {
	if cfg.ShutdownGrace <= 0 {
		cfg.ShutdownGrace = defaultShutdownGrace
	}

	keyCount := totalKeys(cfg.Targets, cfg.Regions)

	c := &RunCoordinator{
		cfg:      cfg,
		probe:    cfg.Probe,
		outbox:   make(chan Event, keyCount*channelBufferMultiplier),
		events:   make(chan Event, keyCount*channelBufferMultiplier),
		dispStop: make(chan struct{}),
	}

	if c.probe == nil {
		c.probe = c.defaultProbe
	}

	return c
}

// Events returns the shared event stream. It is closed after the coordinator
// has stopped launching rounds, in-flight work has completed or exceeded the
// bounded shutdown grace, and committed events have been drained.
func (c *RunCoordinator) Events() <-chan Event {
	return c.events
}

// Start launches the per-target schedulers and the shutdown machinery. It
// must be called exactly once.
func (c *RunCoordinator) Start(ctx context.Context) {
	c.ctx, c.cancel = context.WithCancel(ctx)

	c.dispWG.Add(1)
	go c.dispatch()

	for i := range c.cfg.Targets {
		c.schedWG.Add(1)
		go c.runScheduler(i)
	}

	go c.shutdown()
}

func (c *RunCoordinator) regionsFor(targetIndex int) []string {
	target := c.cfg.Targets[targetIndex]
	if len(target.Regions) > 0 {
		return target.Regions
	}
	return c.cfg.Regions
}

// runScheduler owns one target's timeline. A batch runs synchronously, so a
// slow batch never overlaps the same target's next round; other targets have
// independent schedulers.
func (c *RunCoordinator) runScheduler(targetIndex int) {
	defer c.schedWG.Done()

	batch := ProbeBatch{
		TargetIndex: targetIndex,
		Regions:     c.regionsFor(targetIndex),
	}

	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		if c.ctx.Err() != nil {
			return
		}

		select {
		case <-c.ctx.Done():
			return
		case <-timer.C:
		}

		// The context may have ended while waiting for the timer.
		if c.ctx.Err() != nil {
			return
		}

		c.runBatch(batch)

		if c.cfg.Count > 0 && batch.RoundID+1 >= c.cfg.Count {
			return
		}

		batch.RoundID++
		timer.Reset(c.cfg.Targets[targetIndex].GetRefreshInterval())
	}
}

// runBatch probes all regions of a batch concurrently and commits the batch
// only after every region has returned a terminal outcome.
func (c *RunCoordinator) runBatch(batch ProbeBatch) {
	if len(batch.Regions) == 0 {
		c.commit(batch, []RegionOutcome{c.probe(c.ctx, batch, "")})
		return
	}

	outcomes := make([]RegionOutcome, len(batch.Regions))

	var wg sync.WaitGroup
	for i, region := range batch.Regions {
		wg.Add(1)
		go func(i int, region string) {
			defer wg.Done()
			outcomes[i] = c.probe(c.ctx, batch, region)
		}(i, region)
	}
	wg.Wait()

	c.commit(batch, outcomes)
}

// commit publishes the region outcomes of a batch followed by one
// RoundCommitted event. If the run context ends before the whole batch can
// be published, the partial batch is not committed.
func (c *RunCoordinator) commit(batch ProbeBatch, outcomes []RegionOutcome) {
	for _, outcome := range outcomes {
		event := Event{
			Type:       RoundResult,
			ProbeBatch: batch,
			Region:     outcome.Region,
			Outcome:    outcome.Outcome,
			Result:     outcome.Result,
			Err:        outcome.Err,
		}
		select {
		case c.outbox <- event:
		case <-c.ctx.Done():
			return
		}
	}

	select {
	case c.outbox <- Event{Type: RoundCommitted, ProbeBatch: batch}:
	case <-c.ctx.Done():
	}
}

// dispatch is the only goroutine that sends to, and closes, c.events, which
// makes sending on a closed channel impossible.
func (c *RunCoordinator) dispatch() {
	defer c.dispWG.Done()

	for {
		select {
		case <-c.dispStop:
			c.drainOutbox(nil)
			return
		case event := <-c.outbox:
			select {
			case c.events <- event:
			case <-c.dispStop:
				// Already holding an event that must not be dropped.
				c.drainOutbox(&event)
				return
			}
		}
	}
}

// drainOutbox forwards every already produced event during shutdown. Sends
// block until the consumer accepts them: consumers read the stream until it
// is closed, and the stream is closed only after dispatch returns, so no
// event is lost and no send can block forever. If first is non-nil, it is
// forwarded before draining the outbox.
func (c *RunCoordinator) drainOutbox(first *Event) {
	if first != nil {
		c.events <- *first
	}

	for {
		select {
		case event := <-c.outbox:
			c.events <- event
		default:
			return
		}
	}
}

// shutdown waits for schedulers to settle (bounded after cancellation) and
// then closes the event stream.
func (c *RunCoordinator) shutdown() {
	settled := make(chan struct{})
	go func() {
		c.schedWG.Wait()
		close(settled)
	}()

	select {
	case <-settled:
	case <-c.ctx.Done():
		select {
		case <-settled:
		case <-time.After(c.cfg.ShutdownGrace):
		}
	}

	close(c.dispStop)
	c.dispWG.Wait()
	close(c.events)
	c.cancel()
}

func totalKeys(targets []config.Target, globalRegions []string) int {
	total := 0

	for i := range targets {
		regions := targets[i].Regions
		if len(regions) == 0 {
			regions = globalRegions
		}
		if len(regions) == 0 {
			total++
		} else {
			total += len(regions)
		}
	}

	if total == 0 {
		return 1
	}
	return total
}

// defaultProbe wires the real local HTTP and AWS Lambda executors.
func (c *RunCoordinator) defaultProbe(ctx context.Context, batch ProbeBatch, region string) RegionOutcome {
	target := c.cfg.Targets[batch.TargetIndex]
	netConfig := networkConfig(target)

	if region == "" {
		result := net.CheckWebsite(ctx, target.URL, netConfig)
		return classify(region, result)
	}

	regionResult := aws.InvokeInRegion(ctx, target.URL, netConfig, region, c.cfg.Profile)
	if regionResult.Error != nil {
		return RegionOutcome{
			Region:  region,
			Outcome: ExecutorFailure,
			Result:  executorFailureResult(target.URL),
			Err:     regionResult.Error,
		}
	}

	return classify(region, regionResult.Result)
}

func classify(region string, result net.WebsiteCheckResult) RegionOutcome {
	outcome := ProbeFailure
	if result.IsUp {
		outcome = Success
	}

	return RegionOutcome{
		Region:  region,
		Outcome: outcome,
		Result:  result,
	}
}

func executorFailureResult(url string) net.WebsiteCheckResult {
	return net.WebsiteCheckResult{
		URL:           url,
		IsUp:          false,
		LastCheckTime: time.Now(),
	}
}

func networkConfig(target config.Target) net.NetworkConfig {
	return net.NetworkConfig{
		Timeout:         target.GetTimeout(),
		ShouldFail:      target.ShouldFail,
		FollowRedirects: config.BoolVal(target.FollowRedirects, false),
		AcceptRedirects: config.BoolVal(target.AcceptRedirects, false),
		SkipSSL:         config.BoolVal(target.SkipSSL, false),
		AssertText:      target.AssertText,
		Headers:         target.Headers,
		Method:          target.Method,
		Body:            target.Body,
		BodySizeLimit:   config.Int64Val(target.BodySizeLimit, net.DefaultBodySizeLimit),
	}
}
