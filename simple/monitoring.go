package simple

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Owloops/updo/config"
	"github.com/Owloops/updo/coordinator"
	"github.com/Owloops/updo/metrics"
	"github.com/Owloops/updo/net"
	"github.com/Owloops/updo/notifications"
	"github.com/Owloops/updo/stats"
	"github.com/Owloops/updo/utils"
)

const (
	_shutdownGrace = 5 * time.Second
)

const (
	requestFailedMsg   = "Request failed"
	assertionFailedMsg = "Assertion failed"
)

func getErrorMessage(result net.WebsiteCheckResult) string {
	if result.IsUp {
		return ""
	}
	switch {
	case result.StatusCode > 0:
		return fmt.Sprintf("Non-success status code: %d", result.StatusCode)
	case result.AssertText != "" && !result.AssertionPassed:
		return assertionFailedMsg
	default:
		return requestFailedMsg
	}
}

type TargetResult struct {
	Target   config.Target
	Result   net.WebsiteCheckResult
	Stats    stats.Stats
	Sequence int
	Region   string
}

type MonitoringOptions struct {
	Count         int
	Log           string
	Regions       []string
	Profile       string
	PrometheusURL string
}

func StartMultiTargetMonitoring(targets []config.Target, options MonitoringOptions) {
	if len(targets) == 0 {
		log.Fatal("No targets provided")
	}

	keyRegistry := stats.NewTargetKeyRegistry(targets, options.Regions)
	allKeys := keyRegistry.GetAllKeys()

	monitors := make(map[string]*stats.Monitor, len(allKeys))
	sequences := make(map[string]*int, len(allKeys))
	alertStates := make(map[string]*bool, len(allKeys))
	webhookAlertStates := make(map[string]*bool, len(allKeys))

	for _, key := range allKeys {
		monitor, err := stats.NewMonitor()
		if err != nil {
			log.Fatalf("Failed to initialize stats monitor for %s: %v", key.String(), err)
		}
		keyStr := key.String()
		monitors[keyStr] = monitor
		var seq int
		var alert bool
		var webhookAlert bool
		sequences[keyStr] = &seq
		alertStates[keyStr] = &alert
		webhookAlertStates[keyStr] = &webhookAlert
	}

	// Root context: cancelled by SIGINT/SIGTERM (or explicit cancel) and
	// threaded through local HTTP, AWS config loading and Lambda Invoke.
	ctx, stopSignals := coordinator.SignalContext(context.Background())
	defer stopSignals()

	prometheusURL := options.PrometheusURL
	if prometheusURL == "" {
		if updoURL := os.Getenv("UPDO_PROMETHEUS_RW_SERVER_URL"); updoURL != "" {
			prometheusURL = updoURL
		}
	}

	if prometheusURL != "" {
		metricsConfig := metrics.NewConfig()
		metricsConfig.ServerURL = prometheusURL

		if username := os.Getenv("UPDO_PROMETHEUS_USERNAME"); username != "" {
			metricsConfig.Username = username
		}
		if password := os.Getenv("UPDO_PROMETHEUS_PASSWORD"); password != "" {
			metricsConfig.Password = password
		}
		if bearerToken := os.Getenv("UPDO_PROMETHEUS_BEARER_TOKEN"); bearerToken != "" {
			metricsConfig.Headers["Authorization"] = "Bearer " + bearerToken
		}
		if authHeader := os.Getenv("UPDO_PROMETHEUS_AUTH_HEADER"); authHeader != "" {
			parts := strings.SplitN(authHeader, ":", 2)
			if len(parts) == 2 {
				metricsConfig.Headers[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
			}
		}
		if pushInterval := os.Getenv("UPDO_PROMETHEUS_PUSH_INTERVAL"); pushInterval != "" {
			if duration, err := time.ParseDuration(pushInterval); err == nil {
				metricsConfig.PushInterval = duration
			}
		}

		metrics.InitRemoteWrite(metricsConfig)
		defer metrics.StopRemoteWrite()
	}

	runCoordinator := coordinator.New(coordinator.Config{
		Targets:       targets,
		Count:         options.Count,
		Regions:       options.Regions,
		Profile:       options.Profile,
		ShutdownGrace: _shutdownGrace,
	})
	runCoordinator.Start(ctx)

	logMode := options.Log != ""

	outputManager := NewOutputManager(targets)
	if !logMode {
		outputManager.PrintHeader()
	}

	// Side effects for one committed region outcome.
	committer := coordinator.NewRoundCommitter(func(targetIdx, roundID int, events []coordinator.Event) {
		for _, event := range events {
			processCommittedEvent(event, targets, monitors, sequences, alertStates, webhookAlertStates, options, outputManager, logMode)
		}
	})

	// Range ends only after the coordinator closes the stream: normal
	// completion (all per-target rounds committed) or the bounded shutdown
	// after a signal, with committed events drained.
	for event := range runCoordinator.Events() {
		committer.Handle(event)
	}

	outputManager.PrintFinalStatisticsWithKeys(monitors, keyRegistry, logMode)
}

func processCommittedEvent(
	event coordinator.Event,
	targets []config.Target,
	monitors map[string]*stats.Monitor,
	sequences map[string]*int,
	alertStates map[string]*bool,
	webhookAlertStates map[string]*bool,
	options MonitoringOptions,
	outputManager *OutputManager,
	logMode bool,
) {
	target := targets[event.TargetIndex]
	targetKey := eventTargetKey(target, event)

	monitor, exists := monitors[targetKey.String()]
	if !exists {
		return
	}

	if event.Result.ResponseTruncated {
		log.Printf("Warning: response body from %s truncated at BodySizeLimit of %d bytes",
			target.URL, config.Int64Val(target.BodySizeLimit, net.DefaultBodySizeLimit))
	}

	// Every terminal outcome (success, probe failure and executor failure)
	// is fed to stats, so executor failures stay in the availability
	// denominator.
	monitor.AddResult(event.Result)

	sequence := sequences[targetKey.String()]
	*sequence++
	seq := *sequence

	if event.Outcome == coordinator.ExecutorFailure {
		utils.LogWarning(target.URL, fmt.Sprintf("Lambda invocation failed: %v", event.Err), event.Region)
	}

	if event.Outcome != coordinator.ExecutorFailure {
		if config.BoolVal(target.ReceiveAlert, false) {
			if alertSent, exists := alertStates[targetKey.String()]; exists {
				if err := notifications.HandleAlerts(event.Result.IsUp, alertSent, target.Name, event.Result.URL); err != nil {
					log.Printf("Alert notification failed: %v", err)
				}
			}
		}

		if target.WebhookURL != "" {
			errorMsg := getErrorMessage(event.Result)
			if webhookAlertSent, exists := webhookAlertStates[targetKey.String()]; exists {
				if err := notifications.HandleWebhookAlert(
					target.WebhookURL,
					target.WebhookHeaders,
					event.Result.IsUp,
					webhookAlertSent,
					target.Name,
					event.Result.URL,
					event.Result.ResponseTime,
					event.Result.StatusCode,
					errorMsg,
				); err != nil {
					log.Printf("[ERROR] %v", err)
				}
			}
		}
	}

	targetResult := TargetResult{
		Target:   target,
		Result:   event.Result,
		Stats:    monitor.GetStats(),
		Sequence: seq,
		Region:   event.Region,
	}

	if !logMode {
		outputManager.PrintResult(targetResult)
	} else {
		utils.LogCheck(event.Result, seq, options.Log, event.Region)
		if !event.Result.IsUp {
			utils.LogWarning(target.URL, getErrorMessage(event.Result), event.Region)
		}
	}

	if options.PrometheusURL != "" {
		metrics.RecordCheck(target, event.Result, event.Region)

		if strings.HasPrefix(target.URL, "https://") {
			if sslExpiry := net.GetSSLCertExpiry(target.URL); sslExpiry >= 0 {
				metrics.RecordSSLExpiry(target, sslExpiry)
			}
		}
	}
}

func eventTargetKey(target config.Target, event coordinator.Event) stats.TargetKey {
	indexedName := fmt.Sprintf("%s#%d", target.Name, event.TargetIndex)

	if event.Region != "" {
		return stats.NewRegionTargetKey(indexedName, event.Region, event.TargetIndex)
	}

	return stats.NewLocalTargetKey(indexedName, event.TargetIndex)
}
