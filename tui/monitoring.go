package tui

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Owloops/updo/config"
	"github.com/Owloops/updo/coordinator"
	"github.com/Owloops/updo/metrics"
	"github.com/Owloops/updo/net"
	"github.com/Owloops/updo/notifications"
	"github.com/Owloops/updo/stats"
	ui "github.com/gizak/termui/v3"
)

const _shutdownGrace = 5 * time.Second

type TargetData struct {
	Target       config.Target
	Result       net.WebsiteCheckResult
	Stats        stats.Stats
	TargetKey    stats.TargetKey
	WebhookError error
	LambdaError  error
	AlertError   error
}

type Options struct {
	Count         int
	Log           string
	Regions       []string
	Profile       string
	PrometheusURL string
}

func StartMonitoring(targets []config.Target, options Options) {
	if len(targets) == 0 {
		panic("No targets provided")
	}

	if err := ui.Init(); err != nil {
		panic(err)
	}
	defer ui.Close()

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

	keyRegistry := stats.NewTargetKeyRegistry(targets, options.Regions)
	allKeys := keyRegistry.GetAllKeys()

	monitors := make(map[string]*stats.Monitor, len(allKeys))
	sequences := make(map[string]*int, len(allKeys))
	alertStates := make(map[string]*bool, len(allKeys))
	webhookAlertStates := make(map[string]*bool, len(allKeys))

	for _, key := range allKeys {
		monitor, err := stats.NewMonitor()
		if err != nil {
			panic(fmt.Sprintf("Failed to initialize stats monitor for %s: %v", key.String(), err))
		}
		monitors[key.String()] = monitor
		seq := 0
		alert := false
		webhookAlert := false
		sequences[key.String()] = &seq
		alertStates[key.String()] = &alert
		webhookAlertStates[key.String()] = &webhookAlert
	}

	// Root context: cancelled by SIGINT/SIGTERM (or q/C-c below) and
	// threaded through local HTTP, AWS config loading and Lambda Invoke.
	ctx, cancel := coordinator.SignalContext(context.Background())
	defer cancel()

	runCoordinator := coordinator.New(coordinator.Config{
		Targets:       targets,
		Count:         options.Count,
		Regions:       options.Regions,
		Profile:       options.Profile,
		ShutdownGrace: _shutdownGrace,
	})
	runCoordinator.Start(ctx)

	dataChannel := make(chan TargetData, len(targets)*_dataChannelMultiplier)

	// Driver: reassembles committed rounds with the same RoundCommitter
	// used by simple mode and forwards one TargetData per region outcome.
	go func() {
		defer close(dataChannel)

		committer := coordinator.NewRoundCommitter(func(targetIdx, roundID int, events []coordinator.Event) {
			for _, event := range events {
				data := buildTUIEventData(event, targets, monitors, sequences, alertStates, webhookAlertStates, options)

				select {
				case dataChannel <- data:
				case <-ctx.Done():
					return
				}
			}
		})

		for event := range runCoordinator.Events() {
			if ctx.Err() != nil {
				return
			}
			committer.Handle(event)
		}
	}()

	manager := NewManager(targets, options)
	width, height := ui.TerminalDimensions()
	manager.InitializeLayout(width, height)

	uiRefreshTicker := time.NewTicker(1 * time.Second)
	defer uiRefreshTicker.Stop()

	uiEvents := ui.PollEvents()

	for {
		select {
		case e := <-uiEvents:
			switch e.ID {
			case "q":
				if manager.listWidget != nil && manager.listWidget.IsSearchMode() {
					manager.listWidget.UpdateSearch("q")
					ui.Render(manager.grid)
				} else {
					cancel()
					return
				}
			case "<C-c>":
				cancel()
				return
			case "<Resize>":
				if payload, ok := e.Payload.(ui.Resize); ok {
					width, height = payload.Width, payload.Height
					manager.Resize(width, height)
					ui.Render(manager.grid)
				}
			case "<Down>":
				if !manager.isSingle {
					if manager.IsFocusedOnLogs() {
						manager.NavigateLogs(1)
						ui.Render(manager.grid)
					} else {
						manager.NavigateTargetKeys(1, monitors)
					}
				}
			case "<Up>":
				if !manager.isSingle {
					if manager.IsFocusedOnLogs() {
						manager.NavigateLogs(-1)
						ui.Render(manager.grid)
					} else {
						manager.NavigateTargetKeys(-1, monitors)
					}
				}
			case "<Enter>":
				if manager.IsFocusedOnLogs() && manager.detailsManager.LogsWidget != nil {
					func() {
						defer func() {
							if r := recover(); r != nil {
								_ = r
							}
						}()
						manager.detailsManager.LogsWidget.ToggleExpand()
					}()
					ui.Render(manager.grid)
				} else if manager.listWidget != nil && manager.listWidget.IsHeaderAtIndex(manager.listWidget.SelectedRow) {
					groupID := manager.listWidget.GetGroupAtIndex(manager.listWidget.SelectedRow)
					if groupID != "" {
						manager.preserveHeaderSelection = groupID
						manager.listWidget.ToggleGroupCollapse(groupID)
						manager.updateTargetList()
						manager.preserveHeaderSelection = ""
						ui.Render(manager.grid)
					}
				}
			case "l":
				if manager.listWidget != nil && manager.listWidget.IsSearchMode() {
					manager.listWidget.UpdateSearch("l")
				} else {
					manager.ToggleLogsVisibility()
				}
				ui.Render(manager.grid)
			case "/":
				if len(targets) > 1 && manager.listWidget != nil {
					manager.listWidget.ToggleSearch()
					if manager.listWidget.IsSearchMode() && manager.listWidget.OnSearchChange != nil {
						indices := manager.listWidget.GetFilteredIndices()
						manager.listWidget.OnSearchChange(manager.listWidget.GetQuery(), indices)
					}
					ui.Render(manager.grid)
				}
			case "<Escape>":
				if manager.listWidget != nil && manager.listWidget.IsSearchMode() {
					manager.listWidget.ToggleSearch()
					if manager.listWidget.IsSearchMode() && manager.listWidget.OnSearchChange != nil {
						indices := manager.listWidget.GetFilteredIndices()
						manager.listWidget.OnSearchChange(manager.listWidget.GetQuery(), indices)
					}
					ui.Render(manager.grid)
				}
			case _backspaceKey, _ctrlBackspace, "<Space>":
				if manager.listWidget != nil && manager.listWidget.IsSearchMode() {
					manager.listWidget.UpdateSearch(e.ID)
					ui.Render(manager.grid)
				}
			case "<Tab>":
				if manager.listWidget != nil && !manager.listWidget.IsSearchMode() {
					manager.listWidget.ToggleAllGroups()
					manager.updateTargetList()
					ui.Render(manager.grid)
				}
			default:
				if manager.listWidget != nil && manager.listWidget.IsSearchMode() && len(e.ID) == 1 {
					manager.listWidget.UpdateSearch(e.ID)
					ui.Render(manager.grid)
				}
			}

		case data, ok := <-dataChannel:
			if !ok {
				return
			}
			manager.UpdateTarget(data)

			if options.PrometheusURL != "" {
				region := ""
				if !data.TargetKey.IsLocal {
					region = data.TargetKey.Region
				}
				metrics.RecordCheck(data.Target, data.Result, region)

				if strings.HasPrefix(data.Target.URL, "https://") {
					go func(target config.Target) {
						if sslExpiry := net.GetSSLCertExpiry(target.URL); sslExpiry >= 0 {
							metrics.RecordSSLExpiry(target, sslExpiry)
						}
					}(data.Target)
				}
			}

		case <-uiRefreshTicker.C:
			manager.RefreshStats(monitors)
		}
	}
}

func buildTUIEventData(
	event coordinator.Event,
	targets []config.Target,
	monitors map[string]*stats.Monitor,
	sequences map[string]*int,
	alertStates map[string]*bool,
	webhookAlertStates map[string]*bool,
	options Options,
) TargetData {
	target := targets[event.TargetIndex]
	targetKey := eventTargetKey(target, event)

	monitor, exists := monitors[targetKey.String()]
	if !exists {
		return TargetData{TargetKey: targetKey}
	}

	// All terminal outcomes count; executor failures therefore stay in the
	// availability denominator.
	monitor.AddResult(event.Result)

	sequence := sequences[targetKey.String()]
	*sequence++

	var alertErr error
	var webhookErr error

	if event.Outcome != coordinator.ExecutorFailure {
		if config.BoolVal(target.ReceiveAlert, false) {
			if alertSent, exists := alertStates[targetKey.String()]; exists {
				if err := notifications.HandleAlerts(event.Result.IsUp, alertSent, target.Name, event.Result.URL); err != nil {
					alertErr = err
				}
			}
		}

		if target.WebhookURL != "" {
			errorMsg := ""
			if !event.Result.IsUp {
				switch {
				case event.Result.StatusCode > 0:
					errorMsg = fmt.Sprintf("Non-success status code: %d", event.Result.StatusCode)
				case event.Result.AssertText != "" && !event.Result.AssertionPassed:
					errorMsg = "Assertion failed"
				default:
					errorMsg = "Request failed"
				}
			}

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
					webhookErr = err
				}
			}
		}
	}

	data := TargetData{
		Target:       target,
		Result:       event.Result,
		Stats:        monitor.GetStats(),
		TargetKey:    targetKey,
		AlertError:   alertErr,
		WebhookError: webhookErr,
	}

	if event.Outcome == coordinator.ExecutorFailure {
		data.LambdaError = event.Err
	}

	return data
}

func eventTargetKey(target config.Target, event coordinator.Event) stats.TargetKey {
	indexedName := fmt.Sprintf("%s#%d", target.Name, event.TargetIndex)

	if event.Region != "" {
		return stats.NewRegionTargetKey(indexedName, event.Region, event.TargetIndex)
	}

	return stats.NewLocalTargetKey(indexedName, event.TargetIndex)
}
