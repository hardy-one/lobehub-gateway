package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"
)

func TestConfigsFromEnvUsesDedicatedPorts(t *testing.T) {
	t.Setenv("SERVICE_TOKEN", "test-token")
	t.Setenv("PORT", "9999")
	t.Setenv("AGENT_PORT", "")
	t.Setenv("DEVICE_PORT", "")
	t.Setenv("MESSAGE_PORT", "")

	configs, err := configsFromEnv()
	if err != nil {
		t.Fatalf("configsFromEnv() error = %v", err)
	}
	if configs.agent.Port != "8787" {
		t.Fatalf("agent port = %q, want 8787", configs.agent.Port)
	}
	if configs.device.Port != "8788" {
		t.Fatalf("device port = %q, want 8788", configs.device.Port)
	}
	if configs.message.Port != "8790" {
		t.Fatalf("message port = %q, want 8790", configs.message.Port)
	}

	t.Setenv("AGENT_PORT", "9000")
	t.Setenv("DEVICE_PORT", "9001")
	t.Setenv("MESSAGE_PORT", "9002")
	configs, err = configsFromEnv()
	if err != nil {
		t.Fatalf("configsFromEnv() with overrides error = %v", err)
	}
	if configs.agent.Port != "9000" || configs.device.Port != "9001" || configs.message.Port != "9002" {
		t.Fatalf("ports = %q/%q/%q, want 9000/9001/9002", configs.agent.Port, configs.device.Port, configs.message.Port)
	}
}

func TestConfigsFromEnvRequiresServiceToken(t *testing.T) {
	t.Setenv("SERVICE_TOKEN", "")

	if _, err := configsFromEnv(); err == nil {
		t.Fatal("configsFromEnv() error = nil, want validation error")
	}
}

func TestRunServicesServesEveryGatewayAndStopsCleanly(t *testing.T) {
	t.Setenv("SERVICE_TOKEN", "test-token")
	ports := availablePorts(t, 3)
	t.Setenv("AGENT_PORT", ports[0])
	t.Setenv("DEVICE_PORT", ports[1])
	t.Setenv("MESSAGE_PORT", ports[2])

	configs, err := configsFromEnv()
	if err != nil {
		t.Fatalf("configsFromEnv() error = %v", err)
	}

	stop := make(chan os.Signal, 1)
	done := make(chan int, 1)
	go func() {
		done <- runServices(newServices(configs), stop, time.Second)
	}()

	waitForHealth(t, "http://127.0.0.1:"+configs.agent.Port+"/health")
	waitForHealth(t, "http://127.0.0.1:"+configs.device.Port+"/health")
	waitForHealth(t, "http://127.0.0.1:"+configs.message.Port+"/health")
	stop <- os.Interrupt

	select {
	case exitCode := <-done:
		if exitCode != 0 {
			t.Fatalf("runServices() exit code = %d, want 0", exitCode)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runServices() did not stop")
	}
}

func TestRunServicesReturnsFailureAndShutsDownAllServices(t *testing.T) {
	listenErr := errors.New("listen failed")
	blocked := make(chan struct{})
	var shutdowns sync.WaitGroup
	shutdowns.Add(2)

	services := []service{
		{
			name:   "failed",
			listen: func() error { return listenErr },
			shutdown: func(context.Context) error {
				shutdowns.Done()
				return nil
			},
		},
		{
			name:   "running",
			listen: func() error { <-blocked; return http.ErrServerClosed },
			shutdown: func(context.Context) error {
				close(blocked)
				shutdowns.Done()
				return nil
			},
		},
	}

	if exitCode := runServices(services, make(chan os.Signal), time.Second); exitCode != 1 {
		t.Fatalf("runServices() exit code = %d, want 1", exitCode)
	}

	completed := make(chan struct{})
	go func() {
		shutdowns.Wait()
		close(completed)
	}()
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("not all services were shut down")
	}
}

func TestShutdownServicesRunsConcurrently(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	services := []service{
		shutdownTestService("agent", started, release),
		shutdownTestService("device", started, release),
	}

	done := make(chan bool, 1)
	go func() {
		done <- shutdownServices(services, time.Second)
	}()

	seen := make(map[string]bool)
	for range services {
		select {
		case name := <-started:
			seen[name] = true
		case <-time.After(200 * time.Millisecond):
			t.Fatal("shutdowns did not start concurrently")
		}
	}
	close(release)

	if failed := <-done; failed {
		t.Fatal("shutdownServices() reported a failure")
	}
	if !seen["agent"] || !seen["device"] {
		t.Fatalf("started shutdowns = %v, want both services", seen)
	}
}

func TestShutdownServicesUsesSingleDeadline(t *testing.T) {
	release := make(chan struct{})
	services := []service{
		shutdownTestService("agent", make(chan string, 1), release),
		shutdownTestService("device", make(chan string, 1), release),
	}

	startedAt := time.Now()
	failed := shutdownServices(services, 50*time.Millisecond)
	elapsed := time.Since(startedAt)
	close(release)

	if !failed {
		t.Fatal("shutdownServices() reported success after timing out")
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("shutdownServices() took %v, want one shared deadline", elapsed)
	}
}

func shutdownTestService(name string, started chan<- string, release <-chan struct{}) service {
	return service{
		name: name,
		shutdown: func(context.Context) error {
			started <- name
			<-release
			return nil
		},
	}
}

func availablePorts(t *testing.T, count int) []string {
	t.Helper()
	listeners := make([]net.Listener, count)
	for i := range listeners {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("net.Listen() error = %v", err)
		}
		listeners[i] = listener
	}

	ports := make([]string, len(listeners))
	for i, listener := range listeners {
		ports[i] = fmt.Sprint(listener.Addr().(*net.TCPAddr).Port)
		if err := listener.Close(); err != nil {
			t.Fatalf("listener.Close() error = %v", err)
		}
	}
	return ports
}

func waitForHealth(t *testing.T, url string) {
	t.Helper()
	client := &http.Client{Timeout: 200 * time.Millisecond}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Get(url)
		if err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("health endpoint did not become ready: %s", url)
}
