//go:build integration
// +build integration

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/routerarchitects/nats-agent-core/agentcore"
)

type testNATSServer struct {
	URL string

	cmd  *exec.Cmd
	logs *bytes.Buffer
}

func startTestNATSServer(t *testing.T) *testNATSServer {
	t.Helper()

	bin, err := exec.LookPath("nats-server")
	if err != nil {
		t.Skip("integration test requires real nats-server -js binary in PATH")
	}

	port := freeTCPPort(t)
	storeDir := t.TempDir()
	logBuf := &bytes.Buffer{}

	cmd := exec.Command(bin,
		"-js",
		"-a", "127.0.0.1",
		"-p", strconv.Itoa(port),
		"-sd", storeDir,
	)
	cmd.Stdout = logBuf
	cmd.Stderr = logBuf

	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start nats-server: %v", err)
	}

	srv := &testNATSServer{
		URL:  fmt.Sprintf("nats://127.0.0.1:%d", port),
		cmd:  cmd,
		logs: logBuf,
	}

	t.Cleanup(func() {
		if srv.cmd.Process != nil {
			_ = srv.cmd.Process.Kill()
		}
		_ = srv.cmd.Wait()
	})

	waitForNATSServerReady(t, srv.URL, srv.logs.String)
	return srv
}

func waitForNATSServerReady(t *testing.T, serverURL string, logs func() string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		nc, err := nats.Connect(serverURL,
			nats.Timeout(200*time.Millisecond),
			nats.NoReconnect(),
		)
		if err == nil {
			nc.Close()
			return
		}

		select {
		case <-ctx.Done():
			t.Fatalf("nats-server did not become ready at %s: %v\nserver logs:\n%s", serverURL, ctx.Err(), logs())
		case <-ticker.C:
		}
	}
}

func freeTCPPort(t *testing.T) int {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to allocate free TCP port: %v", err)
	}
	defer ln.Close()

	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("unexpected listener addr type: %T", ln.Addr())
	}
	return addr.Port
}

func newIntegrationConfig(serverURL, bucket string, autoCreate bool) agentcore.Config {
	return agentcore.Config{
		AgentName: "integration-agent",
		Version:   "1.0",
		NATS: agentcore.NATSConfig{
			Servers:              []string{serverURL},
			ConnectTimeout:       2 * time.Second,
			RetryOnFailedConnect: false,
			MaxReconnects:        2,
			ReconnectWait:        100 * time.Millisecond,
		},
		Subjects: agentcore.SubjectConfig{
			ConfigurePattern: "cmd.configure.%s",
			ActionPattern:    "cmd.action.%s.%s",
			ResultPattern:    "result.%s",
			StatusPattern:    "status.%s",
			HealthPattern:    "health.%s",
		},
		KV: agentcore.KVConfig{
			Bucket:           bucket,
			KeyPattern:       "desired.%s",
			AutoCreateBucket: autoCreate,
			History:          1,
			Replicas:         1,
			Storage:          "file",
		},
		Timeouts: agentcore.TimeoutConfig{
			KVTimeout:       2 * time.Second,
			ShutdownTimeout: 2 * time.Second,
		},
	}
}

func uniqueName(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}
