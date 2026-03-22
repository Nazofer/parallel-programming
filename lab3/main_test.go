package main

import (
	"bytes"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestBankFixedConsistency(t *testing.T) {
	cfg := bankConfig{Accounts: 50, Transfers: 50, Seed: 7, Timeout: 10 * time.Second}
	result, err := runBankTask(cfg, variantFixed, 200)
	if err != nil {
		t.Fatalf("fixed: %v", err)
	}
	if !strings.Contains(result.Summary, "consistent=true") {
		t.Fatalf("expected consistent=true, got: %s", result.Summary)
	}
}

func TestBankSeqConsistency(t *testing.T) {
	cfg := bankConfig{Accounts: 50, Transfers: 200, Seed: 7, Timeout: 10 * time.Second}
	result, err := runBankTask(cfg, variantSeq, 10)
	if err != nil {
		t.Fatalf("seq: %v", err)
	}
	if !strings.Contains(result.Summary, "consistent=true") {
		t.Fatalf("expected consistent=true, got: %s", result.Summary)
	}
}

func TestZZ_BankDeadlockTimeout(t *testing.T) {
	// Named TestZZ_ to run last: deadlock variant leaks goroutines blocked
	// on mutexes, which cannot be cleaned up.
	cfg := bankConfig{Accounts: 4, Transfers: 100000, Seed: 3, Timeout: 2 * time.Second}
	_, err := runBankTask(cfg, variantDeadlock, 50)
	// Either completes or detects deadlock — both are acceptable
	if err != nil {
		if !strings.Contains(err.Error(), "deadlock") {
			t.Fatalf("unexpected error: %v", err)
		}
		t.Logf("deadlock correctly detected: %v", err)
	}
}

func TestBankUnsafeRuns(t *testing.T) {
	cfg := bankConfig{Accounts: 10, Transfers: 10, Seed: 5, Timeout: 10 * time.Second}
	// Just verify it doesn't panic/hang — results may be inconsistent
	_, _ = runBankTask(cfg, variantUnsafe, 5)
}

func hasPython3() bool {
	_, err := exec.LookPath("python3")
	return err == nil
}

func TestIPCPipe(t *testing.T) {
	if !hasPython3() {
		t.Skip("python3 not found")
	}
	cfg := ipcConfig{Iterations: 10, Seed: 42}
	result, err := runIPCTask(cfg, variantPipe)
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	if !strings.Contains(result.Summary, "iterations=10") {
		t.Fatalf("unexpected summary: %s", result.Summary)
	}
}

func TestIPCSocket(t *testing.T) {
	if !hasPython3() {
		t.Skip("python3 not found")
	}
	cfg := ipcConfig{Iterations: 10, Seed: 42}
	result, err := runIPCTask(cfg, variantSocket)
	if err != nil {
		t.Fatalf("socket: %v", err)
	}
	if !strings.Contains(result.Summary, "iterations=10") {
		t.Fatalf("unexpected summary: %s", result.Summary)
	}
}

func TestIPCMmap(t *testing.T) {
	if !hasPython3() {
		t.Skip("python3 not found")
	}
	cfg := ipcConfig{Iterations: 10, Seed: 42}
	result, err := runIPCTask(cfg, variantMmap)
	if err != nil {
		t.Fatalf("mmap: %v", err)
	}
	if !strings.Contains(result.Summary, "iterations=10") {
		t.Fatalf("unexpected summary: %s", result.Summary)
	}
}

func TestCLISmoke(t *testing.T) {
	cases := []struct {
		args       []string
		needHeader string
	}{
		{[]string{"run", "bank", "--variant", "fixed", "--workers", "50", "--accounts", "20", "--transfers", "10"}, "Result:"},
		{[]string{"run", "bank", "--variant", "seq", "--workers", "10", "--accounts", "20", "--transfers", "10"}, "Result:"},
	}

	if hasPython3() {
		cases = append(cases, struct {
			args       []string
			needHeader string
		}{[]string{"run", "ipc", "--variant", "pipe", "--iterations", "5"}, "Result:"})
	}

	for _, tc := range cases {
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		if err := runCLI(tc.args, &stdout, &stderr); err != nil {
			t.Fatalf("runCLI(%v): %v stderr=%s", tc.args, err, stderr.String())
		}
		if !strings.Contains(stdout.String(), tc.needHeader) {
			t.Fatalf("unexpected stdout for %v: %s", tc.args, stdout.String())
		}
	}
}
