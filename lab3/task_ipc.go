package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

type ipcConfig struct {
	Iterations int
	Seed       int64
}

func parseIPCFlags(args []string, stderr io.Writer) (ipcConfig, error) {
	fs := newFlagSet("ipc", stderr)
	cfg := ipcConfig{}
	fs.IntVar(&cfg.Iterations, "iterations", 1000, "number of round trips")
	fs.Int64Var(&cfg.Seed, "seed", 42, "deterministic seed")
	if err := fs.Parse(args); err != nil {
		return ipcConfig{}, err
	}
	if err := parsePositiveInt("iterations", cfg.Iterations); err != nil {
		return ipcConfig{}, err
	}
	return cfg, nil
}

func runIPCTask(cfg ipcConfig, variant executionVariant) (TaskResult, error) {
	start := time.Now()

	var totalLatency time.Duration
	var err error
	switch variant {
	case variantPipe:
		totalLatency, err = runIPCPipe(cfg)
	case variantSocket:
		totalLatency, err = runIPCSocket(cfg)
	case variantMmap:
		totalLatency, err = runIPCMmap(cfg)
	default:
		return TaskResult{}, fmt.Errorf("unsupported ipc variant %q", variant)
	}
	if err != nil {
		return TaskResult{}, err
	}

	avgLatency := totalLatency / time.Duration(cfg.Iterations)

	return TaskResult{
		Task:    "ipc",
		Variant: string(variant),
		Workers: 2,
		Params: map[string]string{
			"iterations": strconv.Itoa(cfg.Iterations),
			"seed":       strconv.FormatInt(cfg.Seed, 10),
		},
		Summary: fmt.Sprintf("iterations=%d avg_latency=%s total_latency=%s",
			cfg.Iterations, avgLatency, totalLatency),
		Duration: time.Since(start),
	}, nil
}

// --- Pipe ---

func runIPCPipe(cfg ipcConfig) (time.Duration, error) {
	scriptPath, cleanup, err := writePythonWorker()
	if err != nil {
		return 0, err
	}
	defer cleanup()

	cmd := exec.Command("python3", scriptPath, "pipe")
	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return 0, err
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return 0, err
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("failed to start python worker: %w", err)
	}

	rng := rand.New(rand.NewSource(cfg.Seed))
	var totalLatency time.Duration

	for i := 0; i < cfg.Iterations; i++ {
		value := rng.Int63()
		t0 := time.Now()
		if err := binary.Write(stdinPipe, binary.LittleEndian, value); err != nil {
			stdinPipe.Close()
			cmd.Wait()
			return 0, fmt.Errorf("write iteration %d: %w", i, err)
		}
		var response int64
		if err := binary.Read(stdoutPipe, binary.LittleEndian, &response); err != nil {
			stdinPipe.Close()
			cmd.Wait()
			return 0, fmt.Errorf("read iteration %d: %w", i, err)
		}
		totalLatency += time.Since(t0)
		if response != value {
			stdinPipe.Close()
			cmd.Wait()
			return 0, fmt.Errorf("mismatch at iteration %d: sent %d, got %d", i, value, response)
		}
	}

	stdinPipe.Close()
	if err := cmd.Wait(); err != nil {
		return 0, fmt.Errorf("python worker exited with error: %w", err)
	}
	return totalLatency, nil
}

// --- Unix Domain Socket ---

func runIPCSocket(cfg ipcConfig) (time.Duration, error) {
	scriptPath, cleanup, err := writePythonWorker()
	if err != nil {
		return 0, err
	}
	defer cleanup()

	tmpDir, err := os.MkdirTemp("", "lab3-ipc-")
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(tmpDir)

	socketPath := filepath.Join(tmpDir, "ipc.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return 0, err
	}
	defer listener.Close()

	cmd := exec.Command("python3", scriptPath, "socket", socketPath)
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("failed to start python worker: %w", err)
	}

	conn, err := listener.Accept()
	if err != nil {
		cmd.Wait()
		return 0, err
	}
	defer conn.Close()

	rng := rand.New(rand.NewSource(cfg.Seed))
	var totalLatency time.Duration

	for i := 0; i < cfg.Iterations; i++ {
		value := rng.Int63()
		t0 := time.Now()
		if err := binary.Write(conn, binary.LittleEndian, value); err != nil {
			cmd.Wait()
			return 0, fmt.Errorf("write iteration %d: %w", i, err)
		}
		var response int64
		if err := binary.Read(conn, binary.LittleEndian, &response); err != nil {
			cmd.Wait()
			return 0, fmt.Errorf("read iteration %d: %w", i, err)
		}
		totalLatency += time.Since(t0)
		if response != value {
			cmd.Wait()
			return 0, fmt.Errorf("mismatch at iteration %d: sent %d, got %d", i, value, response)
		}
	}

	conn.Close()
	if err := cmd.Wait(); err != nil {
		// Python exits when connection closes, ignore exit error
	}
	return totalLatency, nil
}

// --- Shared Memory (mmap) ---

func runIPCMmap(cfg ipcConfig) (time.Duration, error) {
	scriptPath, cleanup, err := writePythonWorker()
	if err != nil {
		return 0, err
	}
	defer cleanup()

	tmpFile, err := os.CreateTemp("", "lab3-mmap-*")
	if err != nil {
		return 0, err
	}
	mmapPath := tmpFile.Name()
	defer os.Remove(mmapPath)

	// Allocate 16 bytes: [flag(1)][value(8)][padding(7)]
	if err := tmpFile.Truncate(16); err != nil {
		tmpFile.Close()
		return 0, err
	}
	tmpFile.Close()

	fd, err := syscall.Open(mmapPath, syscall.O_RDWR, 0)
	if err != nil {
		return 0, err
	}
	defer syscall.Close(fd)

	data, err := syscall.Mmap(fd, 0, 16, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return 0, fmt.Errorf("mmap failed: %w", err)
	}
	defer syscall.Munmap(data)

	// Clear the shared memory
	for i := range data {
		data[i] = 0
	}

	cmd := exec.Command("python3", scriptPath, "mmap", mmapPath)
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("failed to start python worker: %w", err)
	}

	rng := rand.New(rand.NewSource(cfg.Seed))
	var totalLatency time.Duration

	for i := 0; i < cfg.Iterations; i++ {
		value := rng.Int63()
		t0 := time.Now()

		// Write value to bytes 1-8
		binary.LittleEndian.PutUint64(data[1:9], uint64(value))
		// Signal child: flag = 0x01
		data[0] = 0x01

		// Spin-wait for child response: flag = 0x02
		for data[0] != 0x02 {
			time.Sleep(time.Microsecond)
		}

		// Read response from bytes 1-8
		response := int64(binary.LittleEndian.Uint64(data[1:9]))
		totalLatency += time.Since(t0)

		// Reset flag
		data[0] = 0x00

		if response != value {
			// Terminate child
			data[0] = 0xFF
			cmd.Wait()
			return 0, fmt.Errorf("mismatch at iteration %d: sent %d, got %d", i, value, response)
		}
	}

	// Signal termination
	data[0] = 0xFF
	cmd.Wait()
	return totalLatency, nil
}

// --- Python worker script ---

const pythonWorkerScript = `#!/usr/bin/env python3
import sys
import socket
import mmap
import os
import time

def run_pipe():
    while True:
        data = sys.stdin.buffer.read(8)
        if len(data) < 8:
            break
        sys.stdout.buffer.write(data)
        sys.stdout.buffer.flush()

def run_socket(path):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(path)
    try:
        while True:
            data = b""
            while len(data) < 8:
                chunk = sock.recv(8 - len(data))
                if not chunk:
                    return
                data += chunk
            sock.sendall(data)
    finally:
        sock.close()

def run_mmap(path):
    fd = os.open(path, os.O_RDWR)
    mm = mmap.mmap(fd, 16)
    try:
        while True:
            # Spin until parent writes (flag=1) or terminates (flag=0xFF)
            flag = mm[0]
            while flag != 0x01:
                if flag == 0xFF:
                    return
                time.sleep(0.000001)
                flag = mm[0]
            # Echo the value back (it stays in bytes 1-8)
            mm[0] = 0x02
    finally:
        mm.close()
        os.close(fd)

if __name__ == "__main__":
    method = sys.argv[1]
    if method == "pipe":
        run_pipe()
    elif method == "socket":
        run_socket(sys.argv[2])
    elif method == "mmap":
        run_mmap(sys.argv[2])
`

func writePythonWorker() (string, func(), error) {
	tmpFile, err := os.CreateTemp("", "lab3-worker-*.py")
	if err != nil {
		return "", nil, err
	}
	path := tmpFile.Name()
	if _, err := tmpFile.WriteString(pythonWorkerScript); err != nil {
		tmpFile.Close()
		os.Remove(path)
		return "", nil, err
	}
	tmpFile.Close()
	return path, func() { os.Remove(path) }, nil
}
