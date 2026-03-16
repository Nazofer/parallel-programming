package main

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPiSequentialAndParallelMatch(t *testing.T) {
	cfg := piConfig{Iterations: 100_000, Seed: 7}
	seq, err := runPiTask(cfg, "seq", 1)
	if err != nil {
		t.Fatalf("runPiTask seq: %v", err)
	}
	par, err := runPiTask(cfg, "par", 4)
	if err != nil {
		t.Fatalf("runPiTask par: %v", err)
	}
	if seq.Summary != par.Summary {
		t.Fatalf("expected identical summaries, got %q and %q", seq.Summary, par.Summary)
	}
}

func TestPiApproximationReasonable(t *testing.T) {
	cfg := piConfig{Iterations: 500_000, Seed: 11}
	result, err := runPiTask(cfg, "seq", 1)
	if err != nil {
		t.Fatalf("runPiTask: %v", err)
	}
	value := extractMetric(result.Summary, "pi")
	if math.Abs(value-math.Pi) > 0.02 {
		t.Fatalf("pi approximation too far: %.8f", value)
	}
}

func TestPrimeFactors(t *testing.T) {
	got := primeFactors(360)
	want := []uint64{2, 2, 2, 3, 3, 5}
	if len(got) != len(want) {
		t.Fatalf("unexpected factors len: %v", got)
	}
	for idx := range want {
		if got[idx] != want[idx] {
			t.Fatalf("unexpected factors: %v", got)
		}
	}
}

func TestPrimesHelpers(t *testing.T) {
	got := collectPrimes(2, 30)
	want := []int{2, 3, 5, 7, 11, 13, 17, 19, 23, 29}
	if len(got) != len(want) {
		t.Fatalf("unexpected prime count: %v", got)
	}
	for idx := range want {
		if got[idx] != want[idx] {
			t.Fatalf("unexpected primes: %v", got)
		}
	}
}

func TestPrimesSequentialAndParallelMatch(t *testing.T) {
	cfg := primesConfig{From: 2, To: 100_000}
	seq, err := runPrimesTask(cfg, "seq", 1)
	if err != nil {
		t.Fatalf("seq: %v", err)
	}
	par, err := runPrimesTask(cfg, "par", 4)
	if err != nil {
		t.Fatalf("par: %v", err)
	}
	if seq.Summary != par.Summary {
		t.Fatalf("summary mismatch: %q != %q", seq.Summary, par.Summary)
	}
}

func TestMatrixSequentialAndParallelMatch(t *testing.T) {
	src := buildMatrix(32, 16)
	seq := transposeSequential(src, 32, 16)
	par := transposeParallel(src, 32, 16, 4)
	if len(seq) != len(par) {
		t.Fatalf("length mismatch")
	}
	for idx := range seq {
		if seq[idx] != par[idx] {
			t.Fatalf("matrix mismatch at %d", idx)
		}
	}
}

func TestWordsDatasetAndCounter(t *testing.T) {
	root := filepath.Join(t.TempDir(), "dataset")
	_, err := generateWordsDataset(wordsDatasetConfig{
		Root:     root,
		Files:    12,
		MaxDepth: 2,
		MinWords: 5,
		MaxWords: 10,
		Seed:     5,
	})
	if err != nil {
		t.Fatalf("generateWordsDataset: %v", err)
	}

	files, err := collectTextFiles(root)
	if err != nil {
		t.Fatalf("collectTextFiles: %v", err)
	}
	seq, err := countWordsSequential(files)
	if err != nil {
		t.Fatalf("countWordsSequential: %v", err)
	}
	par, err := countWordsParallel(files, 3)
	if err != nil {
		t.Fatalf("countWordsParallel: %v", err)
	}
	if seq != par {
		t.Fatalf("word counts mismatch: %+v != %+v", seq, par)
	}
}

func TestCLICommandsSmoke(t *testing.T) {
	root := filepath.Join(t.TempDir(), "words")
	if _, err := generateWordsDataset(wordsDatasetConfig{
		Root:     root,
		Files:    10,
		MaxDepth: 2,
		MinWords: 5,
		MaxWords: 8,
		Seed:     3,
	}); err != nil {
		t.Fatalf("generateWordsDataset: %v", err)
	}

	cases := [][]string{
		{"run", "pi", "--mode", "par", "--workers", "2", "--iterations", "10000"},
		{"run", "factorize", "--mode", "seq", "--numbers", "360,1024,99991"},
		{"run", "primes", "--mode", "par", "--workers", "2", "--from", "2", "--to", "1000"},
		{"run", "matrix", "--mode", "par", "--workers", "2", "--rows", "64", "--cols", "32"},
		{"run", "words", "--mode", "par", "--workers", "2", "--root", root},
		{"bench", "pi", "--workers", "1,2", "--repeat", "2", "--iterations", "10000"},
	}

	for _, args := range cases {
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		if err := runCLI(args, &stdout, &stderr); err != nil {
			t.Fatalf("runCLI(%v): %v stderr=%s", args, err, stderr.String())
		}
		if stdout.Len() == 0 {
			t.Fatalf("expected stdout for args %v", args)
		}
	}
}

func TestGenerateCommandSmoke(t *testing.T) {
	root := filepath.Join(t.TempDir(), "generated")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runCLI([]string{"generate", "words-dataset", "--root", root, "--files", "5", "--max-depth", "2", "--min-words", "3", "--max-words", "5"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("generate command failed: %v stderr=%s", err, stderr.String())
	}
	if _, err := os.Stat(root); err != nil {
		t.Fatalf("expected dataset root to exist: %v", err)
	}
	if !strings.Contains(stdout.String(), "Words dataset generated") {
		t.Fatalf("unexpected output: %s", stdout.String())
	}
}

func extractMetric(summary, key string) float64 {
	parts := strings.Fields(summary)
	prefix := key + "="
	for _, part := range parts {
		if strings.HasPrefix(part, prefix) {
			var value float64
			_, _ = fmtSscanf(part, prefix+"%f", &value)
			return value
		}
	}
	return 0
}

func fmtSscanf(str, format string, a ...any) (int, error) {
	return fmt.Sscanf(str, format, a...)
}
