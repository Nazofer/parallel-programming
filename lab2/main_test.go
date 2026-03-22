package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTagsDatasetAndVariantsMatch(t *testing.T) {
	root := filepath.Join(t.TempDir(), "html")
	_, err := generateHTMLDataset(htmlDatasetConfig{
		Root:     root,
		Files:    24,
		MaxDepth: 2,
		Seed:     5,
	})
	if err != nil {
		t.Fatalf("generateHTMLDataset: %v", err)
	}

	cfg := tagsConfig{Root: root, Threshold: 4}
	seq, err := runTagsTask(cfg, variantSeq, 1)
	if err != nil {
		t.Fatalf("seq: %v", err)
	}
	for _, variant := range []executionVariant{variantMapReduce, variantForkJoin, variantWorkerPool} {
		got, err := runTagsTask(cfg, variant, 3)
		if err != nil {
			t.Fatalf("%s: %v", variant, err)
		}
		if seq.Summary != got.Summary {
			t.Fatalf("summary mismatch for %s: %q != %q", variant, seq.Summary, got.Summary)
		}
	}
}

func TestStatsVariantsMatch(t *testing.T) {
	cfg := statsConfig{Count: 20_000, Seed: 7, Threshold: 512}
	seq, err := runStatsTask(cfg, variantSeq, 1)
	if err != nil {
		t.Fatalf("seq: %v", err)
	}
	for _, variant := range []executionVariant{variantMapReduce, variantForkJoin, variantWorkerPool} {
		got, err := runStatsTask(cfg, variant, 4)
		if err != nil {
			t.Fatalf("%s: %v", variant, err)
		}
		if seq.Summary != got.Summary {
			t.Fatalf("summary mismatch for %s: %q != %q", variant, seq.Summary, got.Summary)
		}
	}
}

func TestMatmulVariantsMatch(t *testing.T) {
	cfg := matmulConfig{Size: 24, Seed: 9, Threshold: 4}
	seq, err := runMatmulTask(cfg, variantSeq, 1)
	if err != nil {
		t.Fatalf("seq: %v", err)
	}
	for _, variant := range []executionVariant{variantMapReduce, variantForkJoin, variantWorkerPool} {
		got, err := runMatmulTask(cfg, variant, 4)
		if err != nil {
			t.Fatalf("%s: %v", variant, err)
		}
		if seq.Summary != got.Summary {
			t.Fatalf("summary mismatch for %s: %q != %q", variant, seq.Summary, got.Summary)
		}
	}
}

func TestTransactionsDatasetAndVariantsMatch(t *testing.T) {
	root := filepath.Join(t.TempDir(), "transactions")
	_, err := generateTransactionsDataset(transactionsDatasetConfig{
		Root:    root,
		Records: 500,
		Seed:    11,
	})
	if err != nil {
		t.Fatalf("generateTransactionsDataset: %v", err)
	}

	cfg := transactionsConfig{
		Input:            filepath.Join(root, "transactions.csv"),
		Buffer:           32,
		ReadWorkers:      2,
		TransformWorkers: 2,
	}
	seq, err := runTransactionsTask(cfg, variantSeq, 1)
	if err != nil {
		t.Fatalf("seq: %v", err)
	}
	for _, variant := range []executionVariant{variantPipeline, variantProdCon} {
		got, err := runTransactionsTask(cfg, variant, 3)
		if err != nil {
			t.Fatalf("%s: %v", variant, err)
		}
		if seq.Summary != got.Summary {
			t.Fatalf("summary mismatch for %s: %q != %q", variant, seq.Summary, got.Summary)
		}
	}
}

func TestCLISmoke(t *testing.T) {
	htmlRoot := filepath.Join(t.TempDir(), "html")
	if _, err := generateHTMLDataset(htmlDatasetConfig{
		Root:     htmlRoot,
		Files:    18,
		MaxDepth: 2,
		Seed:     3,
	}); err != nil {
		t.Fatalf("generateHTMLDataset: %v", err)
	}

	txRoot := filepath.Join(t.TempDir(), "tx")
	if _, err := generateTransactionsDataset(transactionsDatasetConfig{
		Root:    txRoot,
		Records: 200,
		Seed:    3,
	}); err != nil {
		t.Fatalf("generateTransactionsDataset: %v", err)
	}

	cases := []struct {
		args       []string
		needHeader string
	}{
		{[]string{"run", "tags", "--variant", "mapreduce", "--workers", "2", "--root", htmlRoot, "--threshold", "4"}, "Result:"},
		{[]string{"run", "stats", "--variant", "workerpool", "--workers", "2", "--count", "5000", "--threshold", "128"}, "Result:"},
		{[]string{"run", "matmul", "--variant", "forkjoin", "--workers", "2", "--size", "20", "--threshold", "4"}, "Result:"},
		{[]string{"run", "transactions", "--variant", "pipeline", "--workers", "2", "--input", filepath.Join(txRoot, "transactions.csv"), "--buffer", "16"}, "Result:"},
		{[]string{"bench", "stats", "--variants", "mapreduce,workerpool", "--workers", "1,2", "--repeat", "2", "--count", "4000", "--threshold", "128"}, "median_duration"},
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

func TestGenerateCommandSmoke(t *testing.T) {
	htmlRoot := filepath.Join(t.TempDir(), "generated-html")
	txRoot := filepath.Join(t.TempDir(), "generated-tx")

	cases := []struct {
		args     []string
		check    string
		expected string
	}{
		{
			args:     []string{"generate", "html-dataset", "--root", htmlRoot, "--files", "12", "--max-depth", "2", "--seed", "13"},
			check:    htmlRoot,
			expected: "HTML dataset generated",
		},
		{
			args:     []string{"generate", "transactions-dataset", "--root", txRoot, "--records", "25", "--seed", "13"},
			check:    filepath.Join(txRoot, "transactions.csv"),
			expected: "Transactions dataset generated",
		},
	}

	for _, tc := range cases {
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		if err := runCLI(tc.args, &stdout, &stderr); err != nil {
			t.Fatalf("runCLI(%v): %v stderr=%s", tc.args, err, stderr.String())
		}
		if _, err := os.Stat(tc.check); err != nil {
			t.Fatalf("expected path to exist %s: %v", tc.check, err)
		}
		if !strings.Contains(stdout.String(), tc.expected) {
			t.Fatalf("unexpected output: %s", stdout.String())
		}
	}
}

func TestDatasetDeterminism(t *testing.T) {
	htmlRootA := filepath.Join(t.TempDir(), "html-a")
	htmlRootB := filepath.Join(t.TempDir(), "html-b")
	if _, err := generateHTMLDataset(htmlDatasetConfig{Root: htmlRootA, Files: 10, MaxDepth: 2, Seed: 19}); err != nil {
		t.Fatalf("generateHTMLDataset a: %v", err)
	}
	if _, err := generateHTMLDataset(htmlDatasetConfig{Root: htmlRootB, Files: 10, MaxDepth: 2, Seed: 19}); err != nil {
		t.Fatalf("generateHTMLDataset b: %v", err)
	}
	if readTreeSignature(t, htmlRootA) != readTreeSignature(t, htmlRootB) {
		t.Fatalf("html dataset should be deterministic")
	}

	txRootA := filepath.Join(t.TempDir(), "tx-a")
	txRootB := filepath.Join(t.TempDir(), "tx-b")
	if _, err := generateTransactionsDataset(transactionsDatasetConfig{Root: txRootA, Records: 20, Seed: 29}); err != nil {
		t.Fatalf("generateTransactionsDataset a: %v", err)
	}
	if _, err := generateTransactionsDataset(transactionsDatasetConfig{Root: txRootB, Records: 20, Seed: 29}); err != nil {
		t.Fatalf("generateTransactionsDataset b: %v", err)
	}
	contentA, err := os.ReadFile(filepath.Join(txRootA, "transactions.csv"))
	if err != nil {
		t.Fatalf("read tx a: %v", err)
	}
	contentB, err := os.ReadFile(filepath.Join(txRootB, "transactions.csv"))
	if err != nil {
		t.Fatalf("read tx b: %v", err)
	}
	if !bytes.Equal(contentA, contentB) {
		t.Fatalf("transactions dataset should be deterministic")
	}
}

func readTreeSignature(t *testing.T, root string) string {
	t.Helper()
	var parts []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		parts = append(parts, relative+"="+string(content))
		return nil
	})
	if err != nil {
		t.Fatalf("walk signature: %v", err)
	}
	return strings.Join(parts, "\n")
}
