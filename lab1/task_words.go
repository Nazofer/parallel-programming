package main

import (
	"bufio"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type wordsConfig struct {
	Root string
}

type wordsDatasetConfig struct {
	Root     string
	Files    int
	MaxDepth int
	MinWords int
	MaxWords int
	Seed     int64
}

type wordsDatasetStats struct {
	Directories int
	Files       int
	Words       int
}

type wordsStats struct {
	Files int
	Words int
}

func parseWordsFlags(args []string, stderr io.Writer) (wordsConfig, error) {
	fs := newFlagSet("words", stderr)
	cfg := wordsConfig{}
	fs.StringVar(&cfg.Root, "root", "./tmp/words-dataset", "коренева директорія з текстовими файлами")
	if err := fs.Parse(args); err != nil {
		return wordsConfig{}, err
	}
	if strings.TrimSpace(cfg.Root) == "" {
		return wordsConfig{}, fmt.Errorf("root cannot be empty")
	}
	return cfg, nil
}

func parseWordsDatasetFlags(args []string, stderr io.Writer) (wordsDatasetConfig, error) {
	fs := newFlagSet("words-dataset", stderr)
	cfg := wordsDatasetConfig{}
	fs.StringVar(&cfg.Root, "root", "./tmp/words-dataset", "куди згенерувати dataset")
	fs.IntVar(&cfg.Files, "files", 300, "кількість файлів")
	fs.IntVar(&cfg.MaxDepth, "max-depth", 4, "максимальна глибина вкладеності")
	fs.IntVar(&cfg.MinWords, "min-words", 100, "мінімум слів у файлі")
	fs.IntVar(&cfg.MaxWords, "max-words", 800, "максимум слів у файлі")
	fs.Int64Var(&cfg.Seed, "seed", 42, "детермінований seed")
	if err := fs.Parse(args); err != nil {
		return wordsDatasetConfig{}, err
	}
	if strings.TrimSpace(cfg.Root) == "" {
		return wordsDatasetConfig{}, fmt.Errorf("root cannot be empty")
	}
	if err := parsePositiveInt("files", cfg.Files); err != nil {
		return wordsDatasetConfig{}, err
	}
	if err := parsePositiveInt("max-depth", cfg.MaxDepth); err != nil {
		return wordsDatasetConfig{}, err
	}
	if err := parsePositiveInt("min-words", cfg.MinWords); err != nil {
		return wordsDatasetConfig{}, err
	}
	if err := parsePositiveInt("max-words", cfg.MaxWords); err != nil {
		return wordsDatasetConfig{}, err
	}
	if cfg.MaxWords < cfg.MinWords {
		return wordsDatasetConfig{}, fmt.Errorf("max-words must be >= min-words")
	}
	return cfg, nil
}

func runWordsTask(cfg wordsConfig, mode string, workers int) (TaskResult, error) {
	start := time.Now()
	files, err := collectTextFiles(cfg.Root)
	if err != nil {
		return TaskResult{}, err
	}

	var stats wordsStats
	if mode == "seq" {
		stats, err = countWordsSequential(files)
		workers = 1
	} else {
		stats, err = countWordsParallel(files, workers)
	}
	if err != nil {
		return TaskResult{}, err
	}

	return TaskResult{
		Task:    "words",
		Mode:    mode,
		Workers: workers,
		Params: map[string]string{
			"root":  cfg.Root,
			"files": strconv.Itoa(len(files)),
		},
		Summary:  fmt.Sprintf("files=%d words=%d", stats.Files, stats.Words),
		Duration: time.Since(start),
	}, nil
}

func generateWordsDataset(cfg wordsDatasetConfig) (wordsDatasetStats, error) {
	if err := os.RemoveAll(cfg.Root); err != nil {
		return wordsDatasetStats{}, err
	}
	if err := os.MkdirAll(cfg.Root, 0o755); err != nil {
		return wordsDatasetStats{}, err
	}

	rng := rand.New(rand.NewSource(cfg.Seed))
	dirs := []string{cfg.Root}
	stats := wordsDatasetStats{Directories: 1}

	for depth := 1; depth <= cfg.MaxDepth; depth++ {
		parentCount := len(dirs)
		for idx := 0; idx < parentCount; idx++ {
			if rng.Intn(100) < 45 {
				dir := filepath.Join(dirs[idx], fmt.Sprintf("level_%d_%d", depth, idx))
				if err := os.MkdirAll(dir, 0o755); err != nil {
					return wordsDatasetStats{}, err
				}
				dirs = append(dirs, dir)
				stats.Directories++
			}
		}
	}

	for fileIndex := 0; fileIndex < cfg.Files; fileIndex++ {
		dir := dirs[rng.Intn(len(dirs))]
		path := filepath.Join(dir, fmt.Sprintf("sample_%04d.txt", fileIndex))
		wordCount := cfg.MinWords
		if cfg.MaxWords > cfg.MinWords {
			wordCount += rng.Intn(cfg.MaxWords - cfg.MinWords + 1)
		}
		if err := os.WriteFile(path, []byte(buildRandomText(rng, wordCount)), 0o644); err != nil {
			return wordsDatasetStats{}, err
		}
		stats.Files++
		stats.Words += wordCount
	}

	return stats, nil
}

func buildRandomText(rng *rand.Rand, wordCount int) string {
	parts := make([]string, wordCount)
	for idx := range parts {
		parts[idx] = randomWord(rng)
	}
	return strings.Join(parts, " ")
}

func randomWord(rng *rand.Rand) string {
	length := 3 + rng.Intn(8)
	var builder strings.Builder
	builder.Grow(length)
	for i := 0; i < length; i++ {
		builder.WriteByte(byte('a' + rng.Intn(26)))
	}
	return builder.String()
}

func collectTextFiles(root string) ([]string, error) {
	files := make([]string, 0, 128)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if strings.EqualFold(filepath.Ext(entry.Name()), ".txt") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

func countWordsSequential(files []string) (wordsStats, error) {
	stats := wordsStats{}
	for _, path := range files {
		count, err := countWordsInFile(path)
		if err != nil {
			return wordsStats{}, err
		}
		stats.Files++
		stats.Words += count
	}
	return stats, nil
}

func countWordsParallel(files []string, workers int) (wordsStats, error) {
	type result struct {
		Count int
		Err   error
	}

	paths := make(chan string)
	results := make(chan result, len(files))

	for i := 0; i < workers; i++ {
		go func() {
			for path := range paths {
				count, err := countWordsInFile(path)
				results <- result{Count: count, Err: err}
			}
		}()
	}

	go func() {
		for _, path := range files {
			paths <- path
		}
		close(paths)
	}()

	stats := wordsStats{}
	for i := 0; i < len(files); i++ {
		result := <-results
		if result.Err != nil {
			return wordsStats{}, result.Err
		}
		stats.Files++
		stats.Words += result.Count
	}
	return stats, nil
}

func countWordsInFile(path string) (int, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanWords)
	buffer := make([]byte, 0, 64*1024)
	scanner.Buffer(buffer, 1024*1024)

	count := 0
	for scanner.Scan() {
		count++
	}
	return count, scanner.Err()
}
