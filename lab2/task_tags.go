package main

import (
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type tagsConfig struct {
	Root      string
	Threshold int
}

type htmlDatasetConfig struct {
	Root     string
	Files    int
	MaxDepth int
	Seed     int64
}

type htmlDatasetStats struct {
	Directories int
	Files       int
	Tags        int
}

type tagStats struct {
	Files  int
	Counts map[string]int
}

func parseTagsFlags(args []string, stderr io.Writer) (tagsConfig, error) {
	fs := newFlagSet("tags", stderr)
	cfg := tagsConfig{}
	fs.StringVar(&cfg.Root, "root", "./tmp/html-dataset", "root directory with html files")
	fs.IntVar(&cfg.Threshold, "threshold", 32, "threshold for fork-join")
	if err := fs.Parse(args); err != nil {
		return tagsConfig{}, err
	}
	if strings.TrimSpace(cfg.Root) == "" {
		return tagsConfig{}, fmt.Errorf("root cannot be empty")
	}
	if err := parsePositiveInt("threshold", cfg.Threshold); err != nil {
		return tagsConfig{}, err
	}
	return cfg, nil
}

func parseHTMLDatasetFlags(args []string, stderr io.Writer) (htmlDatasetConfig, error) {
	fs := newFlagSet("html-dataset", stderr)
	cfg := htmlDatasetConfig{}
	fs.StringVar(&cfg.Root, "root", "./tmp/html-dataset", "output directory for the dataset")
	fs.IntVar(&cfg.Files, "files", 1200, "number of html files")
	fs.IntVar(&cfg.MaxDepth, "max-depth", 4, "maximum nesting depth")
	fs.Int64Var(&cfg.Seed, "seed", 42, "deterministic seed")
	if err := fs.Parse(args); err != nil {
		return htmlDatasetConfig{}, err
	}
	if strings.TrimSpace(cfg.Root) == "" {
		return htmlDatasetConfig{}, fmt.Errorf("root cannot be empty")
	}
	if err := parsePositiveInt("files", cfg.Files); err != nil {
		return htmlDatasetConfig{}, err
	}
	if err := parsePositiveInt("max-depth", cfg.MaxDepth); err != nil {
		return htmlDatasetConfig{}, err
	}
	return cfg, nil
}

func runTagsTask(cfg tagsConfig, variant executionVariant, workers int) (TaskResult, error) {
	start := time.Now()
	files, err := collectHTMLFiles(cfg.Root)
	if err != nil {
		return TaskResult{}, err
	}

	var stats tagStats
	switch variant {
	case variantSeq:
		stats, err = countTagsSequential(files)
		workers = 1
	case variantMapReduce:
		stats, err = countTagsMapReduce(files, workers)
	case variantForkJoin:
		stats, err = countTagsForkJoin(files, workers, cfg.Threshold)
	case variantWorkerPool:
		stats, err = countTagsWorkerPool(files, workers)
	default:
		return TaskResult{}, fmt.Errorf("unsupported tags variant %q", variant)
	}
	if err != nil {
		return TaskResult{}, err
	}

	totalTags := 0
	for _, count := range stats.Counts {
		totalTags += count
	}

	return TaskResult{
		Task:    "tags",
		Variant: string(variant),
		Workers: workers,
		Params: map[string]string{
			"root":      cfg.Root,
			"files":     strconv.Itoa(len(files)),
			"threshold": strconv.Itoa(cfg.Threshold),
		},
		Summary:  fmt.Sprintf("files=%d total_tags=%d unique_tags=%d checksum=%d", stats.Files, totalTags, len(stats.Counts), checksumTagMap(stats.Counts)),
		Duration: time.Since(start),
	}, nil
}

func generateHTMLDataset(cfg htmlDatasetConfig) (htmlDatasetStats, error) {
	if err := os.RemoveAll(cfg.Root); err != nil {
		return htmlDatasetStats{}, err
	}
	if err := os.MkdirAll(cfg.Root, 0o755); err != nil {
		return htmlDatasetStats{}, err
	}

	rng := rand.New(rand.NewSource(cfg.Seed))
	dirs := []string{cfg.Root}
	stats := htmlDatasetStats{Directories: 1}

	for depth := 1; depth <= cfg.MaxDepth; depth++ {
		parentCount := len(dirs)
		for idx := 0; idx < parentCount; idx++ {
			if rng.Intn(100) < 50 {
				dir := filepath.Join(dirs[idx], fmt.Sprintf("level_%d_%d", depth, idx))
				if err := os.MkdirAll(dir, 0o755); err != nil {
					return htmlDatasetStats{}, err
				}
				dirs = append(dirs, dir)
				stats.Directories++
			}
		}
	}

	for fileIndex := 0; fileIndex < cfg.Files; fileIndex++ {
		dir := dirs[rng.Intn(len(dirs))]
		path := filepath.Join(dir, fmt.Sprintf("page_%04d.html", fileIndex))
		content := buildHTMLDocument(rng, fileIndex)
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			return htmlDatasetStats{}, err
		}
		stats.Files++
		stats.Tags += countOpeningTags(content)
	}

	return stats, nil
}

func buildHTMLDocument(rng *rand.Rand, index int) string {
	products := []string{"books", "games", "phones", "music", "travel"}
	words := []string{"parallel", "worker", "channel", "speed", "cache", "signal", "batch", "reduce", "goroutine", "throughput"}
	var builder strings.Builder
	builder.WriteString("<!doctype html><html><head>")
	builder.WriteString(fmt.Sprintf("<title>Document %d</title>", index))
	builder.WriteString("<meta charset=\"utf-8\">")
	builder.WriteString("</head><body>")
	builder.WriteString("<header><nav><ul>")
	for i := 0; i < 3+rng.Intn(4); i++ {
		builder.WriteString(fmt.Sprintf("<li><a href=\"#item-%d\">%s</a></li>", i, products[rng.Intn(len(products))]))
	}
	builder.WriteString("</ul></nav></header>")
	sections := 2 + rng.Intn(5)
	for section := 0; section < sections; section++ {
		builder.WriteString("<section>")
		builder.WriteString(fmt.Sprintf("<h2>Section %d</h2>", section))
		builder.WriteString("<article>")
		paragraphs := 1 + rng.Intn(4)
		for p := 0; p < paragraphs; p++ {
			builder.WriteString("<p>")
			for word := 0; word < 12+rng.Intn(12); word++ {
				if word > 0 {
					builder.WriteByte(' ')
				}
				builder.WriteString(words[rng.Intn(len(words))])
				if rng.Intn(5) == 0 {
					builder.WriteString(" <strong>")
					builder.WriteString(words[rng.Intn(len(words))])
					builder.WriteString("</strong>")
				}
				if rng.Intn(7) == 0 {
					builder.WriteString(" <em>")
					builder.WriteString(words[rng.Intn(len(words))])
					builder.WriteString("</em>")
				}
			}
			builder.WriteString("</p>")
		}
		builder.WriteString("<div class=\"meta\">")
		builder.WriteString(fmt.Sprintf("<span>%s</span>", products[rng.Intn(len(products))]))
		builder.WriteString(fmt.Sprintf("<span>%s</span>", words[rng.Intn(len(words))]))
		builder.WriteString("</div>")
		builder.WriteString("</article></section>")
	}
	builder.WriteString("<footer><small>generated dataset</small></footer>")
	builder.WriteString("</body></html>")
	return builder.String()
}

func collectHTMLFiles(root string) ([]string, error) {
	files := make([]string, 0, 256)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if strings.EqualFold(filepath.Ext(entry.Name()), ".html") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

func countTagsSequential(files []string) (tagStats, error) {
	stats := tagStats{Counts: make(map[string]int)}
	for _, path := range files {
		counts, err := countTagsInFile(path)
		if err != nil {
			return tagStats{}, err
		}
		mergeTagMaps(stats.Counts, counts)
		stats.Files++
	}
	return stats, nil
}

func countTagsMapReduce(files []string, workers int) (tagStats, error) {
	if len(files) == 0 {
		return tagStats{Counts: make(map[string]int)}, nil
	}

	type result struct {
		Files  int
		Counts map[string]int
		Err    error
	}

	ranges := splitIntRange(0, len(files)-1, workers)
	results := make(chan result, len(ranges))
	for _, part := range ranges {
		go func(start, end int) {
			local := tagStats{Counts: make(map[string]int)}
			for _, path := range files[start : end+1] {
				counts, err := countTagsInFile(path)
				if err != nil {
					results <- result{Err: err}
					return
				}
				mergeTagMaps(local.Counts, counts)
				local.Files++
			}
			results <- result{Files: local.Files, Counts: local.Counts}
		}(part[0], part[1])
	}

	stats := tagStats{Counts: make(map[string]int)}
	for range ranges {
		result := <-results
		if result.Err != nil {
			return tagStats{}, result.Err
		}
		stats.Files += result.Files
		mergeTagMaps(stats.Counts, result.Counts)
	}
	return stats, nil
}

func countTagsForkJoin(files []string, workers, threshold int) (tagStats, error) {
	return countTagsForkJoinRange(files, maxInt(workers, 1), threshold)
}

func countTagsForkJoinRange(files []string, workers, threshold int) (tagStats, error) {
	if len(files) == 0 {
		return tagStats{Counts: make(map[string]int)}, nil
	}
	if workers <= 1 || len(files) <= threshold {
		return countTagsSequential(files)
	}

	mid := len(files) / 2
	leftWorkers := workers / 2
	if leftWorkers == 0 {
		leftWorkers = 1
	}
	rightWorkers := workers - leftWorkers
	if rightWorkers == 0 {
		rightWorkers = 1
	}

	type result struct {
		Stats tagStats
		Err   error
	}

	leftCh := make(chan result, 1)
	go func() {
		stats, err := countTagsForkJoinRange(files[:mid], leftWorkers, threshold)
		leftCh <- result{Stats: stats, Err: err}
	}()

	right, err := countTagsForkJoinRange(files[mid:], rightWorkers, threshold)
	if err != nil {
		<-leftCh
		return tagStats{}, err
	}

	left := <-leftCh
	if left.Err != nil {
		return tagStats{}, left.Err
	}

	mergeTagMaps(left.Stats.Counts, right.Counts)
	left.Stats.Files += right.Files
	return left.Stats, nil
}

func countTagsWorkerPool(files []string, workers int) (tagStats, error) {
	type result struct {
		Counts map[string]int
		Err    error
	}

	jobs := make(chan string)
	results := make(chan result, len(files))
	for i := 0; i < workers; i++ {
		go func() {
			for path := range jobs {
				counts, err := countTagsInFile(path)
				results <- result{Counts: counts, Err: err}
			}
		}()
	}

	go func() {
		for _, path := range files {
			jobs <- path
		}
		close(jobs)
	}()

	stats := tagStats{Counts: make(map[string]int)}
	for range files {
		result := <-results
		if result.Err != nil {
			return tagStats{}, result.Err
		}
		stats.Files++
		mergeTagMaps(stats.Counts, result.Counts)
	}
	return stats, nil
}

func countTagsInFile(path string) (map[string]int, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return tokenizeOpeningTags(string(content)), nil
}

func tokenizeOpeningTags(content string) map[string]int {
	counts := make(map[string]int)
	for idx := 0; idx < len(content); idx++ {
		if content[idx] != '<' || idx+1 >= len(content) {
			continue
		}
		next := content[idx+1]
		if next == '/' || next == '!' || next == '?' {
			continue
		}
		start := idx + 1
		end := start
		for end < len(content) {
			ch := content[end]
			if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-' {
				end++
				continue
			}
			break
		}
		if end > start {
			tag := strings.ToLower(content[start:end])
			counts[tag]++
		}
	}
	return counts
}

func countOpeningTags(content string) int {
	total := 0
	for _, count := range tokenizeOpeningTags(content) {
		total += count
	}
	return total
}

func mergeTagMaps(dst, src map[string]int) {
	for key, value := range src {
		dst[key] += value
	}
}

func checksumTagMap(counts map[string]int) int64 {
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var checksum int64
	for _, key := range keys {
		checksum = checksum*131 + checksumString(key) + int64(counts[key])
	}
	return checksum
}
