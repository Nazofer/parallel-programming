package main

import (
	"fmt"
	"io"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"time"
)

type statsConfig struct {
	Count     int
	Seed      int64
	Threshold int
}

type numberStats struct {
	Min    int64
	Max    int64
	Median float64
	Mean   float64
}

func parseStatsFlags(args []string, stderr io.Writer) (statsConfig, error) {
	fs := newFlagSet("stats", stderr)
	cfg := statsConfig{}
	fs.IntVar(&cfg.Count, "count", 2_000_000, "number of elements in the array")
	fs.Int64Var(&cfg.Seed, "seed", 42, "deterministic seed")
	fs.IntVar(&cfg.Threshold, "threshold", 8192, "threshold for fork-join")
	if err := fs.Parse(args); err != nil {
		return statsConfig{}, err
	}
	if err := parsePositiveInt("count", cfg.Count); err != nil {
		return statsConfig{}, err
	}
	if err := parsePositiveInt("threshold", cfg.Threshold); err != nil {
		return statsConfig{}, err
	}
	return cfg, nil
}

func runStatsTask(cfg statsConfig, variant executionVariant, workers int) (TaskResult, error) {
	start := time.Now()
	numbers := buildNumbers(cfg.Count, cfg.Seed)

	var stats numberStats
	switch variant {
	case variantSeq:
		stats = calculateNumberStats(numbers)
		workers = 1
	case variantMapReduce:
		stats = calculateNumberStatsMapReduce(numbers, workers)
	case variantForkJoin:
		stats = calculateNumberStatsForkJoin(numbers, workers, cfg.Threshold)
	case variantWorkerPool:
		stats = calculateNumberStatsWorkerPool(numbers, workers)
	default:
		return TaskResult{}, fmt.Errorf("unsupported stats variant %q", variant)
	}

	return TaskResult{
		Task:    "stats",
		Variant: string(variant),
		Workers: workers,
		Params: map[string]string{
			"count":     strconv.Itoa(cfg.Count),
			"seed":      strconv.FormatInt(cfg.Seed, 10),
			"threshold": strconv.Itoa(cfg.Threshold),
		},
		Summary:  fmt.Sprintf("min=%d max=%d median=%.2f mean=%.2f", stats.Min, stats.Max, stats.Median, stats.Mean),
		Duration: time.Since(start),
	}, nil
}

func buildNumbers(count int, seed int64) []int64 {
	rng := rand.New(rand.NewSource(seed))
	numbers := make([]int64, count)
	for idx := range numbers {
		base := rng.Int63n(2_000_001) - 1_000_000
		wave := int64(math.Sin(float64(idx%720))*1000) + int64(idx%17)
		numbers[idx] = base + wave
	}
	return numbers
}

// --- Sequential ---

func calculateNumberStats(numbers []int64) numberStats {
	sorted := sortedCopy(numbers)
	return buildNumberStatsFromSorted(sorted)
}

// --- MapReduce ---

func calculateNumberStatsMapReduce(numbers []int64, workers int) numberStats {
	ranges := splitIntRange(0, len(numbers)-1, workers)
	results := make(chan []int64, len(ranges))
	for _, part := range ranges {
		go func(start, end int) {
			chunk := sortedCopy(numbers[start : end+1])
			results <- chunk
		}(part[0], part[1])
	}

	sorted := <-results
	for i := 1; i < len(ranges); i++ {
		chunk := <-results
		sorted = mergeSorted(sorted, chunk)
	}
	return buildNumberStatsFromSorted(sorted)
}

// --- ForkJoin ---

func calculateNumberStatsForkJoin(numbers []int64, workers, threshold int) numberStats {
	sorted := sortForkJoin(numbers, maxInt(workers, 1), threshold)
	return buildNumberStatsFromSorted(sorted)
}

func sortForkJoin(numbers []int64, workers, threshold int) []int64 {
	if len(numbers) <= threshold || workers <= 1 {
		return sortedCopy(numbers)
	}

	mid := len(numbers) / 2
	leftWorkers := workers / 2
	if leftWorkers == 0 {
		leftWorkers = 1
	}
	rightWorkers := workers - leftWorkers
	if rightWorkers == 0 {
		rightWorkers = 1
	}

	leftCh := make(chan []int64, 1)
	go func() {
		leftCh <- sortForkJoin(numbers[:mid], leftWorkers, threshold)
	}()
	right := sortForkJoin(numbers[mid:], rightWorkers, threshold)
	left := <-leftCh
	return mergeSorted(left, right)
}

// --- WorkerPool ---

func calculateNumberStatsWorkerPool(numbers []int64, workers int) numberStats {
	type job struct {
		Start int
		End   int
	}

	jobs := make(chan job)
	results := make(chan []int64, workers)
	chunkSize := (len(numbers) + workers - 1) / workers
	if chunkSize <= 0 {
		chunkSize = len(numbers)
	}

	for i := 0; i < workers; i++ {
		go func() {
			for j := range jobs {
				results <- sortedCopy(numbers[j.Start:j.End])
			}
		}()
	}

	jobCount := 0
	for start := 0; start < len(numbers); start += chunkSize {
		end := start + chunkSize
		if end > len(numbers) {
			end = len(numbers)
		}
		jobs <- job{Start: start, End: end}
		jobCount++
	}
	close(jobs)

	sorted := <-results
	for i := 1; i < jobCount; i++ {
		chunk := <-results
		sorted = mergeSorted(sorted, chunk)
	}
	return buildNumberStatsFromSorted(sorted)
}

// --- Helpers ---

func sortedCopy(numbers []int64) []int64 {
	cloned := append([]int64(nil), numbers...)
	sort.Slice(cloned, func(i, j int) bool { return cloned[i] < cloned[j] })
	return cloned
}

func mergeSorted(a, b []int64) []int64 {
	result := make([]int64, len(a)+len(b))
	i, j, k := 0, 0, 0
	for i < len(a) && j < len(b) {
		if a[i] <= b[j] {
			result[k] = a[i]
			i++
		} else {
			result[k] = b[j]
			j++
		}
		k++
	}
	copy(result[k:], a[i:])
	copy(result[k+len(a)-i:], b[j:])
	return result
}

func buildNumberStatsFromSorted(sorted []int64) numberStats {
	n := len(sorted)
	var sum int64
	for _, v := range sorted {
		sum += v
	}
	mid := n / 2
	var median float64
	if n%2 == 1 {
		median = float64(sorted[mid])
	} else {
		median = float64(sorted[mid-1]+sorted[mid]) / 2
	}
	return numberStats{
		Min:    sorted[0],
		Max:    sorted[n-1],
		Median: median,
		Mean:   float64(sum) / float64(n),
	}
}
