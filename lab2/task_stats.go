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

type aggregateStats struct {
	Min int64
	Max int64
	Sum int64
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

func calculateNumberStats(numbers []int64) numberStats {
	aggregate := aggregateSequential(numbers)
	return buildNumberStats(numbers, aggregate)
}

func calculateNumberStatsMapReduce(numbers []int64, workers int) numberStats {
	ranges := splitIntRange(0, len(numbers)-1, workers)
	results := make(chan aggregateStats, len(ranges))
	for _, part := range ranges {
		go func(start, end int) {
			results <- aggregateSequential(numbers[start : end+1])
		}(part[0], part[1])
	}

	aggregate := aggregateStats{}
	for idx := range ranges {
		part := <-results
		if idx == 0 {
			aggregate = part
			continue
		}
		aggregate = mergeAggregateStats(aggregate, part)
	}
	return buildNumberStats(numbers, aggregate)
}

func calculateNumberStatsForkJoin(numbers []int64, workers, threshold int) numberStats {
	aggregate := aggregateForkJoin(numbers, maxInt(workers, 1), threshold)
	return buildNumberStats(numbers, aggregate)
}

func calculateNumberStatsWorkerPool(numbers []int64, workers int) numberStats {
	type job struct {
		Start int
		End   int
	}

	jobs := make(chan job)
	results := make(chan aggregateStats, workers)
	chunkSize := (len(numbers) + workers - 1) / workers
	if chunkSize <= 0 {
		chunkSize = len(numbers)
	}

	for i := 0; i < workers; i++ {
		go func() {
			for job := range jobs {
				results <- aggregateSequential(numbers[job.Start:job.End])
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

	aggregate := aggregateStats{}
	for idx := 0; idx < jobCount; idx++ {
		part := <-results
		if idx == 0 {
			aggregate = part
			continue
		}
		aggregate = mergeAggregateStats(aggregate, part)
	}
	return buildNumberStats(numbers, aggregate)
}

func aggregateSequential(numbers []int64) aggregateStats {
	stats := aggregateStats{Min: numbers[0], Max: numbers[0]}
	for _, value := range numbers {
		if value < stats.Min {
			stats.Min = value
		}
		if value > stats.Max {
			stats.Max = value
		}
		stats.Sum += value
	}
	return stats
}

func aggregateForkJoin(numbers []int64, workers, threshold int) aggregateStats {
	if len(numbers) <= threshold || workers <= 1 {
		return aggregateSequential(numbers)
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

	leftCh := make(chan aggregateStats, 1)
	go func() {
		leftCh <- aggregateForkJoin(numbers[:mid], leftWorkers, threshold)
	}()
	right := aggregateForkJoin(numbers[mid:], rightWorkers, threshold)
	left := <-leftCh
	return mergeAggregateStats(left, right)
}

func mergeAggregateStats(left, right aggregateStats) aggregateStats {
	if right.Min < left.Min {
		left.Min = right.Min
	}
	if right.Max > left.Max {
		left.Max = right.Max
	}
	left.Sum += right.Sum
	return left
}

func buildNumberStats(numbers []int64, aggregate aggregateStats) numberStats {
	return numberStats{
		Min:    aggregate.Min,
		Max:    aggregate.Max,
		Median: calculateMedian(numbers),
		Mean:   float64(aggregate.Sum) / float64(len(numbers)),
	}
}

func calculateMedian(numbers []int64) float64 {
	cloned := append([]int64(nil), numbers...)
	sort.Slice(cloned, func(i, j int) bool { return cloned[i] < cloned[j] })
	mid := len(cloned) / 2
	if len(cloned)%2 == 1 {
		return float64(cloned[mid])
	}
	return float64(cloned[mid-1]+cloned[mid]) / 2
}
