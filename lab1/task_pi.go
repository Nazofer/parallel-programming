package main

import (
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"time"
)

type piConfig struct {
	Iterations int64
	Seed       uint64
}

const piSeedSalt uint64 = 0x9e3779b97f4a7c15

func parsePiFlags(args []string, stderr io.Writer) (piConfig, error) {
	fs := newFlagSet("pi", stderr)
	cfg := piConfig{}
	fs.Int64Var(&cfg.Iterations, "iterations", 10_000_000, "кількість ітерацій Monte-Carlo")
	fs.Uint64Var(&cfg.Seed, "seed", 42, "детермінований seed")
	if err := fs.Parse(args); err != nil {
		return piConfig{}, err
	}
	if err := parsePositiveInt64("iterations", cfg.Iterations); err != nil {
		return piConfig{}, err
	}
	return cfg, nil
}

func runPiTask(cfg piConfig, mode string, workers int) (TaskResult, error) {
	start := time.Now()

	var inside int64
	if mode == "seq" {
		inside = countPiInside(0, cfg.Iterations, cfg.Seed)
		workers = 1
	} else {
		inside = countPiInsideParallel(cfg.Iterations, workers, cfg.Seed)
	}

	pi := 4 * float64(inside) / float64(cfg.Iterations)
	return TaskResult{
		Task:    "pi",
		Mode:    mode,
		Workers: workers,
		Params: map[string]string{
			"iterations": strconv.FormatInt(cfg.Iterations, 10),
			"seed":       strconv.FormatUint(cfg.Seed, 10),
		},
		Summary:  fmt.Sprintf("pi=%.8f inside=%d", pi, inside),
		Duration: time.Since(start),
	}, nil
}

func countPiInsideParallel(iterations int64, workers int, seed uint64) int64 {
	ranges := splitEvenly(iterations, workers)
	results := make(chan int64, len(ranges))
	for _, part := range ranges {
		go func(start, end int64) {
			results <- countPiInside(start, end, seed)
		}(part[0], part[1])
	}

	var inside int64
	for range ranges {
		inside += <-results
	}
	return inside
}

func countPiInside(start, end int64, seed uint64) int64 {
	var inside int64
	for idx := start; idx < end; idx++ {
		x, y := deterministicPoint(uint64(idx), seed)
		if x*x+y*y <= 1 {
			inside++
		}
	}
	return inside
}

func deterministicPoint(index, seed uint64) (float64, float64) {
	r := rand.New(rand.NewSource(int64(index ^ seed ^ piSeedSalt)))
	return r.Float64(), r.Float64()
}
