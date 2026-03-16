package main

import (
	"fmt"
	"io"
	"strconv"
	"time"
)

type primesConfig struct {
	From int
	To   int
}

type primeStats struct {
	Count int
	Sum   uint64
	Last  int
}

func parsePrimesFlags(args []string, stderr io.Writer) (primesConfig, error) {
	fs := newFlagSet("primes", stderr)
	cfg := primesConfig{}
	fs.IntVar(&cfg.From, "from", 2, "початок діапазону")
	fs.IntVar(&cfg.To, "to", 5_000_000, "кінець діапазону")
	if err := fs.Parse(args); err != nil {
		return primesConfig{}, err
	}
	if cfg.From < 0 {
		return primesConfig{}, fmt.Errorf("from must be >= 0")
	}
	if cfg.To < cfg.From {
		return primesConfig{}, fmt.Errorf("to must be >= from")
	}
	return cfg, nil
}

func runPrimesTask(cfg primesConfig, mode string, workers int) (TaskResult, error) {
	start := time.Now()

	var stats primeStats
	if mode == "seq" {
		stats = countPrimesSequential(cfg.From, cfg.To)
		workers = 1
	} else {
		stats = countPrimesParallel(cfg.From, cfg.To, workers)
	}

	return TaskResult{
		Task:    "primes",
		Mode:    mode,
		Workers: workers,
		Params: map[string]string{
			"from": strconv.Itoa(cfg.From),
			"to":   strconv.Itoa(cfg.To),
		},
		Summary:  fmt.Sprintf("count=%d last=%d sum=%d", stats.Count, stats.Last, stats.Sum),
		Duration: time.Since(start),
	}, nil
}

func countPrimesSequential(from, to int) primeStats {
	stats := primeStats{}
	for candidate := maxInt(from, 2); candidate <= to; candidate++ {
		if isPrime(candidate) {
			stats.Count++
			stats.Sum += uint64(candidate)
			stats.Last = candidate
		}
	}
	return stats
}

func countPrimesParallel(from, to, workers int) primeStats {
	ranges := splitIntRange(maxInt(from, 2), to, workers)
	results := make(chan primeStats, len(ranges))
	for _, part := range ranges {
		go func(start, end int) {
			results <- countPrimesSequential(start, end)
		}(part[0], part[1])
	}

	total := primeStats{}
	for range ranges {
		part := <-results
		total.Count += part.Count
		total.Sum += part.Sum
		if part.Last > total.Last {
			total.Last = part.Last
		}
	}
	return total
}

func collectPrimes(from, to int) []int {
	stats := make([]int, 0)
	for candidate := maxInt(from, 2); candidate <= to; candidate++ {
		if isPrime(candidate) {
			stats = append(stats, candidate)
		}
	}
	return stats
}

func isPrime(value int) bool {
	if value < 2 {
		return false
	}
	if value == 2 {
		return true
	}
	if value%2 == 0 {
		return false
	}
	for divisor := 3; divisor*divisor <= value; divisor += 2 {
		if value%divisor == 0 {
			return false
		}
	}
	return true
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
