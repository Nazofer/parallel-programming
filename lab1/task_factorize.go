package main

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

type factorizeConfig struct {
	Numbers []uint64
}

type factorizationResult struct {
	Number  uint64
	Factors []uint64
}

func parseFactorizeFlags(args []string, stderr io.Writer) (factorizeConfig, error) {
	fs := newFlagSet("factorize", stderr)
	numbersRaw := fs.String("numbers", "", "список чисел через кому")
	if err := fs.Parse(args); err != nil {
		return factorizeConfig{}, err
	}

	cfg := factorizeConfig{}
	if strings.TrimSpace(*numbersRaw) == "" {
		cfg.Numbers = defaultFactorizeNumbers()
		return cfg, nil
	}

	values, err := parseUint64List(*numbersRaw)
	if err != nil {
		return factorizeConfig{}, err
	}
	cfg.Numbers = values
	return cfg, nil
}

func runFactorizeTask(cfg factorizeConfig, mode string, workers int) (TaskResult, error) {
	start := time.Now()

	var results []factorizationResult
	if mode == "seq" {
		results = factorizeSequential(cfg.Numbers)
		workers = 1
	} else {
		results = factorizeParallel(cfg.Numbers, workers)
	}

	totalFactors := 0
	var checksum uint64
	for _, result := range results {
		totalFactors += len(result.Factors)
		for _, factor := range result.Factors {
			checksum += factor
		}
	}

	return TaskResult{
		Task:    "factorize",
		Mode:    mode,
		Workers: workers,
		Params: map[string]string{
			"numbers": strconv.Itoa(len(cfg.Numbers)),
		},
		Summary:  fmt.Sprintf("numbers=%d total_factors=%d checksum=%d", len(results), totalFactors, checksum),
		Duration: time.Since(start),
	}, nil
}

func defaultFactorizeNumbers() []uint64 {
	pairs := [][2]uint64{
		{999983, 1000003},
		{999979, 1000033},
		{999961, 1000037},
		{999953, 1000039},
		{999931, 1000081},
		{999917, 1000099},
		{999907, 1000117},
		{999883, 1000121},
		{999863, 1000133},
		{999853, 1000151},
	}
	numbers := make([]uint64, 0, len(pairs)+4)
	for _, pair := range pairs {
		numbers = append(numbers, pair[0]*pair[1])
	}
	numbers = append(numbers, 600851475143, 32452843*49979687, 982451653, 4294967291)
	return numbers
}

func factorizeSequential(numbers []uint64) []factorizationResult {
	results := make([]factorizationResult, len(numbers))
	for idx, number := range numbers {
		results[idx] = factorizationResult{Number: number, Factors: primeFactors(number)}
	}
	return results
}

func factorizeParallel(numbers []uint64, workers int) []factorizationResult {
	type job struct {
		Index  int
		Number uint64
	}
	type output struct {
		Index  int
		Result factorizationResult
	}

	jobs := make(chan job)
	results := make(chan output, len(numbers))

	for i := 0; i < workers; i++ {
		go func() {
			for job := range jobs {
				results <- output{
					Index:  job.Index,
					Result: factorizationResult{Number: job.Number, Factors: primeFactors(job.Number)},
				}
			}
		}()
	}

	go func() {
		for idx, number := range numbers {
			jobs <- job{Index: idx, Number: number}
		}
		close(jobs)
	}()

	ordered := make([]factorizationResult, len(numbers))
	for i := 0; i < len(numbers); i++ {
		result := <-results
		ordered[result.Index] = result.Result
	}
	return ordered
}

func primeFactors(number uint64) []uint64 {
	if number < 2 {
		return []uint64{number}
	}

	value := number
	factors := make([]uint64, 0, 8)

	for value%2 == 0 {
		factors = append(factors, 2)
		value /= 2
	}

	for divisor := uint64(3); divisor*divisor <= value; divisor += 2 {
		for value%divisor == 0 {
			factors = append(factors, divisor)
			value /= divisor
		}
	}

	if value > 1 {
		factors = append(factors, value)
	}
	return factors
}
