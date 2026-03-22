package main

import (
	"flag"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"
)

type executionVariant string

const (
	variantSeq        executionVariant = "seq"
	variantMapReduce  executionVariant = "mapreduce"
	variantForkJoin   executionVariant = "forkjoin"
	variantWorkerPool executionVariant = "workerpool"
	variantPipeline   executionVariant = "pipeline"
	variantProdCon    executionVariant = "prodcon"
)

func splitEvenly(total int64, workers int) [][2]int64 {
	if workers <= 1 || total <= 1 {
		return [][2]int64{{0, total}}
	}
	if int64(workers) > total {
		workers = int(total)
	}

	parts := make([][2]int64, 0, workers)
	base := total / int64(workers)
	extra := total % int64(workers)
	var start int64
	for idx := 0; idx < workers; idx++ {
		size := base
		if int64(idx) < extra {
			size++
		}
		end := start + size
		parts = append(parts, [2]int64{start, end})
		start = end
	}
	return parts
}

func splitIntRange(start, end, workers int) [][2]int {
	if end < start {
		return [][2]int{}
	}
	total := end - start + 1
	if workers <= 1 || total <= 1 {
		return [][2]int{{start, end}}
	}
	if workers > total {
		workers = total
	}

	base := total / workers
	extra := total % workers
	parts := make([][2]int, 0, workers)
	current := start
	for idx := 0; idx < workers; idx++ {
		size := base
		if idx < extra {
			size++
		}
		partEnd := current + size - 1
		parts = append(parts, [2]int{current, partEnd})
		current = partEnd + 1
	}
	return parts
}

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func parsePositiveInt(name string, value int) error {
	if value <= 0 {
		return fmt.Errorf("%s must be > 0", name)
	}
	return nil
}

func parsePositiveInt64(name string, value int64) error {
	if value <= 0 {
		return fmt.Errorf("%s must be > 0", name)
	}
	return nil
}

func newFlagSet(name string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	return fs
}

func parseWorkersList(raw string) ([]int, error) {
	parts := strings.Split(raw, ",")
	values := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		value, err := strconv.Atoi(part)
		if err != nil || value <= 0 {
			return nil, fmt.Errorf("invalid workers value %q", part)
		}
		values = append(values, value)
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("workers list cannot be empty")
	}
	sort.Ints(values)
	uniq := values[:0]
	for _, value := range values {
		if len(uniq) == 0 || uniq[len(uniq)-1] != value {
			uniq = append(uniq, value)
		}
	}
	return uniq, nil
}

func parseVariantList(raw string) ([]executionVariant, error) {
	parts := strings.Split(raw, ",")
	variants := make([]executionVariant, 0, len(parts))
	seen := make(map[executionVariant]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		variant := executionVariant(part)
		if _, ok := seen[variant]; ok {
			continue
		}
		seen[variant] = struct{}{}
		variants = append(variants, variant)
	}
	if len(variants) == 0 {
		return nil, fmt.Errorf("variants list cannot be empty")
	}
	return variants, nil
}

func medianDuration(values []time.Duration) time.Duration {
	cloned := append([]time.Duration(nil), values...)
	sort.Slice(cloned, func(i, j int) bool { return cloned[i] < cloned[j] })
	mid := len(cloned) / 2
	if len(cloned)%2 == 1 {
		return cloned[mid]
	}
	return (cloned[mid-1] + cloned[mid]) / 2
}

func variantsForTask(task string) []executionVariant {
	switch task {
	case "tags", "stats", "matmul":
		return []executionVariant{variantSeq, variantMapReduce, variantForkJoin, variantWorkerPool}
	case "transactions":
		return []executionVariant{variantSeq, variantPipeline, variantProdCon}
	default:
		return nil
	}
}

func defaultBenchVariants(task string) []executionVariant {
	all := variantsForTask(task)
	if len(all) <= 1 {
		return nil
	}
	return append([]executionVariant(nil), all[1:]...)
}

func validateVariant(task string, variant executionVariant) error {
	for _, candidate := range variantsForTask(task) {
		if candidate == variant {
			return nil
		}
	}
	return fmt.Errorf("variant %q is not supported for task %q", variant, task)
}

func checksumString(value string) int64 {
	var checksum int64
	for _, ch := range value {
		checksum = checksum*131 + int64(ch)
	}
	return checksum
}

func formatMoney(kopecks int64) string {
	sign := ""
	value := kopecks
	if value < 0 {
		sign = "-"
		value = -value
	}
	return fmt.Sprintf("%s%d.%02d", sign, value/100, value%100)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
