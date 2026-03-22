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
	variantSeq      executionVariant = "seq"
	variantUnsafe   executionVariant = "unsafe"
	variantDeadlock executionVariant = "deadlock"
	variantFixed    executionVariant = "fixed"
	variantPipe     executionVariant = "pipe"
	variantSocket   executionVariant = "socket"
	variantMmap     executionVariant = "mmap"
)

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
	case "bank":
		return []executionVariant{variantSeq, variantUnsafe, variantDeadlock, variantFixed}
	case "ipc":
		return []executionVariant{variantPipe, variantSocket, variantMmap}
	default:
		return nil
	}
}

func defaultBenchVariants(task string) []executionVariant {
	switch task {
	case "bank":
		return []executionVariant{variantFixed}
	case "ipc":
		return []executionVariant{variantSocket, variantMmap}
	default:
		return nil
	}
}

func validateVariant(task string, variant executionVariant) error {
	for _, candidate := range variantsForTask(task) {
		if candidate == variant {
			return nil
		}
	}
	return fmt.Errorf("variant %q is not supported for task %q", variant, task)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
