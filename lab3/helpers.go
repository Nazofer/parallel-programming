package main

import (
	"flag"
	"fmt"
	"io"
	"sort"
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

func validateVariant(task string, variant executionVariant) error {
	for _, candidate := range variantsForTask(task) {
		if candidate == variant {
			return nil
		}
	}
	return fmt.Errorf("variant %q is not supported for task %q", variant, task)
}

