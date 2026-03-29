package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

type TaskResult struct {
	Task     string
	Variant  string
	Workers  int
	Params   map[string]string
	Summary  string
	Duration time.Duration
}

func main() {
	if err := runCLI(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func runCLI(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		printRootUsage(stdout)
		return nil
	}

	switch args[0] {
	case "run":
		return runCommand(args[1:], stdout, stderr)
	case "-h", "--help", "help":
		printRootUsage(stdout)
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func printRootUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  lab3 run <bank|ipc> [flags]")
}

func runCommand(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("run requires a task name")
	}

	task := args[0]
	variant, workers, taskArgs, err := parseRunCommon(args[1:], stderr)
	if err != nil {
		return err
	}

	result, err := executeTask(task, variant, workers, taskArgs, stderr)
	if err != nil {
		return err
	}

	printRunResult(stdout, result)
	return nil
}

func parseRunCommon(args []string, _ io.Writer) (executionVariant, int, []string, error) {
	variant := executionVariant("")
	workers := 4
	taskArgs := make([]string, 0, len(args))

	for idx := 0; idx < len(args); idx++ {
		arg := args[idx]
		switch {
		case arg == "--variant":
			if idx+1 >= len(args) {
				return "", 0, nil, errors.New("missing value for --variant")
			}
			variant = executionVariant(args[idx+1])
			idx++
		case strings.HasPrefix(arg, "--variant="):
			variant = executionVariant(strings.TrimPrefix(arg, "--variant="))
		case arg == "--workers":
			if idx+1 >= len(args) {
				return "", 0, nil, errors.New("missing value for --workers")
			}
			value, err := strconv.Atoi(args[idx+1])
			if err != nil {
				return "", 0, nil, fmt.Errorf("invalid workers value %q", args[idx+1])
			}
			workers = value
			idx++
		case strings.HasPrefix(arg, "--workers="):
			value, err := strconv.Atoi(strings.TrimPrefix(arg, "--workers="))
			if err != nil {
				return "", 0, nil, fmt.Errorf("invalid workers value %q", arg)
			}
			workers = value
		default:
			taskArgs = append(taskArgs, arg)
		}
	}

	if workers <= 0 {
		return "", 0, nil, errors.New("workers must be > 0")
	}
	if variant == "" {
		variant = variantSeq
	}
	return variant, workers, taskArgs, nil
}

func printRunResult(w io.Writer, result TaskResult) {
	fmt.Fprintf(w, "Task: %s\n", result.Task)
	fmt.Fprintf(w, "Variant: %s\n", result.Variant)
	fmt.Fprintf(w, "Workers: %d\n", result.Workers)
	if len(result.Params) > 0 {
		fmt.Fprintln(w, "Parameters:")
		for _, key := range sortedKeys(result.Params) {
			fmt.Fprintf(w, "  %s=%s\n", key, result.Params[key])
		}
	}
	fmt.Fprintf(w, "Result: %s\n", result.Summary)
	fmt.Fprintf(w, "Duration: %s\n", result.Duration)
}

func executeTask(task string, variant executionVariant, workers int, taskArgs []string, stderr io.Writer) (TaskResult, error) {
	if err := validateVariant(task, variant); err != nil {
		return TaskResult{}, err
	}

	switch task {
	case "bank":
		cfg, err := parseBankFlags(taskArgs, stderr)
		if err != nil {
			return TaskResult{}, err
		}
		return runBankTask(cfg, variant, workers)
	case "ipc":
		cfg, err := parseIPCFlags(taskArgs, stderr)
		if err != nil {
			return TaskResult{}, err
		}
		return runIPCTask(cfg, variant)
	default:
		return TaskResult{}, fmt.Errorf("unknown task %q", task)
	}
}
