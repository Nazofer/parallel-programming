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

type benchRow struct {
	Variant    string
	Workers    int
	Duration   time.Duration
	Speedup    float64
	Efficiency float64
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
	case "bench":
		return benchCommand(args[1:], stdout, stderr)
	case "-h", "--help", "help":
		printRootUsage(stdout)
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func printRootUsage(w io.Writer) {
	fmt.Fprintln(w, "Commands:")
	fmt.Fprintln(w, "  lab3 run <bank|ipc> [flags]")
	fmt.Fprintln(w, "  lab3 bench <bank|ipc> [flags]")
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

func benchCommand(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("bench requires a task name")
	}

	task := args[0]
	variants, workersList, repeat, taskArgs, err := parseBenchCommon(args[1:], stderr)
	if err != nil {
		return err
	}

	rows, params, err := benchmarkTask(task, variants, workersList, repeat, taskArgs, stderr)
	if err != nil {
		return err
	}

	printBenchResult(stdout, task, params, rows, repeat)
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

func parseBenchCommon(args []string, _ io.Writer) ([]executionVariant, []int, int, []string, error) {
	variantsRaw := ""
	workersRaw := "1,2,4,8"
	repeat := 5
	taskArgs := make([]string, 0, len(args))

	for idx := 0; idx < len(args); idx++ {
		arg := args[idx]
		switch {
		case arg == "--variants":
			if idx+1 >= len(args) {
				return nil, nil, 0, nil, errors.New("missing value for --variants")
			}
			variantsRaw = args[idx+1]
			idx++
		case strings.HasPrefix(arg, "--variants="):
			variantsRaw = strings.TrimPrefix(arg, "--variants=")
		case arg == "--workers":
			if idx+1 >= len(args) {
				return nil, nil, 0, nil, errors.New("missing value for --workers")
			}
			workersRaw = args[idx+1]
			idx++
		case strings.HasPrefix(arg, "--workers="):
			workersRaw = strings.TrimPrefix(arg, "--workers=")
		case arg == "--repeat":
			if idx+1 >= len(args) {
				return nil, nil, 0, nil, errors.New("missing value for --repeat")
			}
			value, err := strconv.Atoi(args[idx+1])
			if err != nil {
				return nil, nil, 0, nil, fmt.Errorf("invalid repeat value %q", args[idx+1])
			}
			repeat = value
			idx++
		case strings.HasPrefix(arg, "--repeat="):
			value, err := strconv.Atoi(strings.TrimPrefix(arg, "--repeat="))
			if err != nil {
				return nil, nil, 0, nil, fmt.Errorf("invalid repeat value %q", arg)
			}
			repeat = value
		default:
			taskArgs = append(taskArgs, arg)
		}
	}

	if repeat <= 0 {
		return nil, nil, 0, nil, errors.New("repeat must be > 0")
	}

	workersList, err := parseWorkersList(workersRaw)
	if err != nil {
		return nil, nil, 0, nil, err
	}

	if strings.TrimSpace(variantsRaw) == "" {
		return nil, workersList, repeat, taskArgs, nil
	}

	variants, err := parseVariantList(variantsRaw)
	if err != nil {
		return nil, nil, 0, nil, err
	}
	return variants, workersList, repeat, taskArgs, nil
}

func benchmarkTask(task string, variants []executionVariant, workersList []int, repeat int, taskArgs []string, stderr io.Writer) ([]benchRow, map[string]string, error) {
	allVariants := variantsForTask(task)
	if len(allVariants) == 0 {
		return nil, nil, fmt.Errorf("unknown task %q", task)
	}
	baseVariant := allVariants[0]

	baseDurations := make([]time.Duration, 0, repeat)
	var baseResult TaskResult
	for i := 0; i < repeat; i++ {
		result, err := executeTask(task, baseVariant, 1, taskArgs, stderr)
		if err != nil {
			return nil, nil, err
		}
		baseResult = result
		baseDurations = append(baseDurations, result.Duration)
	}

	baseMedian := medianDuration(baseDurations)
	rows := []benchRow{{
		Variant:    string(baseVariant),
		Workers:    1,
		Duration:   baseMedian,
		Speedup:    1,
		Efficiency: 1,
	}}

	if variants == nil {
		variants = defaultBenchVariants(task)
	}

	for _, variant := range variants {
		if variant == baseVariant {
			continue
		}
		if err := validateVariant(task, variant); err != nil {
			return nil, nil, err
		}
		for _, workers := range workersList {
			durations := make([]time.Duration, 0, repeat)
			var sample TaskResult
			for i := 0; i < repeat; i++ {
				result, err := executeTask(task, variant, workers, taskArgs, stderr)
				if err != nil {
					return nil, nil, err
				}
				sample = result
				durations = append(durations, result.Duration)
			}
			median := medianDuration(durations)
			speedup := float64(baseMedian) / float64(median)
			rows = append(rows, benchRow{
				Variant:    string(variant),
				Workers:    sample.Workers,
				Duration:   median,
				Speedup:    speedup,
				Efficiency: speedup / float64(maxInt(sample.Workers, 1)),
			})
		}
	}

	return rows, baseResult.Params, nil
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

func printBenchResult(w io.Writer, task string, params map[string]string, rows []benchRow, repeat int) {
	fmt.Fprintf(w, "Task: %s\n", task)
	fmt.Fprintf(w, "Repeat: %d\n", repeat)
	if len(params) > 0 {
		fmt.Fprintln(w, "Parameters:")
		for _, key := range sortedKeys(params) {
			fmt.Fprintf(w, "  %s=%s\n", key, params[key])
		}
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "variant workers median_duration speedup efficiency")
	for _, row := range rows {
		fmt.Fprintf(w, "%-10s %-7d %-15s %-7.3f %.3f\n",
			row.Variant, row.Workers, row.Duration, row.Speedup, row.Efficiency)
	}
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
