package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

type TaskResult struct {
	Task     string
	Mode     string
	Workers  int
	Params   map[string]string
	Summary  string
	Duration time.Duration
}

type benchRow struct {
	Mode       string
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
	case "generate":
		return generateCommand(args[1:], stdout, stderr)
	case "-h", "--help", "help":
		printRootUsage(stdout)
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func printRootUsage(w io.Writer) {
	fmt.Fprintln(w, "Команди:")
	fmt.Fprintln(w, "  lab1 run <pi|factorize|primes|matrix|words> [flags]")
	fmt.Fprintln(w, "  lab1 bench <pi|factorize|primes|matrix|words> [flags]")
	fmt.Fprintln(w, "  lab1 generate words-dataset [flags]")
}

func runCommand(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("run requires a task name")
	}

	task := args[0]
	mode, workers, taskArgs, err := parseRunCommon(args[1:], stderr)
	if err != nil {
		return err
	}

	result, err := executeTask(task, mode, workers, taskArgs, stderr)
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
	workersList, repeat, taskArgs, err := parseBenchCommon(args[1:], stderr)
	if err != nil {
		return err
	}

	rows, params, err := benchmarkTask(task, workersList, repeat, taskArgs, stderr)
	if err != nil {
		return err
	}

	printBenchResult(stdout, task, params, rows, repeat)
	return nil
}

func generateCommand(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("generate requires a target name")
	}
	if args[0] != "words-dataset" {
		return fmt.Errorf("unknown generate target %q", args[0])
	}

	cfg, err := parseWordsDatasetFlags(args[1:], stderr)
	if err != nil {
		return err
	}

	stats, err := generateWordsDataset(cfg)
	if err != nil {
		return err
	}

	fmt.Fprintln(stdout, "Words dataset generated")
	fmt.Fprintf(stdout, "Root: %s\n", cfg.Root)
	fmt.Fprintf(stdout, "Directories: %d\n", stats.Directories)
	fmt.Fprintf(stdout, "Files: %d\n", stats.Files)
	fmt.Fprintf(stdout, "Words: %d\n", stats.Words)
	return nil
}

func parseRunCommon(args []string, _ io.Writer) (string, int, []string, error) {
	mode := "seq"
	workers := 4
	taskArgs := make([]string, 0, len(args))

	for idx := 0; idx < len(args); idx++ {
		arg := args[idx]
		switch {
		case arg == "--mode":
			if idx+1 >= len(args) {
				return "", 0, nil, errors.New("missing value for --mode")
			}
			mode = args[idx+1]
			idx++
		case strings.HasPrefix(arg, "--mode="):
			mode = strings.TrimPrefix(arg, "--mode=")
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

	if mode != "seq" && mode != "par" {
		return "", 0, nil, fmt.Errorf("invalid mode %q", mode)
	}
	if workers <= 0 {
		return "", 0, nil, errors.New("workers must be > 0")
	}

	return mode, workers, taskArgs, nil
}

func parseBenchCommon(args []string, _ io.Writer) ([]int, int, []string, error) {
	workersRaw := "1,2,4,8"
	repeat := 5
	taskArgs := make([]string, 0, len(args))

	for idx := 0; idx < len(args); idx++ {
		arg := args[idx]
		switch {
		case arg == "--workers":
			if idx+1 >= len(args) {
				return nil, 0, nil, errors.New("missing value for --workers")
			}
			workersRaw = args[idx+1]
			idx++
		case strings.HasPrefix(arg, "--workers="):
			workersRaw = strings.TrimPrefix(arg, "--workers=")
		case arg == "--repeat":
			if idx+1 >= len(args) {
				return nil, 0, nil, errors.New("missing value for --repeat")
			}
			value, err := strconv.Atoi(args[idx+1])
			if err != nil {
				return nil, 0, nil, fmt.Errorf("invalid repeat value %q", args[idx+1])
			}
			repeat = value
			idx++
		case strings.HasPrefix(arg, "--repeat="):
			value, err := strconv.Atoi(strings.TrimPrefix(arg, "--repeat="))
			if err != nil {
				return nil, 0, nil, fmt.Errorf("invalid repeat value %q", arg)
			}
			repeat = value
		default:
			taskArgs = append(taskArgs, arg)
		}
	}

	if repeat <= 0 {
		return nil, 0, nil, errors.New("repeat must be > 0")
	}

	workersList, err := parseWorkersList(workersRaw)
	if err != nil {
		return nil, 0, nil, err
	}
	return workersList, repeat, taskArgs, nil
}

func parseWorkersList(raw string) ([]int, error) {
	parts := strings.Split(raw, ",")
	workers := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		value, err := strconv.Atoi(part)
		if err != nil || value <= 0 {
			return nil, fmt.Errorf("invalid workers value %q", part)
		}
		workers = append(workers, value)
	}
	if len(workers) == 0 {
		return nil, errors.New("workers list cannot be empty")
	}
	sort.Ints(workers)
	uniq := workers[:0]
	for _, value := range workers {
		if len(uniq) == 0 || uniq[len(uniq)-1] != value {
			uniq = append(uniq, value)
		}
	}
	return uniq, nil
}

func benchmarkTask(task string, workersList []int, repeat int, taskArgs []string, stderr io.Writer) ([]benchRow, map[string]string, error) {
	seqDurations := make([]time.Duration, 0, repeat)
	var seqResult TaskResult

	for i := 0; i < repeat; i++ {
		result, err := executeTask(task, "seq", 1, taskArgs, stderr)
		if err != nil {
			return nil, nil, err
		}
		seqResult = result
		seqDurations = append(seqDurations, result.Duration)
	}

	seqMedian := medianDuration(seqDurations)
	rows := []benchRow{{
		Mode:       "seq",
		Workers:    1,
		Duration:   seqMedian,
		Speedup:    1,
		Efficiency: 1,
	}}

	for _, workers := range workersList {
		durations := make([]time.Duration, 0, repeat)
		for i := 0; i < repeat; i++ {
			result, err := executeTask(task, "par", workers, taskArgs, stderr)
			if err != nil {
				return nil, nil, err
			}
			durations = append(durations, result.Duration)
		}
		median := medianDuration(durations)
		speedup := float64(seqMedian) / float64(median)
		rows = append(rows, benchRow{
			Mode:       "par",
			Workers:    workers,
			Duration:   median,
			Speedup:    speedup,
			Efficiency: speedup / float64(workers),
		})
	}

	return rows, seqResult.Params, nil
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

func printRunResult(w io.Writer, result TaskResult) {
	fmt.Fprintf(w, "Task: %s\n", result.Task)
	fmt.Fprintf(w, "Mode: %s\n", result.Mode)
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
	fmt.Fprintln(w, "mode workers median_duration speedup efficiency")
	for _, row := range rows {
		fmt.Fprintf(w, "%-4s %-7d %-15s %-7.3f %.3f\n",
			row.Mode, row.Workers, row.Duration, row.Speedup, row.Efficiency)
	}
}

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func executeTask(task, mode string, workers int, taskArgs []string, stderr io.Writer) (TaskResult, error) {
	restore := runtime.GOMAXPROCS(1)
	if mode == "par" {
		runtime.GOMAXPROCS(workers)
	}
	defer runtime.GOMAXPROCS(restore)

	switch task {
	case "pi":
		cfg, err := parsePiFlags(taskArgs, stderr)
		if err != nil {
			return TaskResult{}, err
		}
		return runPiTask(cfg, mode, workers)
	case "factorize":
		cfg, err := parseFactorizeFlags(taskArgs, stderr)
		if err != nil {
			return TaskResult{}, err
		}
		return runFactorizeTask(cfg, mode, workers)
	case "primes":
		cfg, err := parsePrimesFlags(taskArgs, stderr)
		if err != nil {
			return TaskResult{}, err
		}
		return runPrimesTask(cfg, mode, workers)
	case "matrix":
		cfg, err := parseMatrixFlags(taskArgs, stderr)
		if err != nil {
			return TaskResult{}, err
		}
		return runMatrixTask(cfg, mode, workers)
	case "words":
		cfg, err := parseWordsFlags(taskArgs, stderr)
		if err != nil {
			return TaskResult{}, err
		}
		return runWordsTask(cfg, mode, workers)
	default:
		return TaskResult{}, fmt.Errorf("unknown task %q", task)
	}
}

func parseUint64List(raw string) ([]uint64, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("numbers list cannot be empty")
	}
	parts := strings.Split(raw, ",")
	values := make([]uint64, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		value, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid uint64 value %q", part)
		}
		values = append(values, value)
	}
	return values, nil
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
