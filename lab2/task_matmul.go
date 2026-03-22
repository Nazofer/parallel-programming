package main

import (
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"time"
)

type matmulConfig struct {
	Size      int
	Seed      int64
	Threshold int
}

func parseMatmulFlags(args []string, stderr io.Writer) (matmulConfig, error) {
	fs := newFlagSet("matmul", stderr)
	cfg := matmulConfig{}
	fs.IntVar(&cfg.Size, "size", 256, "size of square matrices")
	fs.Int64Var(&cfg.Seed, "seed", 42, "deterministic seed")
	fs.IntVar(&cfg.Threshold, "threshold", 16, "row count threshold for fork-join")
	if err := fs.Parse(args); err != nil {
		return matmulConfig{}, err
	}
	if err := parsePositiveInt("size", cfg.Size); err != nil {
		return matmulConfig{}, err
	}
	if err := parsePositiveInt("threshold", cfg.Threshold); err != nil {
		return matmulConfig{}, err
	}
	return cfg, nil
}

func runMatmulTask(cfg matmulConfig, variant executionVariant, workers int) (TaskResult, error) {
	start := time.Now()
	left := buildMatrix(cfg.Size, cfg.Seed)
	right := buildMatrix(cfg.Size, cfg.Seed+1_000)

	var result []int64
	switch variant {
	case variantSeq:
		result = multiplySequential(left, right, cfg.Size)
		workers = 1
	case variantMapReduce:
		result = multiplyMapReduce(left, right, cfg.Size, workers)
	case variantForkJoin:
		result = multiplyForkJoin(left, right, cfg.Size, workers, cfg.Threshold)
	case variantWorkerPool:
		result = multiplyWorkerPool(left, right, cfg.Size, workers)
	default:
		return TaskResult{}, fmt.Errorf("unsupported matmul variant %q", variant)
	}

	return TaskResult{
		Task:    "matmul",
		Variant: string(variant),
		Workers: workers,
		Params: map[string]string{
			"size":      strconv.Itoa(cfg.Size),
			"seed":      strconv.FormatInt(cfg.Seed, 10),
			"threshold": strconv.Itoa(cfg.Threshold),
		},
		Summary:  fmt.Sprintf("size=%d checksum=%d", cfg.Size, checksumInt64Slice(result)),
		Duration: time.Since(start),
	}, nil
}

func buildMatrix(size int, seed int64) []int64 {
	rng := rand.New(rand.NewSource(seed))
	matrix := make([]int64, size*size)
	for idx := range matrix {
		matrix[idx] = int64(rng.Intn(17) - 8)
	}
	return matrix
}

func multiplySequential(left, right []int64, size int) []int64 {
	result := make([]int64, size*size)
	multiplyRowsInto(left, right, result, size, 0, size)
	return result
}

func multiplyMapReduce(left, right []int64, size, workers int) []int64 {
	type block struct {
		StartRow int
		Values   []int64
	}

	ranges := splitIntRange(0, size-1, workers)
	blocks := make(chan block, len(ranges))
	for _, part := range ranges {
		go func(startRow, endRow int) {
			height := endRow - startRow + 1
			values := make([]int64, height*size)
			multiplyRowsInto(left, right, values, size, startRow, endRow+1)
			blocks <- block{StartRow: startRow, Values: values}
		}(part[0], part[1])
	}

	result := make([]int64, size*size)
	for range ranges {
		block := <-blocks
		copy(result[block.StartRow*size:], block.Values)
	}
	return result
}

func multiplyForkJoin(left, right []int64, size, workers, threshold int) []int64 {
	result := make([]int64, size*size)
	multiplyForkJoinInto(left, right, result, size, 0, size, maxInt(workers, 1), threshold)
	return result
}

func multiplyForkJoinInto(left, right, result []int64, size, startRow, endRow, workers, threshold int) {
	rowCount := endRow - startRow
	if workers <= 1 || rowCount <= threshold {
		multiplyRowsInto(left, right, result, size, startRow, endRow)
		return
	}

	mid := startRow + rowCount/2
	leftWorkers := workers / 2
	if leftWorkers == 0 {
		leftWorkers = 1
	}
	rightWorkers := workers - leftWorkers
	if rightWorkers == 0 {
		rightWorkers = 1
	}

	done := make(chan struct{}, 1)
	go func() {
		multiplyForkJoinInto(left, right, result, size, startRow, mid, leftWorkers, threshold)
		done <- struct{}{}
	}()
	multiplyForkJoinInto(left, right, result, size, mid, endRow, rightWorkers, threshold)
	<-done
}

func multiplyWorkerPool(left, right []int64, size, workers int) []int64 {
	type job struct {
		StartRow int
		EndRow   int
	}

	jobs := make(chan job)
	done := make(chan struct{}, workers)
	result := make([]int64, size*size)
	chunkRows := maxInt(1, size/(workers*2))

	for i := 0; i < workers; i++ {
		go func() {
			for job := range jobs {
				multiplyRowsInto(left, right, result, size, job.StartRow, job.EndRow)
				done <- struct{}{}
			}
		}()
	}

	jobCount := 0
	for start := 0; start < size; start += chunkRows {
		end := start + chunkRows
		if end > size {
			end = size
		}
		jobs <- job{StartRow: start, EndRow: end}
		jobCount++
	}
	close(jobs)

	for i := 0; i < jobCount; i++ {
		<-done
	}
	return result
}

func multiplyRowsInto(left, right, target []int64, size, startRow, endRow int) {
	for row := startRow; row < endRow; row++ {
		rowOffset := row * size
		targetOffset := rowOffset
		if len(target) != size*size {
			targetOffset = (row - startRow) * size
		}
		for k := 0; k < size; k++ {
			leftValue := left[rowOffset+k]
			rightOffset := k * size
			for col := 0; col < size; col++ {
				target[targetOffset+col] += leftValue * right[rightOffset+col]
			}
		}
	}
}

func checksumInt64Slice(values []int64) int64 {
	var checksum int64
	for _, value := range values {
		checksum = checksum*131 + value
	}
	return checksum
}
