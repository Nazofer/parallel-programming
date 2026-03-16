package main

import (
	"fmt"
	"io"
	"strconv"
	"time"
)

type matrixConfig struct {
	Rows int
	Cols int
}

func parseMatrixFlags(args []string, stderr io.Writer) (matrixConfig, error) {
	fs := newFlagSet("matrix", stderr)
	cfg := matrixConfig{}
	fs.IntVar(&cfg.Rows, "rows", 4096, "кількість рядків")
	fs.IntVar(&cfg.Cols, "cols", 4096, "кількість стовпців")
	if err := fs.Parse(args); err != nil {
		return matrixConfig{}, err
	}
	if err := parsePositiveInt("rows", cfg.Rows); err != nil {
		return matrixConfig{}, err
	}
	if err := parsePositiveInt("cols", cfg.Cols); err != nil {
		return matrixConfig{}, err
	}
	return cfg, nil
}

func runMatrixTask(cfg matrixConfig, mode string, workers int) (TaskResult, error) {
	start := time.Now()

	src := buildMatrix(cfg.Rows, cfg.Cols)
	var dst []int32
	if mode == "seq" {
		dst = transposeSequential(src, cfg.Rows, cfg.Cols)
		workers = 1
	} else {
		dst = transposeParallel(src, cfg.Rows, cfg.Cols, workers)
	}

	return TaskResult{
		Task:    "matrix",
		Mode:    mode,
		Workers: workers,
		Params: map[string]string{
			"rows": strconv.Itoa(cfg.Rows),
			"cols": strconv.Itoa(cfg.Cols),
		},
		Summary:  fmt.Sprintf("checksum=%d", checksumInt32(dst)),
		Duration: time.Since(start),
	}, nil
}

func buildMatrix(rows, cols int) []int32 {
	matrix := make([]int32, rows*cols)
	for idx := range matrix {
		matrix[idx] = int32((idx*17 + 11) % 1000)
	}
	return matrix
}

func transposeSequential(src []int32, rows, cols int) []int32 {
	dst := make([]int32, len(src))
	for row := 0; row < rows; row++ {
		for col := 0; col < cols; col++ {
			dst[col*rows+row] = src[row*cols+col]
		}
	}
	return dst
}

func transposeParallel(src []int32, rows, cols, workers int) []int32 {
	dst := make([]int32, len(src))
	ranges := splitIntRange(0, rows-1, workers)
	done := make(chan struct{}, len(ranges))
	for _, part := range ranges {
		go func(startRow, endRow int) {
			for row := startRow; row <= endRow; row++ {
				for col := 0; col < cols; col++ {
					dst[col*rows+row] = src[row*cols+col]
				}
			}
			done <- struct{}{}
		}(part[0], part[1])
	}
	for range ranges {
		<-done
	}
	return dst
}

func checksumInt32(values []int32) int64 {
	var checksum int64
	for _, value := range values {
		checksum += int64(value)
	}
	return checksum
}
