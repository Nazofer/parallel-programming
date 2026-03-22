package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const defaultBatchSize = 512

type transactionsConfig struct {
	Input            string
	Buffer           int
	BatchSize        int
	ReadWorkers      int
	TransformWorkers int
}

type transactionsDatasetConfig struct {
	Root    string
	Records int
	Seed    int64
}

type transactionsDatasetStats struct {
	Files   int
	Records int
}

type Transaction struct {
	UserID      string
	AmountCents int64
	Currency    string
	Date        string
	ProductType string
	UserStatus  string
}

type convertedTransaction struct {
	Tx             Transaction
	GrossUAHKopeck int64
}

type processedTransaction struct {
	UserID             string
	GrossUAHKopecks    int64
	CashbackUAHKopecks int64
	NetUAHKopecks      int64
}

type transactionStats struct {
	Records       int
	GrossUAH      int64
	CashbackUAH   int64
	NetUAH        int64
	ChecksumValue int64
}

func parseTransactionsFlags(args []string, stderr io.Writer) (transactionsConfig, error) {
	fs := newFlagSet("transactions", stderr)
	cfg := transactionsConfig{}
	fs.StringVar(&cfg.Input, "input", "./tmp/transactions-dataset/transactions.csv", "path to transactions csv")
	fs.IntVar(&cfg.Buffer, "buffer", 0, "channel buffer size (in batches)")
	fs.IntVar(&cfg.BatchSize, "batch", 0, "records per batch")
	fs.IntVar(&cfg.ReadWorkers, "read-workers", 0, "number of workers in parse stage pipeline")
	fs.IntVar(&cfg.TransformWorkers, "transform-workers", 0, "number of workers in transform stages pipeline")
	if err := fs.Parse(args); err != nil {
		return transactionsConfig{}, err
	}
	if strings.TrimSpace(cfg.Input) == "" {
		return transactionsConfig{}, fmt.Errorf("input cannot be empty")
	}
	if cfg.Buffer < 0 {
		return transactionsConfig{}, fmt.Errorf("buffer must be >= 0")
	}
	if cfg.BatchSize < 0 {
		return transactionsConfig{}, fmt.Errorf("batch must be >= 0")
	}
	if cfg.ReadWorkers < 0 {
		return transactionsConfig{}, fmt.Errorf("read-workers must be >= 0")
	}
	if cfg.TransformWorkers < 0 {
		return transactionsConfig{}, fmt.Errorf("transform-workers must be >= 0")
	}
	return cfg, nil
}

func parseTransactionsDatasetFlags(args []string, stderr io.Writer) (transactionsDatasetConfig, error) {
	fs := newFlagSet("transactions-dataset", stderr)
	cfg := transactionsDatasetConfig{}
	fs.StringVar(&cfg.Root, "root", "./tmp/transactions-dataset", "output directory for the dataset")
	fs.IntVar(&cfg.Records, "records", 200_000, "number of transactions")
	fs.Int64Var(&cfg.Seed, "seed", 42, "deterministic seed")
	if err := fs.Parse(args); err != nil {
		return transactionsDatasetConfig{}, err
	}
	if strings.TrimSpace(cfg.Root) == "" {
		return transactionsDatasetConfig{}, fmt.Errorf("root cannot be empty")
	}
	if err := parsePositiveInt("records", cfg.Records); err != nil {
		return transactionsDatasetConfig{}, err
	}
	return cfg, nil
}

func runTransactionsTask(cfg transactionsConfig, variant executionVariant, workers int) (TaskResult, error) {
	start := time.Now()
	cfg = normalizeTransactionsConfig(cfg, workers)

	var stats transactionStats
	var err error
	switch variant {
	case variantSeq:
		stats, err = processTransactionsSequential(cfg.Input)
		workers = 1
	case variantPipeline:
		stats, err = processTransactionsPipeline(cfg)
	case variantProdCon:
		stats, err = processTransactionsProducerConsumer(cfg, workers)
	default:
		return TaskResult{}, fmt.Errorf("unsupported transactions variant %q", variant)
	}
	if err != nil {
		return TaskResult{}, err
	}

	return TaskResult{
		Task:    "transactions",
		Variant: string(variant),
		Workers: workers,
		Params: map[string]string{
			"input":             cfg.Input,
			"buffer":            strconv.Itoa(cfg.Buffer),
			"batch":             strconv.Itoa(cfg.BatchSize),
			"read_workers":      strconv.Itoa(cfg.ReadWorkers),
			"transform_workers": strconv.Itoa(cfg.TransformWorkers),
		},
		Summary: fmt.Sprintf(
			"records=%d gross_uah=%s cashback_uah=%s net_uah=%s checksum=%d",
			stats.Records,
			formatMoney(stats.GrossUAH),
			formatMoney(stats.CashbackUAH),
			formatMoney(stats.NetUAH),
			stats.ChecksumValue,
		),
		Duration: time.Since(start),
	}, nil
}

func normalizeTransactionsConfig(cfg transactionsConfig, workers int) transactionsConfig {
	if cfg.ReadWorkers == 0 {
		cfg.ReadWorkers = workers
	}
	if cfg.TransformWorkers == 0 {
		cfg.TransformWorkers = workers
	}
	if cfg.Buffer == 0 {
		cfg.Buffer = maxInt(8, workers*4)
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = defaultBatchSize
	}
	return cfg
}

func generateTransactionsDataset(cfg transactionsDatasetConfig) (transactionsDatasetStats, error) {
	if err := os.RemoveAll(cfg.Root); err != nil {
		return transactionsDatasetStats{}, err
	}
	if err := os.MkdirAll(cfg.Root, 0o755); err != nil {
		return transactionsDatasetStats{}, err
	}

	filePath := filepath.Join(cfg.Root, "transactions.csv")
	file, err := os.Create(filePath)
	if err != nil {
		return transactionsDatasetStats{}, err
	}
	defer file.Close()

	rng := rand.New(rand.NewSource(cfg.Seed))
	writer := csv.NewWriter(file)
	if err := writer.Write([]string{"user_id", "amount", "currency", "date", "product_type", "user_status"}); err != nil {
		return transactionsDatasetStats{}, err
	}

	products := []string{"books", "electronics", "toys", "travel", "groceries", "tools"}
	currencies := []string{"UAH", "USD", "EUR"}
	statuses := []string{"standard", "silver", "gold"}
	baseDate := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

	for idx := 0; idx < cfg.Records; idx++ {
		amountCents := int64(100 + rng.Intn(250_000))
		date := baseDate.AddDate(0, 0, rng.Intn(365)).Format("2006-01-02")
		record := []string{
			fmt.Sprintf("U%06d", 1+rng.Intn(20_000)),
			formatMoney(amountCents),
			currencies[rng.Intn(len(currencies))],
			date,
			products[rng.Intn(len(products))],
			statuses[rng.Intn(len(statuses))],
		}
		if err := writer.Write(record); err != nil {
			return transactionsDatasetStats{}, err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return transactionsDatasetStats{}, err
	}

	return transactionsDatasetStats{Files: 1, Records: cfg.Records}, nil
}

// --- Sequential ---

func processTransactionsSequential(path string) (transactionStats, error) {
	reader, file, err := openTransactionsReader(path)
	if err != nil {
		return transactionStats{}, err
	}
	defer file.Close()

	stats := transactionStats{}
	for {
		record, err := reader.Read()
		if err == io.EOF {
			return stats, nil
		}
		if err != nil {
			return transactionStats{}, err
		}
		tx, err := parseTransactionRecord(record)
		if err != nil {
			return transactionStats{}, err
		}
		stats.add(processTransaction(tx))
	}
}

// --- Pipeline (batched) ---

func processTransactionsPipeline(cfg transactionsConfig) (transactionStats, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rawBatches := make(chan [][]string, cfg.Buffer)
	parsedBatches := make(chan []Transaction, cfg.Buffer)
	convertedBatches := make(chan []convertedTransaction, cfg.Buffer)
	processedBatches := make(chan []processedTransaction, cfg.Buffer)
	errCh := make(chan error, 1)

	// Stage 1: read CSV into batches of raw rows
	go func() {
		defer close(rawBatches)
		reader, file, err := openTransactionsReader(cfg.Input)
		if err != nil {
			sendPipelineError(errCh, cancel, err)
			return
		}
		defer file.Close()

		batch := make([][]string, 0, cfg.BatchSize)
		for {
			record, err := reader.Read()
			if err == io.EOF {
				if len(batch) > 0 {
					select {
					case <-ctx.Done():
						return
					case rawBatches <- batch:
					}
				}
				return
			}
			if err != nil {
				sendPipelineError(errCh, cancel, err)
				return
			}
			batch = append(batch, record)
			if len(batch) >= cfg.BatchSize {
				select {
				case <-ctx.Done():
					return
				case rawBatches <- batch:
				}
				batch = make([][]string, 0, cfg.BatchSize)
			}
		}
	}()

	// Stage 2: parse raw rows into Transaction structs
	var parseWG sync.WaitGroup
	parseWG.Add(cfg.ReadWorkers)
	for i := 0; i < cfg.ReadWorkers; i++ {
		go func() {
			defer parseWG.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case batch, ok := <-rawBatches:
					if !ok {
						return
					}
					parsed := make([]Transaction, 0, len(batch))
					for _, record := range batch {
						tx, err := parseTransactionRecord(record)
						if err != nil {
							sendPipelineError(errCh, cancel, err)
							return
						}
						parsed = append(parsed, tx)
					}
					select {
					case <-ctx.Done():
						return
					case parsedBatches <- parsed:
					}
				}
			}
		}()
	}
	go func() {
		parseWG.Wait()
		close(parsedBatches)
	}()

	// Stage 3: convert currency
	var convertWG sync.WaitGroup
	convertWG.Add(cfg.TransformWorkers)
	for i := 0; i < cfg.TransformWorkers; i++ {
		go func() {
			defer convertWG.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case batch, ok := <-parsedBatches:
					if !ok {
						return
					}
					conv := make([]convertedTransaction, 0, len(batch))
					for _, tx := range batch {
						conv = append(conv, convertedTransaction{
							Tx:             tx,
							GrossUAHKopeck: convertToUAHKopecks(tx.AmountCents, tx.Currency),
						})
					}
					select {
					case <-ctx.Done():
						return
					case convertedBatches <- conv:
					}
				}
			}
		}()
	}
	go func() {
		convertWG.Wait()
		close(convertedBatches)
	}()

	// Stage 4: apply cashback
	var cashbackWG sync.WaitGroup
	cashbackWG.Add(cfg.TransformWorkers)
	for i := 0; i < cfg.TransformWorkers; i++ {
		go func() {
			defer cashbackWG.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case batch, ok := <-convertedBatches:
					if !ok {
						return
					}
					out := make([]processedTransaction, 0, len(batch))
					for _, item := range batch {
						out = append(out, applyCashback(item))
					}
					select {
					case <-ctx.Done():
						return
					case processedBatches <- out:
					}
				}
			}
		}()
	}
	go func() {
		cashbackWG.Wait()
		close(processedBatches)
	}()

	// Aggregator
	stats := transactionStats{}
	for batch := range processedBatches {
		for _, item := range batch {
			stats.add(item)
		}
	}
	select {
	case err := <-errCh:
		return transactionStats{}, err
	default:
		return stats, nil
	}
}

// --- Producer-Consumer (batched) ---

func processTransactionsProducerConsumer(cfg transactionsConfig, workers int) (transactionStats, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobs := make(chan []Transaction, cfg.Buffer)
	results := make(chan transactionStats, cfg.Buffer)
	errCh := make(chan error, 1)

	// Producer: read and parse CSV into batches
	go func() {
		defer close(jobs)
		reader, file, err := openTransactionsReader(cfg.Input)
		if err != nil {
			sendPipelineError(errCh, cancel, err)
			return
		}
		defer file.Close()

		batch := make([]Transaction, 0, cfg.BatchSize)
		for {
			record, err := reader.Read()
			if err == io.EOF {
				if len(batch) > 0 {
					select {
					case <-ctx.Done():
						return
					case jobs <- batch:
					}
				}
				return
			}
			if err != nil {
				sendPipelineError(errCh, cancel, err)
				return
			}
			tx, err := parseTransactionRecord(record)
			if err != nil {
				sendPipelineError(errCh, cancel, err)
				return
			}
			batch = append(batch, tx)
			if len(batch) >= cfg.BatchSize {
				select {
				case <-ctx.Done():
					return
				case jobs <- batch:
				}
				batch = make([]Transaction, 0, cfg.BatchSize)
			}
		}
	}()

	// Consumers: process batches and compute partial stats
	var consumerWG sync.WaitGroup
	consumerWG.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer consumerWG.Done()
			local := transactionStats{}
			for {
				select {
				case <-ctx.Done():
					return
				case batch, ok := <-jobs:
					if !ok {
						results <- local
						return
					}
					for _, tx := range batch {
						local.add(processTransaction(tx))
					}
				}
			}
		}()
	}

	go func() {
		consumerWG.Wait()
		close(results)
	}()

	// Aggregate partial stats
	stats := transactionStats{}
	for partial := range results {
		stats.Records += partial.Records
		stats.GrossUAH += partial.GrossUAH
		stats.CashbackUAH += partial.CashbackUAH
		stats.NetUAH += partial.NetUAH
		stats.ChecksumValue += partial.ChecksumValue
	}
	select {
	case err := <-errCh:
		return transactionStats{}, err
	default:
		return stats, nil
	}
}

// --- Shared helpers ---

func openTransactionsReader(path string) (*csv.Reader, *os.File, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		file.Close()
		return nil, nil, err
	}
	expected := []string{"user_id", "amount", "currency", "date", "product_type", "user_status"}
	if len(header) != len(expected) {
		file.Close()
		return nil, nil, fmt.Errorf("unexpected transactions header")
	}
	for idx := range expected {
		if header[idx] != expected[idx] {
			file.Close()
			return nil, nil, fmt.Errorf("unexpected transactions header")
		}
	}
	return reader, file, nil
}

func parseTransactionRecord(record []string) (Transaction, error) {
	if len(record) != 6 {
		return Transaction{}, fmt.Errorf("invalid transaction record width: %d", len(record))
	}
	amountCents, err := parseMoney(record[1])
	if err != nil {
		return Transaction{}, err
	}
	return Transaction{
		UserID:      record[0],
		AmountCents: amountCents,
		Currency:    record[2],
		Date:        record[3],
		ProductType: record[4],
		UserStatus:  record[5],
	}, nil
}

func parseMoney(raw string) (int64, error) {
	parts := strings.Split(strings.TrimSpace(raw), ".")
	if len(parts) == 1 {
		value, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid amount %q", raw)
		}
		return value * 100, nil
	}
	if len(parts) != 2 || len(parts[1]) != 2 {
		return 0, fmt.Errorf("invalid amount %q", raw)
	}
	whole, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid amount %q", raw)
	}
	fraction, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid amount %q", raw)
	}
	return whole*100 + fraction, nil
}

func convertToUAHKopecks(amountCents int64, currency string) int64 {
	switch currency {
	case "UAH":
		return amountCents
	case "USD":
		return amountCents * 40
	case "EUR":
		return amountCents * 42
	default:
		return amountCents
	}
}

func cashbackPercent(status string) int64 {
	switch status {
	case "gold":
		return 20
	case "silver":
		return 10
	default:
		return 0
	}
}

func applyCashback(item convertedTransaction) processedTransaction {
	cashback := item.GrossUAHKopeck * cashbackPercent(item.Tx.UserStatus) / 100
	return processedTransaction{
		UserID:             item.Tx.UserID,
		GrossUAHKopecks:    item.GrossUAHKopeck,
		CashbackUAHKopecks: cashback,
		NetUAHKopecks:      item.GrossUAHKopeck - cashback,
	}
}

func processTransaction(tx Transaction) processedTransaction {
	return applyCashback(convertedTransaction{
		Tx:             tx,
		GrossUAHKopeck: convertToUAHKopecks(tx.AmountCents, tx.Currency),
	})
}

func (stats *transactionStats) add(item processedTransaction) {
	stats.Records++
	stats.GrossUAH += item.GrossUAHKopecks
	stats.CashbackUAH += item.CashbackUAHKopecks
	stats.NetUAH += item.NetUAHKopecks
	stats.ChecksumValue += checksumString(item.UserID) + item.GrossUAHKopecks*3 + item.CashbackUAHKopecks*5 + item.NetUAHKopecks*7
}

func sendPipelineError(errCh chan error, cancel context.CancelFunc, err error) {
	select {
	case errCh <- err:
	default:
	}
	cancel()
}
