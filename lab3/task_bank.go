package main

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type bankConfig struct {
	Accounts  int
	Transfers int
	Seed      int64
	Timeout   time.Duration
}

type Account struct {
	ID      int
	Balance int64
	mu      sync.Mutex
}

func parseBankFlags(args []string, stderr io.Writer) (bankConfig, error) {
	fs := newFlagSet("bank", stderr)
	cfg := bankConfig{}
	fs.IntVar(&cfg.Accounts, "accounts", 200, "number of bank accounts")
	fs.IntVar(&cfg.Transfers, "transfers", 100, "transfers per goroutine")
	fs.Int64Var(&cfg.Seed, "seed", 42, "deterministic seed")
	timeout := fs.Int("timeout", 5, "deadlock detection timeout in seconds")
	if err := fs.Parse(args); err != nil {
		return bankConfig{}, err
	}
	if err := parsePositiveInt("accounts", cfg.Accounts); err != nil {
		return bankConfig{}, err
	}
	if cfg.Accounts < 2 {
		return bankConfig{}, fmt.Errorf("accounts must be >= 2")
	}
	if err := parsePositiveInt("transfers", cfg.Transfers); err != nil {
		return bankConfig{}, err
	}
	cfg.Timeout = time.Duration(*timeout) * time.Second
	return cfg, nil
}

func runBankTask(cfg bankConfig, variant executionVariant, workers int) (TaskResult, error) {
	start := time.Now()
	accounts := initAccounts(cfg.Accounts, cfg.Seed)
	totalBefore := sumBalances(accounts)

	var transfersDone int64
	var deadlockDetected bool
	var returnErr error
	switch variant {
	case variantSeq:
		transfersDone = runTransfersSeq(accounts, cfg, workers)
		workers = 1
	case variantUnsafe:
		transfersDone = runTransfersUnsafe(accounts, cfg, workers)
	case variantDeadlock:
		var err error
		transfersDone, err = runTransfersDeadlock(accounts, cfg, workers)
		if err != nil {
			deadlockDetected = true
			returnErr = err
		}
	case variantFixed:
		transfersDone = runTransfersFixed(accounts, cfg, workers)
	default:
		return TaskResult{}, fmt.Errorf("unsupported bank variant %q", variant)
	}

	totalAfter := sumBalances(accounts)
	consistent := totalBefore == totalAfter

	summary := fmt.Sprintf(
		"total_before=%d total_after=%d consistent=%t transfers=%d",
		totalBefore, totalAfter, consistent, transfersDone,
	)
	if deadlockDetected {
		summary += " deadlock=detected"
	}

	return TaskResult{
		Task:    "bank",
		Variant: string(variant),
		Workers: workers,
		Params: map[string]string{
			"accounts":  strconv.Itoa(cfg.Accounts),
			"transfers": strconv.Itoa(cfg.Transfers),
			"seed":      strconv.FormatInt(cfg.Seed, 10),
		},
		Summary:  summary,
		Duration: time.Since(start),
	}, returnErr
}

func initAccounts(n int, seed int64) []*Account {
	rng := rand.New(rand.NewSource(seed))
	accounts := make([]*Account, n)
	for i := range accounts {
		accounts[i] = &Account{
			ID:      i,
			Balance: int64(1000 + rng.Intn(99001)), // 1000..100000 cents
		}
	}
	return accounts
}

func sumBalances(accounts []*Account) int64 {
	var total int64
	for _, acc := range accounts {
		total += acc.Balance
	}
	return total
}

// --- Sequential (baseline) ---

func runTransfersSeq(accounts []*Account, cfg bankConfig, workers int) int64 {
	rng := rand.New(rand.NewSource(cfg.Seed + 1_000_000))
	var done int64
	total := workers * cfg.Transfers
	for i := 0; i < total; i++ {
		from, to := pickTwo(rng, len(accounts))
		amount := int64(1 + rng.Intn(100))
		if accounts[from].Balance >= amount {
			accounts[from].Balance -= amount
			accounts[to].Balance += amount
			done++
		}
	}
	return done
}

// --- Unsafe (race condition, no synchronization) ---

func runTransfersUnsafe(accounts []*Account, cfg bankConfig, workers int) int64 {
	var done int64
	var wg sync.WaitGroup
	wg.Add(workers)
	for g := 0; g < workers; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(cfg.Seed + int64(goroutineID)))
			for i := 0; i < cfg.Transfers; i++ {
				from, to := pickTwo(rng, len(accounts))
				amount := int64(1 + rng.Intn(100))
				// No locks: data race on Balance reads and writes
				if accounts[from].Balance >= amount {
					accounts[from].Balance -= amount
					accounts[to].Balance += amount
					done++ // data race on done counter too
				}
			}
		}(g)
	}
	wg.Wait()
	return done
}

// --- Deadlock (inconsistent lock ordering) ---

func runTransfersDeadlock(accounts []*Account, cfg bankConfig, workers int) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	var done int64
	var wg sync.WaitGroup
	wg.Add(workers)
	for g := 0; g < workers; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(cfg.Seed + int64(goroutineID)))
			for i := 0; i < cfg.Transfers; i++ {
				select {
				case <-ctx.Done():
					return
				default:
				}
				from, to := pickTwo(rng, len(accounts))
				// Lock in from->to order (inconsistent, causes deadlock)
				accounts[from].mu.Lock()
				accounts[to].mu.Lock()
				amount := int64(1 + rng.Intn(100))
				if accounts[from].Balance >= amount {
					accounts[from].Balance -= amount
					accounts[to].Balance += amount
					atomic.AddInt64(&done, 1)
				}
				accounts[to].mu.Unlock()
				accounts[from].mu.Unlock()
			}
		}(g)
	}

	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()

	select {
	case <-finished:
		return atomic.LoadInt64(&done), nil
	case <-ctx.Done():
		return atomic.LoadInt64(&done), fmt.Errorf("deadlock detected: timeout after %s", cfg.Timeout)
	}
}

// --- Fixed (consistent lock ordering by account ID) ---

func runTransfersFixed(accounts []*Account, cfg bankConfig, workers int) int64 {
	var done int64
	var wg sync.WaitGroup
	wg.Add(workers)
	for g := 0; g < workers; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(cfg.Seed + int64(goroutineID)))
			for i := 0; i < cfg.Transfers; i++ {
				from, to := pickTwo(rng, len(accounts))
				// Always lock lower ID first to prevent deadlock
				first, second := accounts[from], accounts[to]
				if first.ID > second.ID {
					first, second = second, first
				}
				first.mu.Lock()
				second.mu.Lock()
				amount := int64(1 + rng.Intn(100))
				if accounts[from].Balance >= amount {
					accounts[from].Balance -= amount
					accounts[to].Balance += amount
					atomic.AddInt64(&done, 1)
				}
				second.mu.Unlock()
				first.mu.Unlock()
			}
		}(g)
	}
	wg.Wait()
	return atomic.LoadInt64(&done)
}

func pickTwo(rng *rand.Rand, n int) (int, int) {
	a := rng.Intn(n)
	b := rng.Intn(n - 1)
	if b >= a {
		b++
	}
	return a, b
}
