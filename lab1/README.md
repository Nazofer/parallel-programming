# Lab 1

Run in terminal:

```bash
cd lab1
```

Run a task in single / parallel mode:

```bash
go run . run <pi|factorize|primes|matrix|words> [flags]
```

Run benchmark for a task (`1,2,4,8` workers etc.):

```bash
go run . bench <pi|factorize|primes|matrix|words> [flags]
```

Generate dataset for words task:

```bash
go run . generate words-dataset [flags]
```

Build binary and run compiled version:

```bash
go build -o lab1 .
./lab1 run <pi|factorize|primes|matrix|words> [flags]
```
