# Lab 2

Run in terminal:

```bash
cd lab2
```

Run a task in single / parallel mode:

```bash
go run . run <tags|stats|matmul|transactions> [flags]
```

Run benchmark for a task (`1,2,4,8` workers etc.):

```bash
go run . bench <tags|stats|matmul|transactions> [flags]
```

Generate dataset for tags or transactions task:

```bash
go run . generate <html-dataset|transactions-dataset> [flags]
```

Build binary and run compiled version:

```bash
go build -o lab2 .
./lab2 run <tags|stats|matmul|transactions> [flags]
```
