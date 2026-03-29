# Lab 3

Run in terminal:

```bash
cd lab3
```

Run a task:

```bash
go run . run <bank|ipc> [flags]
```

Demonstrate race condition (use -race flag):

```bash
go run -race . run bank --variant unsafe --workers 100 --accounts 50 --transfers 10
```

Build binary and run compiled version:

```bash
go build -o lab3 .
./lab3 run <bank|ipc> [flags]
```
