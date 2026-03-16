package main

func splitEvenly(total int64, workers int) [][2]int64 {
	if workers <= 1 || total <= 1 {
		return [][2]int64{{0, total}}
	}
	if int64(workers) > total {
		workers = int(total)
	}

	parts := make([][2]int64, 0, workers)
	base := total / int64(workers)
	extra := total % int64(workers)
	var start int64
	for idx := 0; idx < workers; idx++ {
		size := base
		if int64(idx) < extra {
			size++
		}
		end := start + size
		parts = append(parts, [2]int64{start, end})
		start = end
	}
	return parts
}

func splitIntRange(start, end, workers int) [][2]int {
	if end < start {
		return [][2]int{}
	}
	total := end - start + 1
	if workers <= 1 || total <= 1 {
		return [][2]int{{start, end}}
	}
	if workers > total {
		workers = total
	}

	base := total / workers
	extra := total % workers
	parts := make([][2]int, 0, workers)
	current := start
	for idx := 0; idx < workers; idx++ {
		size := base
		if idx < extra {
			size++
		}
		partEnd := current + size - 1
		parts = append(parts, [2]int{current, partEnd})
		current = partEnd + 1
	}
	return parts
}
