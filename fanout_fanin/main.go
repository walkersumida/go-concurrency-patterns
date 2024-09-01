package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

func Fanout[T any](ctx context.Context, source <-chan T, n int, worker func(context.Context, T) T) []<-chan T {
	out := make([]<-chan T, 0, n)

	for i := 0; i < n; i++ {
		ch := make(chan T)
		out = append(out, ch)

		go func() {
			defer close(ch)
			for val := range source {
				select {
				case <-ctx.Done():
					return
				default:
					ch <- worker(ctx, val)
				}
			}
		}()
	}

	return out
}

func Fanin[T any](ctx context.Context, channels []<-chan T) <-chan T {
	out := make(chan T)

	var wg sync.WaitGroup
	for _, ch := range channels {
		wg.Add(1)
		go func(c <-chan T) {
			defer wg.Done()
			for val := range c {
				select {
				case <-ctx.Done():
					return
				case out <- val:
				}
			}
		}(ch)
	}

	go func() {
		defer close(out)
		wg.Wait()
	}()

	return out
}

type Job struct {
	ID     int
	Result int
	Err    error
}

func main() {
	start := time.Now()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobChs := make(chan Job, 10)
	for i := 0; i < 10; i++ {
		jobChs <- Job{ID: i}
	}
	close(jobChs)

	worker := func(ctx context.Context, r Job) Job {
		time.Sleep(1 * time.Second) // heavy processing
		if r.ID == 5 {
			// cancel() // uncomment this to cancel the process
			r.Err = fmt.Errorf("error occurred")
			return r
		}

		r.Result = r.ID * 2
		return r
	}

	// Fan-out
	outputChs := Fanout(ctx, jobChs, 3, worker)

	// Fan-in
	result := Fanin(ctx, outputChs)

	// Process the results
	for r := range result {
		fmt.Println(r)
	}

	fmt.Println("Time taken:", time.Since(start))
}
