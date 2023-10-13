package fetcher

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Create a mock struct for FetchableItems
type mockFetchableItem struct {
	item int
	wait time.Duration
}

type mockOutput struct {
	item int
}

// Implement the Fetch method
func (m *mockFetchableItem) Fetch(ctx context.Context) ([]*mockOutput, error) {
	time.Sleep(m.wait)
	outputs := make([]*mockOutput, 5)
	for i := range outputs {
		sampleOutput := mockOutput{item: m.item}
		outputs[i] = &sampleOutput
	}
	return outputs, nil
}

var _ FetchableItems[[]*mockOutput] = (*mockFetchableItem)(nil)

type testOverWatch struct {
	started, stopped bool
}

func (ow *testOverWatch) Start() { ow.started = true }
func (ow *testOverWatch) Stop()  { ow.stopped = true }

func TestConcurrentFetcher(t *testing.T) {
	t.Run("Comprehensively tests the concurrent fetcher", func(t *testing.T) {
		ctx := context.Background()

		inputChan := make(chan FetchableItems[[]*mockOutput], 10)
		for i := 0; i < 10; i++ {
			item := mockFetchableItem{item: i, wait: 1 * time.Second}
			inputChan <- &item
		}
		close(inputChan)

		ow := &testOverWatch{}

		// Create a fetcher
		fetcher, err := NewConcurrentFetcher[*mockFetchableItem](ctx, 3, 3, inputChan, ow)
		if err != nil {
			t.Fatalf("Error creating fetcher: %v", err)
		}

		start := time.Now()
		outChan, _, err := fetcher.Start()
		if err != nil {
			t.Fatalf("Error starting fetcher: %v", err)
		}

		var results []*mockOutput
		for result := range outChan {
			results = append(results, result...)
		}

		assert.True(t, ow.started)
		assert.True(t, ow.stopped)

		// Check if the fetcher returned the expected results
		expectedLen := 50
		if len(results) != expectedLen {
			t.Errorf("Expected %d results, got %d", expectedLen, len(results))
		}

		// Check if the fetcher returned an error
		if fetcher.Err() != nil {
			t.Errorf("Fetcher returned an error: %v", fetcher.Err())
		}

		// Check if the fetcher took around the estimated amount of time
		timeElapsed := time.Since(start)
		rounds := int(math.Ceil(float64(10) / 3))
		expectedTime := time.Duration(rounds) * time.Second
		buffer := 100 * time.Millisecond
		if timeElapsed-expectedTime > buffer {
			t.Errorf("Expected fetcher to take around %d ms, took %d ms", int64(expectedTime/time.Millisecond), int64(timeElapsed/time.Millisecond))
		}
	})

	t.Run("Cancel the concurrent fetcher", func(t *testing.T) {
		// Create a context with a timeout
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Create an input channel
		inputChan := make(chan FetchableItems[[]*mockOutput], 5)
		for i := 0; i < 5; i++ {
			item := mockFetchableItem{item: i, wait: 2 * time.Second}
			inputChan <- &item
		}
		close(inputChan)
		ow := &testOverWatch{}

		// Create a new fetcher
		fetcher, err := NewConcurrentFetcher[*mockFetchableItem](ctx, 2, 2, inputChan, ow)
		if err != nil {
			t.Fatalf("Error creating fetcher: %v", err)
		}

		// Start the fetcher
		outChan, cancelFunc, err := fetcher.Start()
		if err != nil {
			t.Fatal(err)
		}

		// Ensure that the fetcher is cancelled successfully
		go func() {
			cancelFunc()
		}()

		var count int
		for range outChan {
			// Just drain the channel
			count += 1
		}

		assert.Less(t, count, 5)

		err = fetcher.Err()
		assert.EqualError(t, err, "fetcher canceled")

		assert.True(t, ow.started)
		assert.True(t, ow.stopped)
	})

	t.Run("timeout the concurrent fetcher", func(t *testing.T) {
		// Create a context with a timeout
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		// Create an input channel
		inputChan := make(chan FetchableItems[[]*mockOutput], 10)
		for i := 0; i < 10; i++ {
			item := mockFetchableItem{item: i, wait: 1 * time.Second}
			inputChan <- &item
		}
		close(inputChan)

		ow := &testOverWatch{}

		// Create a new fetcher
		fetcher, err := NewConcurrentFetcher[*mockFetchableItem](ctx, 2, 2, inputChan, ow)
		if err != nil {
			t.Fatalf("Error creating fetcher: %v", err)
		}

		// Start the fetcher
		outChan, _, err := fetcher.Start()
		if err != nil {
			t.Fatal(err)
		}

		var count int
		for range outChan {
			// Just drain the channel
			count += 1
		}

		assert.Less(t, count, 10)

		err = fetcher.Err()
		assert.EqualError(t, err, "context deadline exceeded")

		assert.True(t, ow.started)
		assert.True(t, ow.stopped)
	})

	t.Run("context cancel the concurrent fetcher", func(t *testing.T) {
		// Create a context with a timeout
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

		// Create an input channel
		inputChan := make(chan FetchableItems[[]*mockOutput], 5)
		for i := 0; i < 5; i++ {
			item := mockFetchableItem{item: i, wait: 2 * time.Second}
			inputChan <- &item
		}
		close(inputChan)

		ow := &testOverWatch{}

		// Create a new fetcher
		fetcher, err := NewConcurrentFetcher[*mockFetchableItem](ctx, 2, 2, inputChan, ow)
		if err != nil {
			t.Fatalf("Error creating fetcher: %v", err)
		}

		// Start the fetcher
		outChan, _, err := fetcher.Start()
		if err != nil {
			t.Fatal(err)
		}

		// Ensure that the fetcher is cancelled successfully
		go func() {
			cancel()
		}()

		var count int
		for range outChan {
			// Just drain the channel
			count += 1
		}

		assert.Less(t, count, 5)

		err = fetcher.Err()
		assert.EqualError(t, err, "context canceled")

		assert.True(t, ow.started)
		assert.True(t, ow.stopped)
	})
}
