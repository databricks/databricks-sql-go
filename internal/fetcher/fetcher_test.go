package fetcher

import (
	"context"
	"github.com/databricks/databricks-sql-go/internal/config"
	"math"
	"testing"
	"time"
)

// Create a mock struct for FetchableItems
type mockFetchableItem struct {
	item int
	wait time.Duration
	err  error
}

type mockOutput struct {
	item int
}

// Implement the Fetch method
func (m *mockFetchableItem) Fetch(ctx context.Context, cfg *config.Config) ([]*mockOutput, error) {
	time.Sleep(m.wait)
	outputs := make([]*mockOutput, 5)
	for i := range outputs {
		sampleOutput := mockOutput{item: m.item}
		outputs[i] = &sampleOutput
	}
	return outputs, nil
}

var _ FetchableItems[*mockOutput] = (*mockFetchableItem)(nil)

func TestConcurrentFetcher(t *testing.T) {
	t.Run("Comprehensively tests the concurrent fetcher", func(t *testing.T) {
		ctx := context.Background()
		cfg := &config.Config{}
		inputChan := make(chan FetchableItems[*mockOutput], 10)
		for i := 0; i < 10; i++ {
			item := mockFetchableItem{item: i, wait: 1 * time.Second}
			inputChan <- &item
		}
		close(inputChan)

		// Create a fetcher
		fetcher, err := NewConcurrentFetcher[*mockFetchableItem](ctx, 3, cfg, inputChan)
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
			results = append(results, result)
		}

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
}
