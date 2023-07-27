package fetcher

import (
	"context"
	"github.com/databricks/databricks-sql-go/internal/config"
	"testing"
)

// Create a mock struct for FetchableItems
type mockFetchableItem struct {
	item int
	err  error
}

type mockOutput struct {
	item int
}

// Implement the Fetch method
func (m *mockFetchableItem) Fetch(ctx context.Context, cfg *config.Config) ([]*mockOutput, error) {
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
			item := mockFetchableItem{item: i}
			inputChan <- &item
		}
		close(inputChan)

		// Create a fetcher
		fetcher, err := NewConcurrentFetcher[*mockFetchableItem](ctx, 3, cfg, inputChan)
		if err != nil {
			t.Fatalf("Error creating fetcher: %v", err)
		}

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
	})
}
