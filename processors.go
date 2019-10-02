package hubble

import (
	"context"
	"fmt"
	stdio "io"
	"sync"

	"github.com/stellar/go/exp/ingest/io"
	supportPipeline "github.com/stellar/go/exp/support/pipeline"
)

// SimpleProcessor defines the base Processor data structure
// and can be used in more complex Processor implementations.
// Those implementations must implement the StateProcessor interface.
type SimpleProcessor struct {
	sync.Mutex
	callCount int
}

// Reset sets the call counter to 0.
func (sp *SimpleProcessor) Reset() {
	sp.callCount = 0
}

// IncrementAndReturnCallCount increases and returns the call count.
func (sp *SimpleProcessor) IncrementAndReturnCallCount() int {
	sp.Lock()
	defer sp.Unlock()
	sp.callCount++
	return sp.callCount
}

// PrintAllProcessor implements the StateProcessor interface
type PrintAllProcessor struct {
	SimpleProcessor
}

// ProcessState processes and prints entries.
func (p *PrintAllProcessor) ProcessState(ctx context.Context, store *supportPipeline.Store, r io.StateReader, w io.StateWriter) error {
	defer w.Close()
	defer r.Close()

	entries := 0
	for {
		entry, err := r.Read()
		if err != nil {
			if err == stdio.EOF {
				break
			} else {
				return err
			}
		}

		entries++
		fmt.Printf("%+v\n", entry)

		select {
		case <-ctx.Done():
			return nil
		default:
			continue
		}
	}

	fmt.Printf("Found %d entries\n", entries)
	return nil
}

// Name prints the name of the processor.
func (p *PrintAllProcessor) Name() string {
	return "PrintAllProcessor"
}
