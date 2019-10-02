package main

import (
	"fmt"
	"hubble"

	"github.com/stellar/go/exp/ingest"
	ingestPipeline "github.com/stellar/go/exp/ingest/pipeline"
	"github.com/stellar/go/support/historyarchive"
)

func main() {
	fmt.Println("Kicking off ingestion!")
	session, err := getTestSession()
	if err != nil {
		print(err)
		return
	}
	fmt.Println("Got test session!")
	err = session.Run()
	if err != nil {
		print(err)
		return
	}
	fmt.Println("Completing ingestion!")
}

func getTestSession() (*ingest.SingleLedgerSession, error) {
	archive, err := getTestArchive()
	if err != nil {
		return nil, err
	}
	statePipeline, err := getTestStatePipeline()
	if err != nil {
		return nil, err
	}
	session := &ingest.SingleLedgerSession{
		Archive:       archive,
		StatePipeline: statePipeline,
	}
	return session, nil
}

func getTestArchive() (*historyarchive.Archive, error) {
	return historyarchive.Connect(
		fmt.Sprintf("s3://history.stellar.org/prd/core-live/core_live_001/"),
		historyarchive.ConnectOptions{
			S3Region:         "eu-west-1",
			UnsignedRequests: true,
		},
	)
}

func getTestStatePipeline() (*ingestPipeline.StatePipeline, error) {
	pipeline := &ingestPipeline.StatePipeline{}
	printAllProcessor := &hubble.PrintAllProcessor{}
	pipeline.SetRoot(
		ingestPipeline.StateNode(printAllProcessor),
	)
	return pipeline, nil
}
