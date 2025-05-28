package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/godror/godror"
)

func main() {
	appCfgFile := flag.String("main-config", "config.json", "Path to main config JSON file")
	runCfgFile := flag.String("extraction-config", "extraction_config.json", "Path to extraction config JSON file")
	flag.Parse()

	appCfg, err := loadMainConfig(*appCfgFile)
	if err != nil {
		log.Fatalf("Failed to load main config: %v", err)
	}

	runCfg, err := loadExtractionConfig(*runCfgFile)
	if err != nil {
		log.Fatalf("Failed to load extraction config: %v", err)
	}

	fmt.Println("extractionCfg", runCfg)

	connString := fmt.Sprintf(`user="%s" password="%s" connectString="%s:%d/%s"`,
		appCfg.DBUser, appCfg.DBPassword, appCfg.DBHost, appCfg.DBPort, appCfg.DBSid)

	fmt.Println("Connection String", connString)

	procCount := len(runCfg.Procedures)

	db, err := sql.Open("godror", connString)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(appCfg.Concurrency * procCount)
	db.SetMaxIdleConns(appCfg.Concurrency * procCount)
	db.SetConnMaxLifetime(30 * time.Minute)

	sols, err := readSols(appCfg.SolFilePath)
	if err != nil {
		log.Fatalf("Failed to read SOL IDs: %v", err)
	}

	procLogCh := make(chan ProcLog, 1000)
	var summaryMu sync.Mutex
	procSummary := make(map[string]ProcSummary)

	go writeProcLogs(filepath.Join(appCfg.LogFilePath, "procedure_calls.csv"), procLogCh)

	sem := make(chan struct{}, appCfg.Concurrency)
	var wg sync.WaitGroup

	ctx := context.Background()

	totalSols := len(sols)
	log.Printf("Starting processing for %d SOL IDs and %d procedures", totalSols, procCount)

	// Tracking the overall start time
	overallStart := time.Now()

	// Progress tracking
	var mu sync.Mutex
	completed := 0

	for _, sol := range sols {
		wg.Add(1)
		sem <- struct{}{}
		go func(solID string) {
			defer wg.Done()
			defer func() { <-sem }()
			runProceduresForSol(ctx, db, solID, &runCfg, procLogCh, &summaryMu, procSummary)

			// Update and log progress
			mu.Lock()
			completed++
			fmt.Printf("Completed %d/%d (%.2f%%)\n", completed, totalSols, float64(completed)*100/float64(totalSols))
			mu.Unlock()
		}(sol)
	}

	wg.Wait()
	close(procLogCh)

	writeProcedureSummary(filepath.Join(appCfg.LogFilePath, "procedure_summary.csv"), procSummary)

	err = consolidateSpoolFiles(&runCfg)
	if err != nil {
		log.Printf("Error consolidating spool files: %v", err)
	}

	totalTime := time.Since(overallStart)
	log.Printf("All done! Processed %d SOLs in %s", totalSols, totalTime)
}
