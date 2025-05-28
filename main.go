package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/godror/godror"
)

var (
	appCfgFile = new(string)
	runCfgFile = new(string)
	mode       string
)

func init() {
	flag.StringVar(appCfgFile, "appCfg", "", "Path to the main application configuration file")
	flag.StringVar(runCfgFile, "runCfg", "", "Path to the extraction configuration file")
	flag.StringVar(&mode, "mode", "", "Mode of operation: E - Extract, I - Insert")

	flag.Parse()

	// Validate mode
	if mode != "E" && mode != "I" {
		log.Fatal("Invalid mode. Valid values are 'E' for Extract and 'I' for Insert.")
	}

	if *appCfgFile == "" || *runCfgFile == "" {
		log.Fatal("Both appCfg and runCfg must be specified")
	}

	if _, err := os.Stat(*appCfgFile); os.IsNotExist(err) {
		log.Fatalf("Application configuration file does not exist: %s", *appCfgFile)
	}

	if _, err := os.Stat(*runCfgFile); os.IsNotExist(err) {
		log.Fatalf("Extraction configuration file does not exist: %s", *runCfgFile)
	}
}

func main() {

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

	if (mode == "I" && !runCfg.RunInsertionParallel) || (mode == "E" && !runCfg.RunExtractionParallel) {
		log.Println("Running procedures sequentially as parallel execution is disabled")
		appCfg.Concurrency = 1
	}

	var LogFile, LogFileSummary string
	if mode == "I" {
		LogFile = runCfg.PackageName + "_insert.csv"
		LogFileSummary = runCfg.PackageName + "_insert_summary.csv"
	} else if mode == "E" {
		LogFile = runCfg.PackageName + "_extract.csv"
		LogFileSummary = runCfg.PackageName + "_extract_summary.csv"
	}

	go writeLog(filepath.Join(appCfg.LogFilePath, LogFile), procLogCh)

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
			if mode == "E" {
				runExtractionForSol(ctx, db, solID, &runCfg, procLogCh, &summaryMu, procSummary)
			} else if mode == "I" {
				runProceduresForSol(ctx, db, solID, &runCfg, procLogCh, &summaryMu, procSummary)
			}

			// Update and log progress
			mu.Lock()
			completed++
			//fmt.Printf("Completed %d/%d (%.2f%%)\n", completed, totalSols, float64(completed)*100/float64(totalSols))
			mu.Unlock()
		}(sol)
	}

	wg.Wait()
	close(procLogCh)

	writeSummary(filepath.Join(appCfg.LogFilePath, LogFileSummary), procSummary)

	err = consolidateSpoolFiles(&runCfg)
	if err != nil {
		log.Printf("Error consolidating spool files: %v", err)
	}

	totalTime := time.Since(overallStart)
	log.Printf("All done! Processed %d SOLs in %s", totalSols, totalTime)
}
