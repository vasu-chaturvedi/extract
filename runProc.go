package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"
)

func runProceduresForSol(ctx context.Context, db *sql.DB, solID string, procConfig *ExtractionConfig, logCh chan<- ProcLog, mu *sync.Mutex, summary map[string]ProcSummary) {
	var wg sync.WaitGroup
	procCh := make(chan string)

	// Worker pool for parallel procedure execution
	numWorkers := 4 // Adjust as needed for your environment
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for proc := range procCh {
				start := time.Now()
				err := callProcedure(ctx, db, procConfig.PackageName, proc, solID)
				end := time.Now()

				plog := ProcLog{
					SolID:         solID,
					Procedure:     proc,
					StartTime:     start,
					EndTime:       end,
					ExecutionTime: end.Sub(start),
				}
				if err != nil {
					plog.Status = "FAIL"
					plog.ErrorDetails = err.Error()
				} else {
					plog.Status = "SUCCESS"
				}
				logCh <- plog

				mu.Lock()
				s, exists := summary[proc]
				if !exists {
					s = ProcSummary{
						Procedure: proc,
						StartTime: start,
						EndTime:   end,
						Status:    plog.Status,
					}
				} else {
					if start.Before(s.StartTime) {
						s.StartTime = start
					}
					if end.After(s.EndTime) {
						s.EndTime = end
					}
					// Once failed always fail
					if s.Status != "FAIL" && plog.Status == "FAIL" {
						s.Status = "FAIL"
					}
				}
				summary[proc] = s
				mu.Unlock()
			}
		}()
	}

	// Feed procedures to workers
	for _, proc := range procConfig.Procedures {
		procCh <- proc
	}
	close(procCh)
	wg.Wait()
}

// Call stored procedure with solID parameter
func callProcedure(ctx context.Context, db *sql.DB, pkgName, procName, solID string) error {
	query := fmt.Sprintf("BEGIN %s.%s(:1); END;", pkgName, procName)
	_, err := db.ExecContext(ctx, query, solID)
	return err
}

// Write procedure logs to CSV file
func writeProcLogs(path string, logCh <-chan ProcLog) {
	file, err := os.Create(path)
	if err != nil {
		log.Fatalf("Failed to create procedure log file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	writer.Write([]string{"SOL_ID", "PROCEDURE", "START_TIME", "END_TIME", "EXECUTION_SECONDS", "STATUS", "ERROR_DETAILS"})

	for plog := range logCh {
		errDetails := plog.ErrorDetails
		if errDetails == "" {
			errDetails = "-"
		}
		timeFormat := "02-01-2006 15:04:05"
		record := []string{
			plog.SolID,
			plog.Procedure,
			plog.StartTime.Format(timeFormat),
			plog.EndTime.Format(timeFormat),
			fmt.Sprintf("%.3f", plog.ExecutionTime.Seconds()),
			plog.Status,
			errDetails,
		}
		writer.Write(record)
	}
}

// Write procedure summary CSV after all executions
func writeProcedureSummary(path string, summary map[string]ProcSummary) {
	file, err := os.Create(path)
	if err != nil {
		log.Printf("Failed to create procedure summary file: %v", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Header
	writer.Write([]string{"PROCEDURE", "EARLIEST_START_TIME", "LATEST_END_TIME", "EXECUTION_SECONDS", "STATUS"})

	// Sort procedures alphabetically
	var procs []string
	for p := range summary {
		procs = append(procs, p)
	}
	sort.Strings(procs)

	for _, p := range procs {
		s := summary[p]
		execSeconds := s.EndTime.Sub(s.StartTime).Seconds()
		timeFormat := "02-01-2006 15:04:05"
		writer.Write([]string{
			p,
			s.StartTime.Format(timeFormat),
			s.EndTime.Format(timeFormat),
			fmt.Sprintf("%.3f", execSeconds),
			s.Status,
		})
	}
}
