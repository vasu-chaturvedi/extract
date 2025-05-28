package main

import (
	"context"
	"database/sql"
	"fmt"
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
