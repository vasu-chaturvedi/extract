package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

func runExtractionForSol(ctx context.Context, db *sql.DB, solID string, procConfig *ExtractionConfig, logCh chan<- ProcLog, mu *sync.Mutex, summary map[string]ProcSummary) {
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
				err := extractData(ctx, db, proc, solID, procConfig)
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
func extractData(ctx context.Context, db *sql.DB, procName, solID string, cfg *ExtractionConfig) error {
	TemplFile := filepath.Join(cfg.TemplatePath, fmt.Sprintf("%s.csv", procName))
	//fmt.Println("Using template file:", TemplFile)
	cols, err := readColumnsFromCSV(TemplFile)

	if err != nil {
		return err
	}

	colNames := make([]string, len(cols))
	for i, col := range cols {
		colNames[i] = col.Name
	}

	query := fmt.Sprintf("SELECT %s FROM %s WHERE SOL_ID = :1", strings.Join(colNames, ", "), procName)
	fmt.Printf("Executing query: %s with SOL_ID: %s\n", query, solID)
	rows, err := db.QueryContext(ctx, query, solID)
	if err != nil {
		return err
	}
	defer rows.Close()

	tempFile := filepath.Join(cfg.SpoolOutputPath, fmt.Sprintf("%s_%s.spool", procName, solID))
	f, err := os.Create(tempFile)
	if err != nil {
		return err
	}
	defer f.Close()

	for rows.Next() {
		values := make([]sql.NullString, len(cols))
		scanArgs := make([]interface{}, len(cols))
		for i := range values {
			scanArgs[i] = &values[i]
		}
		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}
		var strValues []string
		for _, v := range values {
			if v.Valid {
				strValues = append(strValues, v.String)
			} else {
				strValues = append(strValues, "")
			}
		}
		f.WriteString(formatRow(cfg, cols, strValues) + "\n")
	}
	return nil
}

func mergeFiles(runCfg *ExtractionConfig) error {
	path := runCfg.SpoolOutputPath

	for _, proc := range runCfg.Procedures {
		pattern := fmt.Sprintf("%s/%s*.spool", path, proc)
		tempFile := fmt.Sprintf("%s/%s.tmp", path, proc)
		finalFile := fmt.Sprintf("%s/%s.txt", path, proc)

		start := time.Now()

		// Step 1: Create or truncate temp file
		outFile, err := os.Create(tempFile)
		if err != nil {
			log.Fatalf("Failed to create %s: %v", tempFile, err)
			return err
		}
		defer outFile.Close()

		// Step 2: Run `cat ./output/RC001*.spool` and write to temp file
		catCmd := exec.Command("bash", "-c", "cat "+pattern)
		catCmd.Stdout = outFile

		log.Printf("Merging files matching %s into %s...\n", pattern, tempFile)
		if err := catCmd.Run(); err != nil {
			log.Fatalf("Failed to merge files: %v", err)
			return err
		}
		log.Println("Merge successful.")

		// Step 3: Rename temp file to final .txt
		if err := os.Rename(tempFile, finalFile); err != nil {
			log.Fatalf("Failed to rename %s to %s: %v", tempFile, finalFile, err)
			return err
		}
		log.Printf("File written successfully to %s\n", finalFile)

		// Step 4: Delete spool files only if merge was successful
		rmCmd := exec.Command("bash", "-c", "rm "+pattern)
		log.Printf("Deleting files: %s\n", pattern)
		if err := rmCmd.Run(); err != nil {
			log.Fatalf("Failed to delete spool files: %v", err)
			return err
		}
		log.Println("Cleanup complete.")

		// Step 5: Log total time taken
		elapsed := time.Since(start)
		log.Printf("Total time taken for procedure %s: %s\n", proc, elapsed)
	}
	return nil
}

func readColumnsFromCSV(path string) ([]ColumnConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	headers, err := r.Read()
	if err != nil {
		return nil, err
	}

	index := make(map[string]int)
	for i, h := range headers {
		index[strings.ToLower(h)] = i
	}

	var cols []ColumnConfig
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		col := ColumnConfig{Name: row[index["name"]]}
		if i, ok := index["length"]; ok && i < len(row) {
			col.Length, _ = strconv.Atoi(row[i])
		}
		if i, ok := index["align"]; ok && i < len(row) {
			col.Align = row[i]
		}
		cols = append(cols, col)
	}
	return cols, nil
}

func sanitize(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "\n", " "), "\r", " ")
}

func formatRow(cfg *ExtractionConfig, cols []ColumnConfig, values []string) string {
	switch cfg.Format {
	case "delimited":
		var parts []string
		for _, v := range values {
			parts = append(parts, sanitize(v))
		}
		return strings.Join(parts, cfg.Delimiter)
	case "fixed":
		var out strings.Builder
		for i, col := range cols {
			val := sanitize(values[i])
			if len(val) > col.Length {
				val = val[:col.Length]
			}
			if col.Align == "right" {
				out.WriteString(fmt.Sprintf("%*s", col.Length, val))
			} else {
				out.WriteString(fmt.Sprintf("%-*s", col.Length, val))
			}
		}
		return out.String()
	default:
		return ""
	}
}
