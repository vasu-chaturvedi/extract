package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

func runExtractionForSol(ctx context.Context, db *sql.DB, solID string, extractionConfig *ExtractionConfig, procLogCh chan ProcLog, mutex *sync.Mutex, procSummary map[string]ProcSummary) {
	panic("unimplemented")
}

// Consolidate multiple spool files per procedure into one <proc>.txt file
func consolidateSpoolFiles(procConfig *ExtractionConfig) error {
	fmt.Println("Extraction Output Directory:", procConfig.SpoolOutputPath)
	files, err := os.ReadDir(procConfig.SpoolOutputPath)
	if err != nil {
		return fmt.Errorf("failed to read spool output directory: %w", err)
	}

	// Map procedure name -> list of spool files
	spoolFilesMap := make(map[string][]string)
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		name := f.Name()
		for _, proc := range procConfig.Procedures {
			if strings.HasPrefix(name, proc+"_") && (strings.HasSuffix(name, ".txt") || strings.HasSuffix(name, ".spool")) {
				spoolFilesMap[proc] = append(spoolFilesMap[proc], filepath.Join(procConfig.SpoolOutputPath, name))
			}
		}
	}

	for proc, files := range spoolFilesMap {
		outFile := filepath.Join(procConfig.SpoolOutputPath, proc+".txt")
		fOut, err := os.Create(outFile)
		if err != nil {
			log.Printf("Failed to create consolidated file for %s: %v", proc, err)
			continue
		}

		var totalBytes int64
		for i, fpath := range files {
			fIn, err := os.Open(fpath)
			if err != nil {
				log.Printf("Failed to open spool file %s: %v", fpath, err)
				continue
			}

			n, err := io.Copy(fOut, fIn)
			fIn.Close()
			if err != nil {
				log.Printf("Error copying spool file %s: %v", fpath, err)
				continue
			}
			totalBytes += n

			// Delete the file after successful copy
			err = os.Remove(fpath)
			if err != nil {
				log.Printf("Failed to delete spool file %s after consolidation: %v", fpath, err)
			} else {
				log.Printf("Deleted spool file %s after consolidation", fpath)
			}

			// Add newline between files except after last one
			if i < len(files)-1 {
				if _, err := fOut.Write([]byte("\n")); err != nil {
					log.Printf("Failed to write newline after file %s: %v", fpath, err)
				}
			}
		}

		if err := fOut.Close(); err != nil {
			log.Printf("Failed to close consolidated file %s: %v", outFile, err)
		}

		log.Printf("Consolidated %d files for %s, total bytes: %d", len(files), proc, totalBytes)
	}

	return nil
}
