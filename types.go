package main

import "time"

type ProcLog struct {
	SolID         string
	Procedure     string
	StartTime     time.Time
	EndTime       time.Time
	ExecutionTime time.Duration
	Status        string
	ErrorDetails  string
}

type ProcSummary struct {
	Procedure string
	StartTime time.Time
	EndTime   time.Time
	Status    string
}
