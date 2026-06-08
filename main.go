package main

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// Errors
var (
	ErrShardOwnershipLost         = errors.New("shard ownership lost: stale range ID")
	ErrWorkflowTaskAlreadyStarted = errors.New("workflow task already started")
)

// HistoryEvent represents a workflow history event.
type HistoryEvent struct {
	ID   int
	Type string
}

// WorkflowState represents the state of a workflow execution.
type WorkflowState struct {
	WorkflowID              string
	LastRangeID             int64
	WorkflowTaskActive      bool
	WorkflowTaskStartedTime time.Time
	WorkflowTaskTimeout     time.Duration
	History                 []HistoryEvent
}

// ShardContext represents the shard context with a RangeID.
type ShardContext struct {
	mu      sync.Mutex
	ShardID int
	RangeID int64
}

func NewShardContext(shardID int, rangeID int64) *ShardContext {
	return &ShardContext{
		ShardID: shardID,
		RangeID: rangeID,
	}
}

func (s *ShardContext) GetRangeID() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.RangeID
}

func (s *ShardContext) UpdateRangeID(newRangeID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.RangeID = newRangeID
}

// Database represents the persistence layer.
type Database struct {
	mu        sync.Mutex
	workflows map[string]*WorkflowState
}

func NewDatabase() *Database {
	return &Database{
		workflows: make(map[string]*WorkflowState),
	}
}

func (db *Database) GetWorkflow(workflowID string) (*WorkflowState, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	state, exists := db.workflows[workflowID]
	if !exists {
		return nil, false
	}
	// Return a copy to simulate DB isolation
	copyState := *state
	copyState.History = append([]HistoryEvent(nil), state.History...)
	return &copyState, true
}

// UpdateWorkflow updates the workflow state conditionally based on RangeID to prevent split-brain updates.
func (db *Database) UpdateWorkflow(workflowID string, state *WorkflowState, expectedRangeID int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	current, exists := db.workflows[workflowID]
	if exists {
		// Fencing check: Ensure the range ID has not progressed beyond the expected range ID
		if current.LastRangeID > expectedRangeID {
			return ErrShardOwnershipLost
		}
	}

	// Update state
	state.LastRangeID = expectedRangeID
	db.workflows[workflowID] = state
	return nil
}

// HistoryEngine implements the history service logic.
type HistoryEngine struct {
	shardCtx *ShardContext
	db       *Database
}

func NewHistoryEngine(shardCtx *ShardContext, db *Database) *HistoryEngine {
	return &HistoryEngine{
		shardCtx: shardCtx,
		db:       db,
	}
}

// RecordWorkflowTaskStarted records that a workflow task has started, with idempotency and fencing.
func (e *HistoryEngine) RecordWorkflowTaskStarted(workflowID string, requestRangeID int64, timeout time.Duration) error {
	// 1. Fencing check: Reject requests from stale engine instances or with stale RangeIDs
	currentRangeID := e.shardCtx.GetRangeID()
	if requestRangeID < currentRangeID {
		return ErrShardOwnershipLost
	}

	// 2. Retrieve current workflow state
	state, exists := e.db.GetWorkflow(workflowID)
	if !exists {
		state = &WorkflowState{
			WorkflowID:          workflowID,
			WorkflowTaskTimeout: timeout,
		}
	}

	// 3. Check if a workflow task is already active
	if state.WorkflowTaskActive {
		// Check if the task has timed out (StartToClose timeout)
		if time.Since(state.WorkflowTaskStartedTime) < state.WorkflowTaskTimeout {
			// Task is active and has not timed out yet.
			// To ensure idempotency and prevent duplicate execution, we check if this is a duplicate request.
			// If it's already active and not timed out, we should not re-dispatch or append duplicate events.
			return ErrWorkflowTaskAlreadyStarted
		}
		// If timed out, we can clear the active state and allow a new task to start
		state.WorkflowTaskActive = false
	}

	// 4. Check if WorkflowTaskStarted event already exists in history for the current attempt to prevent duplicates
	for _, event := range state.History {
		if event.Type == "WorkflowTaskStarted" && state.WorkflowTaskActive {
			// Already started, do not append duplicate event
			return nil
		}
	}

	// 5. Update state to active
	state.WorkflowTaskActive = true
	state.WorkflowTaskStartedTime = time.Now()
	state.History = append(state.History, HistoryEvent{
		ID:   len(state.History) + 1,
		Type: "WorkflowTaskStarted",
	})

	// 6. Commit to DB with conditional RangeID check (fencing)
	err := e.db.UpdateWorkflow(workflowID, state, requestRangeID)
	if err != nil {
		return err
	}

	return nil
}

func main() {
	fmt.Println("Starting Temporal Shard Failover Simulation...")

	db := NewDatabase()
	shardCtx := NewShardContext(1, 100) // Shard 1, RangeID 100

	engine1 := NewHistoryEngine(shardCtx, db)

	workflowID := "wf-1"
	timeout := 5 * time.Second

	// Scenario 1: Normal dispatch
	fmt.Println("\n--- Scenario 1: Normal Dispatch ---")
	err := engine1.RecordWorkflowTaskStarted(workflowID, 100, timeout)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Println("Workflow task started successfully.")
	}

	// Verify history
	state, _ := db.GetWorkflow(workflowID)
	fmt.Printf("History events: %v\n", state.History)

	// Scenario 2: Duplicate dispatch attempt before timeout
	fmt.Println("\n--- Scenario 2: Duplicate Dispatch Attempt (Before Timeout) ---")
	err = engine1.RecordWorkflowTaskStarted(workflowID, 100, timeout)
	if err == ErrWorkflowTaskAlreadyStarted {
		fmt.Println("Successfully prevented duplicate task execution (Task already active).")
	} else {
		fmt.Printf("Unexpected result: %v\n", err)
	}

	// Scenario 3: Shard Failover (RangeID increments to 101)
	fmt.Println("\n--- Scenario 3: Shard Failover (RangeID increments to 101) ---")
	shardCtx.UpdateRangeID(101)
	engine2 := NewHistoryEngine(shardCtx, db) // New owner engine

	// Stale engine (engine1) tries to update with old RangeID 100
	fmt.Println("Stale engine (RangeID 100) attempts to record task started...")
	err = engine1.RecordWorkflowTaskStarted(workflowID, 100, timeout)
	if err == ErrShardOwnershipLost {
		fmt.Println("Successfully rejected stale engine update (Fencing active).")
	} else {
		fmt.Printf("Unexpected result: %v\n", err)
	}

	// New engine (engine2) attempts to record task started with RangeID 101
	fmt.Println("New engine (RangeID 101) attempts to record task started...")
	err = engine2.RecordWorkflowTaskStarted(workflowID, 101, timeout)
	if err == ErrWorkflowTaskAlreadyStarted {
		fmt.Println("Successfully prevented duplicate task execution on new shard owner.")
	} else {
		fmt.Printf("Unexpected result: %v\n", err)
	}

	// Scenario 4: Task times out, new dispatch allowed
	fmt.Println("\n--- Scenario 4: Task Times Out ---")
	// Simulate timeout by modifying the started time in DB
	db.mu.Lock()
	db.workflows[workflowID].WorkflowTaskStartedTime = time.Now().Add(-6 * time.Second)
	db.mu.Unlock()

	fmt.Println("Attempting dispatch after timeout...")
	err = engine2.RecordWorkflowTaskStarted(workflowID, 101, timeout)
	if err == nil {
		fmt.Println("Workflow task started successfully after timeout.")
	} else {
		fmt.Printf("Error: %v\n", err)
	}

	// Verify history has exactly 2 WorkflowTaskStarted events (one from initial, one from after timeout)
	state, _ = db.GetWorkflow(workflowID)
	fmt.Printf("Final History events: %v\n", state.History)
}
