// Linearizability checking for pgtask.
//
// The Rust side records operations against a real database, with the wall-clock
// interval each call occupied. This reads that history and asks Porcupine
// whether some sequential ordering of those calls explains the results, given
// the reference model named by --model.
//
// That is a stronger question than "did any invariant hold". An invariant check
// looks at states one at a time; this asks whether the whole concurrent history
// could have been produced by a machine that never breaks its own rules. A stale
// write that succeeds, or a durable step whose value changes, has no valid
// ordering, so it fails here even if every individual snapshot looked fine.
//
//	go run . --model lease --history history.json
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/anishathalye/porcupine"
)

// Input is one operation as the Rust side issued it.
type Input struct {
	Op      string          `json:"op"`
	Attempt int             `json:"attempt"`
	Token   string          `json:"token"`
	Value   json.RawMessage `json:"value"`
}

// Output is what the database returned.
type Output struct {
	OK      bool            `json:"ok"`
	Attempt int             `json:"attempt"`
	Token   string          `json:"token"`
	State   string          `json:"state"`
	Present bool            `json:"present"`
	Value   json.RawMessage `json:"value"`
	TaskID  string          `json:"task_id"`
	Created bool            `json:"created"`
}

type Operation struct {
	ClientID  int    `json:"client_id"`
	Partition string `json:"partition"`
	Call      int64  `json:"call"`
	Return    int64  `json:"return"`
	Input     Input  `json:"input"`
	Output    Output `json:"output"`
}

type History struct {
	Model       string      `json:"model"`
	MaxAttempts int         `json:"max_attempts"`
	Capacity    int         `json:"capacity"`
	Operations  []Operation `json:"operations"`
}

// Keyed carries the partition key alongside the input, because Porcupine's
// Partition callback only sees inputs.
type Keyed struct {
	Partition string
	Input     Input
}

// partitionByKey splits a history into one independent sub-history per key.
// pgtask makes no cross-key ordering promise -- `claim` uses SKIP LOCKED
// precisely so two workers get different tasks -- so what must hold is the
// sequence of operations on each key on its own.
func partitionByKey(history []porcupine.Operation) [][]porcupine.Operation {
	byKey := make(map[string][]porcupine.Operation)
	var order []string
	for _, op := range history {
		key := op.Input.(Keyed).Partition
		if _, seen := byKey[key]; !seen {
			order = append(order, key)
		}
		byKey[key] = append(byKey[key], op)
	}
	partitions := make([][]porcupine.Operation, 0, len(order))
	for _, key := range order {
		partitions = append(partitions, byKey[key])
	}
	return partitions
}

func buildModel(name string, history History) (porcupine.Model, error) {
	switch name {
	case "lease":
		return buildLeaseModel(history.MaxAttempts), nil
	case "register":
		return buildRegisterModel(), nil
	case "idempotency":
		return buildIdempotencyModel(), nil
	case "capacity":
		if history.Capacity <= 0 {
			return porcupine.Model{}, fmt.Errorf("the capacity model needs a positive capacity")
		}
		return buildCapacityModel(history.Capacity), nil
	}
	return porcupine.Model{}, fmt.Errorf("unknown model %q (want lease, register, idempotency or capacity)", name)
}

func main() {
	historyPath := flag.String("history", "", "path to the JSON history")
	modelName := flag.String("model", "", "reference model: lease, register, idempotency or capacity (default: the history's own)")
	timeout := flag.Duration("timeout", 60*time.Second, "checking timeout")
	expectFail := flag.Bool("expect-fail", false, "exit 0 only if the history is NOT linearizable")
	flag.Parse()

	if *historyPath == "" {
		fmt.Fprintln(os.Stderr, "--history is required")
		os.Exit(2)
	}

	raw, err := os.ReadFile(*historyPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not read history: %v\n", err)
		os.Exit(2)
	}
	var history History
	if err := json.Unmarshal(raw, &history); err != nil {
		fmt.Fprintf(os.Stderr, "could not parse history: %v\n", err)
		os.Exit(2)
	}
	if len(history.Operations) == 0 {
		fmt.Fprintln(os.Stderr, "history is empty, so it proves nothing")
		os.Exit(2)
	}

	name := *modelName
	if name == "" {
		name = history.Model
	}
	model, err := buildModel(name, history)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	operations := make([]porcupine.Operation, 0, len(history.Operations))
	for _, op := range history.Operations {
		operations = append(operations, porcupine.Operation{
			ClientId: op.ClientID,
			Input:    Keyed{Partition: op.Partition, Input: op.Input},
			Call:     op.Call,
			Output:   op.Output,
			Return:   op.Return,
		})
	}

	result, info := porcupine.CheckOperationsVerbose(model, operations, *timeout)

	switch result {
	case porcupine.Ok:
		fmt.Printf("linearizable: %d %s operations across %d partitions\n",
			len(operations), name, countPartitions(history.Operations))
		if *expectFail {
			fmt.Fprintln(os.Stderr, "expected this history to be rejected, but it linearized")
			os.Exit(1)
		}
	case porcupine.Illegal:
		fmt.Printf("NOT linearizable: %d %s operations\n", len(operations), name)
		if path, err := writeVisualization(model, info); err == nil {
			fmt.Printf("visualization: %s\n", path)
		}
		if !*expectFail {
			os.Exit(1)
		}
	case porcupine.Unknown:
		fmt.Fprintf(os.Stderr, "checker timed out after %s; history may be too large\n", *timeout)
		os.Exit(2)
	}
}

func countPartitions(operations []Operation) int {
	keys := make(map[string]struct{})
	for _, op := range operations {
		keys[op.Partition] = struct{}{}
	}
	return len(keys)
}

func writeVisualization(model porcupine.Model, info porcupine.LinearizationInfo) (string, error) {
	file, err := os.CreateTemp("", "pgtask-linearizability-*.html")
	if err != nil {
		return "", err
	}
	defer file.Close()
	if err := porcupine.Visualize(model, info, file); err != nil {
		return "", err
	}
	return file.Name(), nil
}
