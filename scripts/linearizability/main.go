// Linearizability checker for pgtask's lease protocol.
//
// The Rust side records every lease operation it performs against a real
// database, with the wall-clock interval each call occupied. This reads that
// history and asks Porcupine whether some sequential ordering of those calls
// explains the results, given the reference model below.
//
// That is a stronger question than "did any invariant hold". An invariant check
// looks at states one at a time; this asks whether the whole concurrent history
// could have been produced by a machine that never breaks its own rules. A stale
// write that succeeds has no valid ordering, so it fails here even if every
// individual state snapshot looked fine.
//
// Operations are partitioned by task, because pgtask makes no cross-task
// ordering promise: `claim` uses SKIP LOCKED, so two workers deliberately get
// different tasks. What must hold is the sequence of transitions for each task
// on its own.
//
//	go run . --history history.json
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/anishathalye/porcupine"
)

// Input is one lease operation as the Rust side issued it.
type Input struct {
	Op      string `json:"op"`
	Attempt int    `json:"attempt"`
	Token   string `json:"token"`
}

// Output is what the database returned.
type Output struct {
	OK      bool   `json:"ok"`
	Attempt int    `json:"attempt"`
	Token   string `json:"token"`
	State   string `json:"state"`
}

type Operation struct {
	ClientID int    `json:"client_id"`
	Task     string `json:"task"`
	Call     int64  `json:"call"`
	Return   int64  `json:"return"`
	Input    Input  `json:"input"`
	Output   Output `json:"output"`
}

type History struct {
	MaxAttempts int         `json:"max_attempts"`
	Operations  []Operation `json:"operations"`
}

// State is one task, as the reference model sees it.
type State struct {
	Phase   string // pending, running, succeeded, failed
	Attempt int
	Token   string
}

const (
	pending   = "pending"
	running   = "running"
	succeeded = "succeeded"
	failed    = "failed"
)

// owns reports whether the presented lease is the live one. This is the whole
// fencing rule: state, attempt and token must all still match.
func (s State) owns(in Input) bool {
	return s.Phase == running && s.Attempt == in.Attempt && s.Token == in.Token
}

func buildModel(maxAttempts int) porcupine.Model {
	return porcupine.Model{
		// One independent history per task.
		Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
			byTask := make(map[string][]porcupine.Operation)
			var order []string
			for _, op := range history {
				task := op.Input.(Input2).Task
				if _, seen := byTask[task]; !seen {
					order = append(order, task)
				}
				byTask[task] = append(byTask[task], op)
			}
			partitions := make([][]porcupine.Operation, 0, len(order))
			for _, task := range order {
				partitions = append(partitions, byTask[task])
			}
			return partitions
		},
		Init: func() any {
			return State{Phase: pending, Attempt: 0}
		},
		Step: func(stateAny, inputAny, outputAny any) (bool, any) {
			state := stateAny.(State)
			in := inputAny.(Input2).Input
			out := outputAny.(Output)

			switch in.Op {
			case "claim":
				// A claim that returned this task must have found it pending
				// with attempts to spare, and must have bumped the attempt by
				// exactly one.
				if state.Phase != pending {
					return false, state
				}
				if state.Attempt >= maxAttempts {
					return false, state
				}
				if out.Attempt != state.Attempt+1 {
					return false, state
				}
				return true, State{Phase: running, Attempt: out.Attempt, Token: out.Token}

			case "complete":
				if out.OK {
					// Accepted, so this lease had to be the live one.
					if !state.owns(in) {
						return false, state
					}
					return true, State{Phase: succeeded, Attempt: state.Attempt}
				}
				// Rejected, so this lease must NOT have been the live one.
				// A rejection while holding the live lease is just as wrong as
				// an acceptance while holding a stale one.
				if state.owns(in) {
					return false, state
				}
				return true, state

			case "fail":
				if out.OK {
					if !state.owns(in) {
						return false, state
					}
					// Retries while attempts remain, otherwise terminal.
					if state.Attempt < maxAttempts {
						if out.State != pending {
							return false, state
						}
						return true, State{Phase: pending, Attempt: state.Attempt}
					}
					if out.State != failed {
						return false, state
					}
					return true, State{Phase: failed, Attempt: state.Attempt}
				}
				if state.owns(in) {
					return false, state
				}
				return true, state

			case "renew":
				if out.OK {
					if !state.owns(in) {
						return false, state
					}
					return true, state
				}
				if state.owns(in) {
					return false, state
				}
				return true, state

			case "recover":
				// Recovery reclaims an expired lease. It leaves the attempt
				// alone, so the task comes back claimable only while it has
				// attempts left.
				if out.OK {
					if state.Phase != running {
						return false, state
					}
					if state.Attempt < maxAttempts {
						return true, State{Phase: pending, Attempt: state.Attempt}
					}
					return true, State{Phase: failed, Attempt: state.Attempt}
				}
				return true, state
			}
			return false, state
		},
		DescribeOperation: func(inputAny, outputAny any) string {
			in := inputAny.(Input2).Input
			out := outputAny.(Output)
			return fmt.Sprintf("%s(attempt=%d, token=%.8s) -> ok=%v attempt=%d state=%s",
				in.Op, in.Attempt, in.Token, out.OK, out.Attempt, out.State)
		},
		DescribeState: func(stateAny any) string {
			state := stateAny.(State)
			return fmt.Sprintf("%s(attempt=%d, token=%.8s)", state.Phase, state.Attempt, state.Token)
		},
	}
}

// Input2 carries the task alongside the input so Partition can see it.
type Input2 struct {
	Task  string
	Input Input
}

func main() {
	historyPath := flag.String("history", "", "path to the JSON history")
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

	operations := make([]porcupine.Operation, 0, len(history.Operations))
	for _, op := range history.Operations {
		operations = append(operations, porcupine.Operation{
			ClientId: op.ClientID,
			Input:    Input2{Task: op.Task, Input: op.Input},
			Call:     op.Call,
			Output:   op.Output,
			Return:   op.Return,
		})
	}

	result, info := porcupine.CheckOperationsVerbose(buildModel(history.MaxAttempts), operations, *timeout)

	switch result {
	case porcupine.Ok:
		fmt.Printf("linearizable: %d operations across %d tasks\n",
			len(operations), countTasks(history.Operations))
		if *expectFail {
			fmt.Fprintln(os.Stderr, "expected this history to be rejected, but it linearized")
			os.Exit(1)
		}
	case porcupine.Illegal:
		fmt.Printf("NOT linearizable: %d operations\n", len(operations))
		if path, err := writeVisualization(buildModel(history.MaxAttempts), info); err == nil {
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

func countTasks(operations []Operation) int {
	tasks := make(map[string]struct{})
	for _, op := range operations {
		tasks[op.Task] = struct{}{}
	}
	return len(tasks)
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
