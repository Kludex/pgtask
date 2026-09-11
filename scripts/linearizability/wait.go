package main

import (
	"fmt"

	"github.com/anishathalye/porcupine"
)

// WaitState is one task parked, or not, on one wait step.
type WaitState struct {
	// Source is set once the thing being waited for has happened: a signal
	// emitted, or a child reaching a terminal state.
	Source bool
	// Parked is set while the task sits in `waiting`.
	Parked bool
}

// buildWaitModel covers parking a task and waking it again.
//
// Both wait paths have the same shape: read whatever is being waited for, and
// if it is not there yet, register a wait row and park the task. Something else
// is then expected to notice the registration and wake it.
//
// The promise, from docs/sql-protocol.md, is that "emitting before or after the
// waiter registers gives the same result -- there is no lost-wakeup window to
// design around". As a linearizable object that reads:
//
//	wait   -> "ready"   only if the source is already there
//	       -> "waiting" only if it is not, and then the task is parked
//	wake   -> the source is there, and nothing is left parked
//	observe-> reports whether the task is still parked
//
// Those three together are what make a lost wake-up a linearizability
// violation rather than merely a hang. If a wait returned "waiting" it must
// have been ordered before the wake, and the wake must therefore have
// unparked it -- so observing the task still parked afterwards has no
// consistent ordering. Had the wait been ordered after the wake, it would have
// had to return "ready".
//
// That is #23 exactly, and it means this model finds that bug without anyone
// having to describe it first.
func buildWaitModel() porcupine.Model {
	model := porcupine.NondeterministicModel{
		PartitionEvent: partitionEventByKey,
		Init: func() []any {
			return []any{WaitState{}}
		},
		Step: func(stateAny, inputAny, outputAny any) []any {
			state := stateAny.(WaitState)
			in := inputAny.(Keyed).Input
			out := outputAny.(Output)

			switch in.Op {
			case "wait":
				if out.Unknown {
					// Registering may or may not have landed.
					if state.Source {
						return only(state)
					}
					return []any{state, WaitState{Source: false, Parked: true}}
				}
				switch out.State {
				case "ready":
					// Told the source was already there, so it must have been.
					if !state.Source {
						return none()
					}
					return only(state)
				case "waiting":
					// Told to park, so the source must not have arrived yet.
					if state.Source {
						return none()
					}
					return only(WaitState{Source: false, Parked: true})
				case "lost":
					// Fenced out; the lease had moved on. Nothing changed.
					return only(state)
				}
				return none()

			case "wake":
				if out.Unknown {
					next := WaitState{Source: true, Parked: false}
					return []any{state, next}
				}
				// The source is now there, and anything parked on it is
				// released. A wake that leaves a waiter parked is the failure
				// this whole model exists to catch.
				return only(WaitState{Source: true, Parked: false})

			case "observe":
				if out.Unknown {
					return only(state)
				}
				if out.Parked != state.Parked {
					return none()
				}
				return only(state)
			}
			return none()
		},
		DescribeOperation: func(inputAny, outputAny any) string {
			in := inputAny.(Keyed).Input
			out := outputAny.(Output)
			if out.Unknown {
				return fmt.Sprintf("%s() -> UNKNOWN (cut off)", in.Op)
			}
			switch in.Op {
			case "observe":
				return fmt.Sprintf("observe() -> parked=%v", out.Parked)
			case "wait":
				return fmt.Sprintf("wait() -> %s", out.State)
			}
			return "wake()"
		},
		DescribeState: func(stateAny any) string {
			state := stateAny.(WaitState)
			return fmt.Sprintf("source=%v parked=%v", state.Source, state.Parked)
		},
	}
	return model.ToModel()
}
