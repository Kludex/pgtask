package main

import (
	"fmt"

	"github.com/anishathalye/porcupine"
)

// CapacityState is how many tasks a queue currently counts as outstanding.
type CapacityState struct {
	Outstanding int
}

// buildCapacityModel checks the admission control on a capacity-limited queue.
//
// From the docs: "When a queue sets max_outstanding_tasks, admission above the
// limit fails with SQLSTATE PT001", and "Queue capacity is backpressure, not
// task loss."
//
// The implementation does not count rows at admission time. It keeps a running
// `capacity_outstanding_tasks` on the queue row, incremented by a trigger on
// insert and decremented when a task reaches a terminal state. That is
// denormalised state, and denormalised state can drift: a counter that drifts
// low admits more than the limit allows, and one that drifts high starts
// rejecting work while the queue has room. Neither shows up in a snapshot of
// the tasks table, because the tasks table is not what admission consults.
//
// Modelling it as a bounded counter makes both directions detectable, since an
// admission decision that disagrees with the true outstanding count has no
// ordering that explains it.
//
//	enqueue()  -> {ok}   ok=false means PT001
//	complete() -> {ok}   frees a slot
func buildCapacityModel(capacity int) porcupine.Model {
	return porcupine.Model{
		Partition: partitionByKey,
		Init: func() any {
			return CapacityState{}
		},
		Step: func(stateAny, inputAny, outputAny any) (bool, any) {
			state := stateAny.(CapacityState)
			in := inputAny.(Keyed).Input
			out := outputAny.(Output)

			switch in.Op {
			case "enqueue":
				if out.OK {
					// Admitted, so there had to be room.
					if state.Outstanding >= capacity {
						return false, state
					}
					return true, CapacityState{Outstanding: state.Outstanding + 1}
				}
				// Rejected, so the queue had to be full. Rejecting while there
				// is room is backpressure applied to work that should have been
				// accepted, and is just as wrong as admitting past the limit.
				if state.Outstanding < capacity {
					return false, state
				}
				return true, state

			case "complete":
				if !out.OK {
					return true, state
				}
				// Something outstanding must have finished for this to succeed.
				if state.Outstanding == 0 {
					return false, state
				}
				return true, CapacityState{Outstanding: state.Outstanding - 1}
			}
			return false, state
		},
		DescribeOperation: func(inputAny, outputAny any) string {
			in := inputAny.(Keyed).Input
			out := outputAny.(Output)
			return fmt.Sprintf("%s() -> ok=%v", in.Op, out.OK)
		},
		DescribeState: func(stateAny any) string {
			state := stateAny.(CapacityState)
			return fmt.Sprintf("outstanding=%d/%d", state.Outstanding, capacity)
		},
	}
}
