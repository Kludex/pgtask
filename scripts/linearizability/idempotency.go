package main

import (
	"fmt"

	"github.com/anishathalye/porcupine"
)

// IdempotencyState is one reservation: which task the key resolves to, once
// somebody has claimed it.
type IdempotencyState struct {
	Reserved bool
	TaskID   string
}

// buildIdempotencyModel checks what `pgtask.enqueue` promises for a key.
//
// From the docs: "idempotency_key makes a repeated enqueue return the same
// task", and the returned `created` flag says whether this call is the one that
// made it. So for a given key exactly one caller may ever be told created=true,
// and every caller -- winner and losers alike -- must be handed the same task id.
//
// This is a compare-and-set register, and the interesting part is that the SQL
// implementing it is not a plain INSERT. `enqueue` does an
// `INSERT ... ON CONFLICT DO UPDATE ... WHERE <the reservation has expired>`,
// then falls back to a separate SELECT when that update matches nothing. Two
// callers racing on one key take different paths through that, which is exactly
// the sort of thing a sequential test never reaches.
//
// Two callers both told created=true would mean two tasks for one key -- the
// duplicate the feature exists to prevent -- and neither ordering explains it.
func buildIdempotencyModel() porcupine.Model {
	return porcupine.Model{
		Partition: partitionByKey,
		Init: func() any {
			return IdempotencyState{}
		},
		Step: func(stateAny, inputAny, outputAny any) (bool, any) {
			state := stateAny.(IdempotencyState)
			in := inputAny.(Keyed).Input
			out := outputAny.(Output)

			if in.Op != "enqueue" {
				return false, state
			}
			// An enqueue that failed (capacity, validation) leaves no trace.
			if !out.OK {
				return true, state
			}
			if !state.Reserved {
				// Nobody had this key, so this caller must be the one told it
				// created the task.
				if !out.Created {
					return false, state
				}
				return true, IdempotencyState{Reserved: true, TaskID: out.TaskID}
			}
			// The key was already reserved, so this caller must be told it did
			// not create anything, and must be handed the original id.
			if out.Created || out.TaskID != state.TaskID {
				return false, state
			}
			return true, state
		},
		DescribeOperation: func(inputAny, outputAny any) string {
			out := outputAny.(Output)
			return fmt.Sprintf("enqueue() -> task=%.8s created=%v", out.TaskID, out.Created)
		},
		DescribeState: func(stateAny any) string {
			state := stateAny.(IdempotencyState)
			if !state.Reserved {
				return "unreserved"
			}
			return fmt.Sprintf("reserved by %.8s", state.TaskID)
		},
	}
}
