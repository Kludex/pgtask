package main

import (
	"fmt"

	"github.com/anishathalye/porcupine"
)

// RegisterState is a write-once cell: empty until something wins, then fixed.
type RegisterState struct {
	Written bool
	Value   string
}

// buildRegisterModel covers the two places pgtask promises first-write-wins.
//
// `commit_checkpoint` and `emit_signal` both insert `ON CONFLICT DO UPDATE SET
// value = <existing>`, and both return the row that ended up there. So the
// second writer of a step or a signal gets the FIRST writer's value back, which
// is the whole basis of durable execution: a handler that replays a step must
// see what the original run recorded, not what this run computed.
//
// The property is that the cell never changes value once set, and that every
// caller -- writer or reader -- is told the same thing. A value that changes
// between replays is silent workflow corruption, and it is exactly the kind of
// failure that looks fine in any single snapshot.
//
// The model is nondeterministic because a write that was cut off mid-flight may
// or may not have landed. An indeterminate write into an empty cell leaves two
// possible worlds, and both are explored.
//
// Operations:
//
//	write(value) -> {ok, value}   ok=false means the write was fenced out
//	read()       -> {present, value}
func buildRegisterModel() porcupine.Model {
	model := porcupine.NondeterministicModel{
		PartitionEvent: partitionEventByKey,
		Init: func() []any {
			return []any{RegisterState{}}
		},
		Step: func(stateAny, inputAny, outputAny any) []any {
			state := stateAny.(RegisterState)
			in := inputAny.(Keyed).Input
			out := outputAny.(Output)

			if out.Unknown {
				// A read that never returned tells us nothing at all.
				if in.Op == "read" {
					return only(state)
				}
				if state.Written {
					// Already settled, so the write could not have changed it
					// whether it landed or not.
					return only(state)
				}
				// It either won the empty cell or never arrived.
				return []any{state, RegisterState{Written: true, Value: string(in.Value)}}
			}

			switch in.Op {
			case "write":
				if !out.OK {
					// Fenced out, so nothing was written.
					return only(state)
				}
				if !state.Written {
					// First writer wins, and is handed back its own value.
					if string(out.Value) != string(in.Value) {
						return none()
					}
					return only(RegisterState{Written: true, Value: string(out.Value)})
				}
				// Someone got there first, so this writer must be told the
				// established value, not its own.
				if string(out.Value) != state.Value {
					return none()
				}
				return only(state)

			case "read":
				if out.Present != state.Written {
					return none()
				}
				if out.Present && string(out.Value) != state.Value {
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
				return fmt.Sprintf("%s(%s) -> UNKNOWN (cut off)", in.Op, in.Value)
			}
			if in.Op == "read" {
				return fmt.Sprintf("read() -> present=%v value=%s", out.Present, out.Value)
			}
			return fmt.Sprintf("write(%s) -> ok=%v value=%s", in.Value, out.OK, out.Value)
		},
		DescribeState: func(stateAny any) string {
			state := stateAny.(RegisterState)
			if !state.Written {
				return "empty"
			}
			return fmt.Sprintf("value=%s", state.Value)
		},
	}
	return model.ToModel()
}
