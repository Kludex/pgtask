package main

import (
	"fmt"

	"github.com/anishathalye/porcupine"
)

// LeaseState is one task, as the reference model sees it.
type LeaseState struct {
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
func (s LeaseState) owns(in Input) bool {
	return s.Phase == running && s.Attempt == in.Attempt && s.Token == in.Token
}

// only is the deterministic case: exactly one possible next state.
func only(state any) []any { return []any{state} }

// none rejects: no state explains this result.
func none() []any { return nil }

// buildLeaseModel is the claim/renew/complete/fail/recover machine.
//
// It is nondeterministic because of fault injection. When a call is cut off
// mid-flight the client never learns whether the write landed, and the database
// may well have committed it anyway. Such an operation is recorded with an
// unknown outcome, and the model then returns *both* possibilities: the state
// where it applied and the state where it did not. Porcupine explores both.
//
// That is the point of injecting faults at all. pgtask's failure model claims
// "the client treats the transaction outcome as unknown ... every mutation is
// idempotent for the same task, attempt, and lease token", and an indeterminate
// write is exactly the situation that claim is about.
//
// Faults are never injected into `claim`, because a claim whose result was lost
// would have minted a lease token the client never saw, and no model can say
// what state that left behind.
func buildLeaseModel(maxAttempts int) porcupine.Model {
	// applied is where a successful write of `in` leaves `state`.
	applied := func(state LeaseState, in Input) (LeaseState, bool) {
		switch in.Op {
		case "complete":
			return LeaseState{Phase: succeeded, Attempt: state.Attempt}, true
		case "fail", "recover":
			if state.Attempt < maxAttempts {
				return LeaseState{Phase: pending, Attempt: state.Attempt}, true
			}
			return LeaseState{Phase: failed, Attempt: state.Attempt}, true
		case "renew":
			return state, true
		}
		return state, false
	}

	model := porcupine.NondeterministicModel{
		PartitionEvent: partitionEventByKey,
		Init: func() []any {
			return []any{LeaseState{Phase: pending, Attempt: 0}}
		},
		Step: func(stateAny, inputAny, outputAny any) []any {
			state := stateAny.(LeaseState)
			in := inputAny.(Keyed).Input
			out := outputAny.(Output)

			// The client never learned the outcome, so both the world where it
			// took effect and the world where it did not are on the table.
			if out.Unknown {
				if in.Op == "recover" {
					if state.Phase != running {
						return only(state)
					}
					next, ok := applied(state, in)
					if !ok {
						return none()
					}
					return []any{state, next}
				}
				if !state.owns(in) {
					// Fenced out either way, so nothing can have changed.
					return only(state)
				}
				next, ok := applied(state, in)
				if !ok {
					return none()
				}
				return []any{state, next}
			}

			switch in.Op {
			case "claim":
				// A claim that returned this task must have found it pending
				// with attempts to spare, and must have bumped the attempt by
				// exactly one.
				if state.Phase != pending || state.Attempt >= maxAttempts {
					return none()
				}
				if out.Attempt != state.Attempt+1 {
					return none()
				}
				return only(LeaseState{Phase: running, Attempt: out.Attempt, Token: out.Token})

			case "complete", "fail":
				if out.OK {
					// Accepted, so this lease had to be the live one.
					if !state.owns(in) {
						return none()
					}
					next, _ := applied(state, in)
					// fail also reports which state it landed in.
					if in.Op == "fail" && out.State != next.Phase {
						return none()
					}
					return only(next)
				}
				// Rejected, so this lease must NOT have been the live one. A
				// rejection while holding the live lease is just as wrong as an
				// acceptance while holding a stale one.
				if state.owns(in) {
					return none()
				}
				return only(state)

			case "renew":
				if out.OK != state.owns(in) {
					return none()
				}
				return only(state)

			case "recover":
				// Recovery reclaims an expired lease. It leaves the attempt
				// alone, so the task comes back claimable only while it has
				// attempts left.
				if out.OK {
					if state.Phase != running {
						return none()
					}
					next, _ := applied(state, in)
					return only(next)
				}
				return only(state)
			}
			return none()
		},
		DescribeOperation: func(inputAny, outputAny any) string {
			in := inputAny.(Keyed).Input
			out := outputAny.(Output)
			if out.Unknown {
				return fmt.Sprintf("%s(attempt=%d, token=%.8s) -> UNKNOWN (cut off)",
					in.Op, in.Attempt, in.Token)
			}
			return fmt.Sprintf("%s(attempt=%d, token=%.8s) -> ok=%v attempt=%d state=%s",
				in.Op, in.Attempt, in.Token, out.OK, out.Attempt, out.State)
		},
		DescribeState: func(stateAny any) string {
			state := stateAny.(LeaseState)
			return fmt.Sprintf("%s(attempt=%d, token=%.8s)", state.Phase, state.Attempt, state.Token)
		},
	}
	return model.ToModel()
}
