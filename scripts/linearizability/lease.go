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

// buildLeaseModel is the claim/renew/complete/fail/recover machine.
func buildLeaseModel(maxAttempts int) porcupine.Model {
	return porcupine.Model{
		Partition: partitionByKey,
		Init: func() any {
			return LeaseState{Phase: pending, Attempt: 0}
		},
		Step: func(stateAny, inputAny, outputAny any) (bool, any) {
			state := stateAny.(LeaseState)
			in := inputAny.(Keyed).Input
			out := outputAny.(Output)

			switch in.Op {
			case "claim":
				// A claim that returned this task must have found it pending
				// with attempts to spare, and must have bumped the attempt by
				// exactly one.
				if state.Phase != pending || state.Attempt >= maxAttempts {
					return false, state
				}
				if out.Attempt != state.Attempt+1 {
					return false, state
				}
				return true, LeaseState{Phase: running, Attempt: out.Attempt, Token: out.Token}

			case "complete":
				if out.OK {
					// Accepted, so this lease had to be the live one.
					if !state.owns(in) {
						return false, state
					}
					return true, LeaseState{Phase: succeeded, Attempt: state.Attempt}
				}
				// Rejected, so this lease must NOT have been the live one. A
				// rejection while holding the live lease is just as wrong as an
				// acceptance while holding a stale one.
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
						return true, LeaseState{Phase: pending, Attempt: state.Attempt}
					}
					if out.State != failed {
						return false, state
					}
					return true, LeaseState{Phase: failed, Attempt: state.Attempt}
				}
				if state.owns(in) {
					return false, state
				}
				return true, state

			case "renew":
				if out.OK != state.owns(in) {
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
						return true, LeaseState{Phase: pending, Attempt: state.Attempt}
					}
					return true, LeaseState{Phase: failed, Attempt: state.Attempt}
				}
				return true, state
			}
			return false, state
		},
		DescribeOperation: func(inputAny, outputAny any) string {
			in := inputAny.(Keyed).Input
			out := outputAny.(Output)
			return fmt.Sprintf("%s(attempt=%d, token=%.8s) -> ok=%v attempt=%d state=%s",
				in.Op, in.Attempt, in.Token, out.OK, out.Attempt, out.State)
		},
		DescribeState: func(stateAny any) string {
			state := stateAny.(LeaseState)
			return fmt.Sprintf("%s(attempt=%d, token=%.8s)", state.Phase, state.Attempt, state.Token)
		},
	}
}
