--------------------------- MODULE TaskLifecycle ---------------------------
(***************************************************************************)
(* The pgtask claim/lease/retry/recovery core.                              *)
(*                                                                          *)
(* Delivery is at-least-once, so two workers running the same task at once   *)
(* is allowed and expected. What must never happen is two of them writing    *)
(* a result. pgtask fences with (task id, state=running, attempt, lease      *)
(* token): a mutation applies only while all four still match.              *)
(*                                                                          *)
(* The interesting case is a worker whose lease expired but whose handler is *)
(* still running. Recovery returns the task to `pending`, another worker      *)
(* claims it with a fresh attempt and token, and the original worker then     *)
(* tries to record its result. That write has to be rejected.                *)
(*                                                                          *)
(* `inflight` is the set of handler executions that still believe they own a  *)
(* task. Expiry deliberately leaves them there, which is what makes the       *)
(* fencing check meaningful.                                                 *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Tasks, Workers, MaxAttempts

ASSUME MaxAttempts \in Nat /\ MaxAttempts > 0

VARIABLES
    state,      \* state[t]: the task's state column
    attempt,    \* attempt[t]: the attempt counter
    lease,      \* lease[t]: the fencing token, 0 when there is no lease
    inflight,   \* handler executions that think they hold a lease
    nextToken   \* hands out unique lease tokens

vars == <<state, attempt, lease, inflight, nextToken>>

Terminal == {"succeeded", "failed", "cancelled"}
States == {"pending", "running"} \union Terminal

\* Every claim mints one token and each task is claimed at most MaxAttempts
\* times, which bounds the token space for the model checker.
MaxTokens == Cardinality(Tasks) * MaxAttempts

\* A handler execution: worker w believes it holds task t at (a, tok).
Handlers == [w: Workers, t: Tasks, a: 1..MaxAttempts, tok: 1..MaxTokens]

TypeOK ==
    /\ state \in [Tasks -> States]
    /\ attempt \in [Tasks -> 0..MaxAttempts]
    /\ lease \in [Tasks -> Nat]
    /\ inflight \subseteq Handlers
    /\ nextToken \in 1..(MaxTokens + 1)

Init ==
    /\ state = [t \in Tasks |-> "pending"]
    /\ attempt = [t \in Tasks |-> 0]
    /\ lease = [t \in Tasks |-> 0]
    /\ inflight = {}
    /\ nextToken = 1

\* The fencing predicate, exactly as the SQL WHERE clauses spell it out:
\* id matches, state is running, attempt matches, lease token matches.
Owns(h) ==
    /\ state[h.t] = "running"
    /\ attempt[h.t] = h.a
    /\ lease[h.t] = h.tok

(***************************************************************************)
(* pgtask.claim                                                             *)
(***************************************************************************)
Claim(w, t) ==
    /\ state[t] = "pending"
    /\ attempt[t] < MaxAttempts          \* claim filters attempt < max_attempts
    /\ state' = [state EXCEPT ![t] = "running"]
    /\ attempt' = [attempt EXCEPT ![t] = @ + 1]
    /\ lease' = [lease EXCEPT ![t] = nextToken]
    /\ nextToken' = nextToken + 1
    /\ inflight' = inflight \union
         {[w |-> w, t |-> t, a |-> attempt[t] + 1, tok |-> nextToken]}

(***************************************************************************)
(* pgtask.complete_task / fail_task, both fenced                            *)
(***************************************************************************)
Complete(h) ==
    /\ h \in inflight
    /\ Owns(h)
    /\ state' = [state EXCEPT ![h.t] = "succeeded"]
    /\ lease' = [lease EXCEPT ![h.t] = 0]
    /\ inflight' = inflight \ {h}
    /\ UNCHANGED <<attempt, nextToken>>

FailWithRetry(h) ==
    /\ h \in inflight
    /\ Owns(h)
    /\ attempt[h.t] < MaxAttempts
    /\ state' = [state EXCEPT ![h.t] = "pending"]
    /\ lease' = [lease EXCEPT ![h.t] = 0]
    /\ inflight' = inflight \ {h}
    /\ UNCHANGED <<attempt, nextToken>>

FailTerminally(h) ==
    /\ h \in inflight
    /\ Owns(h)
    /\ state' = [state EXCEPT ![h.t] = "failed"]
    /\ lease' = [lease EXCEPT ![h.t] = 0]
    /\ inflight' = inflight \ {h}
    /\ UNCHANGED <<attempt, nextToken>>

\* A handler that no longer owns its task discovers this and gives up. The
\* SQL matched zero rows, so nothing was written.
LeaseLost(h) ==
    /\ h \in inflight
    /\ ~Owns(h)
    /\ inflight' = inflight \ {h}
    /\ UNCHANGED <<state, attempt, lease, nextToken>>

(***************************************************************************)
(* pgtask.recover_expired                                                   *)
(*                                                                          *)
(* Time is abstracted away: a lease may expire at any moment while running.  *)
(* Crucially this does NOT clear `inflight`, because the worker's handler is  *)
(* still going and will try to write its result later.                       *)
(***************************************************************************)
ExpireLease(t) ==
    /\ state[t] = "running"
    /\ state' = [state EXCEPT ![t] =
                    IF attempt[t] < MaxAttempts THEN "pending" ELSE "failed"]
    /\ lease' = [lease EXCEPT ![t] = 0]
    /\ UNCHANGED <<attempt, inflight, nextToken>>

(***************************************************************************)
(* pgtask.cancel_task on a task that is not running                         *)
(***************************************************************************)
Cancel(t) ==
    /\ state[t] = "pending"
    /\ state' = [state EXCEPT ![t] = "cancelled"]
    /\ UNCHANGED <<attempt, lease, inflight, nextToken>>

Next ==
    \/ \E w \in Workers, t \in Tasks : Claim(w, t)
    \/ \E h \in inflight :
         Complete(h) \/ FailWithRetry(h) \/ FailTerminally(h) \/ LeaseLost(h)
    \/ \E t \in Tasks : ExpireLease(t) \/ Cancel(t)
    \/ (\A t \in Tasks : state[t] \in Terminal) /\ inflight = {} /\ UNCHANGED vars

Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ \A t \in Tasks : WF_vars(ExpireLease(t))
    /\ \A w \in Workers, t \in Tasks : WF_vars(Claim(w, t))
    /\ WF_vars(\E h \in inflight : Complete(h) \/ LeaseLost(h))

(***************************************************************************)
(* Safety                                                                   *)
(***************************************************************************)

\* At most one handler can pass the fencing check for a task at any time.
\* Many may be running; only one can write.
AtMostOneFencedWriter ==
    \A t \in Tasks :
        Cardinality({h \in inflight : h.t = t /\ Owns(h)}) <= 1

\* The `running` state and the lease are inseparable, which the table's own
\* CHECK constraint also enforces.
RunningIffLeased ==
    \A t \in Tasks : (state[t] = "running") <=> (lease[t] # 0)

\* A terminal task never holds a lease.
TerminalUnleased ==
    \A t \in Tasks : state[t] \in Terminal => lease[t] = 0

\* claim never runs a task beyond its budget.
AttemptBounded ==
    \A t \in Tasks : attempt[t] <= MaxAttempts

\* Lease tokens are never reused, so a stale token can never be mistaken for
\* a live one.
TokensUnique ==
    \A t \in Tasks : lease[t] < nextToken

Safety ==
    /\ TypeOK /\ AtMostOneFencedWriter /\ RunningIffLeased
    /\ TerminalUnleased /\ AttemptBounded /\ TokensUnique

(***************************************************************************)
(* Temporal                                                                 *)
(***************************************************************************)

\* Every task mutation must be performed by the handler that owns the full
\* attempt and lease-token fence.
FencedMutations ==
    [][\A h \in inflight :
        (Complete(h) \/ FailWithRetry(h) \/ FailTerminally(h)) => Owns(h)]_vars

\* Terminal states are absorbing: nothing ever moves a finished task.
TerminalIsStable ==
    [][\A t \in Tasks : state[t] \in Terminal => state'[t] = state[t]]_vars

\* Attempts only ever go up, so a replayed claim cannot rewind the fence.
AttemptMonotonic ==
    [][\A t \in Tasks : attempt'[t] >= attempt[t]]_vars

\* No task is lost: every task ends up in a terminal state.
EventuallyTerminal ==
    <>[](\A t \in Tasks : state[t] \in Terminal)

(***************************************************************************)
(* Vacuity guards                                                           *)
(*                                                                          *)
(* AtMostOneFencedWriter holds trivially in a model where nothing is ever    *)
(* claimed twice, so a clean TLC run only means something if the dangerous   *)
(* states are actually reached. Each predicate below is the NEGATION of a    *)
(* state we need reachable, so TLC reporting it "violated" is the witness.   *)
(***************************************************************************)

\* Tasks do get claimed.
CoverRunning == ~(\E t \in Tasks : state[t] = "running")

\* A task returns to pending and is retried.
CoverRetried == ~(\E t \in Tasks : state[t] = "pending" /\ attempt[t] > 0)

\* The case the whole fencing design exists for: a handler still believes it
\* owns a task that has since moved on. If this is unreachable then
\* AtMostOneFencedWriter is not testing anything.
CoverStaleHandler == ~(\E h \in inflight : ~Owns(h))

\* A task can exhaust its attempt budget.
CoverExhausted == ~(\E t \in Tasks : attempt[t] = MaxAttempts)

\* Two handlers can be in flight for the same task at once, which is what
\* at-least-once delivery means.
CoverConcurrentHandlers ==
    ~(\E h1, h2 \in inflight : h1 # h2 /\ h1.t = h2.t)

=============================================================================
