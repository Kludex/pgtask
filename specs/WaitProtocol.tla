---------------------------- MODULE WaitProtocol ----------------------------
(***************************************************************************)
(* pgtask parks a task by registering a wait row and then setting the task  *)
(* to `waiting`. Something else is expected to wake it. Both wait paths     *)
(* have the same shape:                                                     *)
(*                                                                          *)
(*   waiter (pgtask.wait_for_signal / pgtask.wait_for_result)               *)
(*     1. read the wake source                                              *)
(*     2. if it is already there, return `ready` and keep running           *)
(*     3. otherwise INSERT the wait row, set the task to `waiting`, COMMIT   *)
(*                                                                          *)
(*   waker (pgtask.emit_signal / pgtask.complete_task on the child)         *)
(*     1. write the wake source                                             *)
(*     2. scan for unresolved wait rows and resolve them                    *)
(*     3. COMMIT                                                            *)
(*                                                                          *)
(* Under READ COMMITTED the waiter's INSERT is invisible to the waker until *)
(* the waiter commits. So unless something forces the two transactions to    *)
(* serialise, there is an interleaving in which the waiter reads "no source" *)
(* and the waker reads "no wait" -- and the task parks with its wake-up      *)
(* already spent.                                                           *)
(*                                                                          *)
(* Modelling each SQL function as one atomic step would hide this entirely.  *)
(* The read and the commit are therefore separate actions here, and only     *)
(* committed writes are visible to the other transaction.                    *)
(*                                                                          *)
(* SourceLocked - does the waiter hold a row lock that the waker's write     *)
(*   conflicts with, for the whole span from its read to its commit?         *)
(*                                                                          *)
(*   TRUE  models pgtask.wait_for_signal. It holds FOR UPDATE on the task    *)
(*         row, and emit_signal's INSERT into pgtask.signals needs FOR KEY   *)
(*         SHARE on that same row to validate its foreign key. FOR UPDATE    *)
(*         and FOR KEY SHARE conflict, so the waker blocks.                  *)
(*                                                                          *)
(*   FALSE models pgtask.wait_for_result. It holds FOR UPDATE on the PARENT  *)
(*         row, but the waker is complete_task on the CHILD row, which       *)
(*         takes no lock on the parent. Nothing serialises them.             *)
(*                                                                          *)
(* Recheck - after inserting the wait row, does the waiter read the source   *)
(*   a second time in the same transaction? This is the obvious cheap fix,   *)
(*   and the model shows it does not close the window.                       *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS SourceLocked, Recheck

VARIABLES
    waiterPC,         \* where the waiter's transaction is
    wakerPC,          \* where the waker's transaction is
    lockOwner,        \* who holds the conflicting row lock
    sourceCommitted,  \* the wake source is committed, so it is visible
    waitCommitted,    \* the wait row is committed, so it is visible
    parked,           \* the task committed in state `waiting`
    resolved,         \* the wait was resolved and the task rescheduled
    wakerSawWait      \* what the waker's scan found in its snapshot

vars == <<waiterPC, wakerPC, lockOwner, sourceCommitted, waitCommitted,
          parked, resolved, wakerSawWait>>

WaiterStates == {"read", "register", "recheck", "commit", "resumed", "parked"}
WakerStates  == {"write", "scan", "commit", "done"}

TypeOK ==
    /\ waiterPC \in WaiterStates
    /\ wakerPC \in WakerStates
    /\ lockOwner \in {"none", "waiter", "waker"}
    /\ sourceCommitted \in BOOLEAN
    /\ waitCommitted \in BOOLEAN
    /\ parked \in BOOLEAN
    /\ resolved \in BOOLEAN
    /\ wakerSawWait \in BOOLEAN

Init ==
    /\ waiterPC = "read"
    /\ wakerPC = "write"
    /\ lockOwner = "none"
    /\ sourceCommitted = FALSE
    /\ waitCommitted = FALSE
    /\ parked = FALSE
    /\ resolved = FALSE
    /\ wakerSawWait = FALSE

(***************************************************************************)
(* Waiter                                                                   *)
(***************************************************************************)

\* Takes the row lock, then reads the wake source in its snapshot. A source
\* that is already committed means there is nothing to wait for.
WaiterRead ==
    /\ waiterPC = "read"
    /\ SourceLocked => lockOwner = "none"    \* blocks while the waker holds it
    /\ IF sourceCommitted
         THEN /\ waiterPC' = "resumed"       \* returns `ready`, transaction ends
              /\ lockOwner' = "none"
         ELSE /\ waiterPC' = "register"
              /\ lockOwner' = IF SourceLocked THEN "waiter" ELSE "none"
    /\ UNCHANGED <<wakerPC, sourceCommitted, waitCommitted, parked, resolved,
                   wakerSawWait>>

\* INSERTs the wait row. It stays invisible to the waker until commit.
WaiterRegister ==
    /\ waiterPC = "register"
    /\ waiterPC' = IF Recheck THEN "recheck" ELSE "commit"
    /\ UNCHANGED <<wakerPC, lockOwner, sourceCommitted, waitCommitted, parked,
                   resolved, wakerSawWait>>

\* The proposed cheap fix: read the source again on a fresh statement
\* snapshot before parking.
WaiterRecheck ==
    /\ waiterPC = "recheck"
    /\ IF sourceCommitted
         THEN /\ waiterPC' = "resumed"
              /\ lockOwner' = "none"
         ELSE /\ waiterPC' = "commit"
              /\ UNCHANGED lockOwner
    /\ UNCHANGED <<wakerPC, sourceCommitted, waitCommitted, parked, resolved,
                   wakerSawWait>>

\* Commits: the wait row becomes visible and the task is parked.
WaiterCommit ==
    /\ waiterPC = "commit"
    /\ waitCommitted' = TRUE
    /\ parked' = TRUE
    /\ lockOwner' = "none"
    /\ waiterPC' = "parked"
    /\ UNCHANGED <<wakerPC, sourceCommitted, resolved, wakerSawWait>>

(***************************************************************************)
(* Waker                                                                    *)
(***************************************************************************)

\* Writes the wake source, taking the conflicting lock if there is one.
WakerWrite ==
    /\ wakerPC = "write"
    /\ SourceLocked => lockOwner = "none"    \* blocks while the waiter holds it
    /\ lockOwner' = IF SourceLocked THEN "waker" ELSE "none"
    /\ wakerPC' = "scan"
    /\ UNCHANGED <<waiterPC, sourceCommitted, waitCommitted, parked, resolved,
                   wakerSawWait>>

\* Scans for wait rows. Only committed ones are in its snapshot.
WakerScan ==
    /\ wakerPC = "scan"
    /\ wakerSawWait' = waitCommitted
    /\ wakerPC' = "commit"
    /\ UNCHANGED <<waiterPC, lockOwner, sourceCommitted, waitCommitted, parked,
                   resolved>>

\* Commits the source, and resolves the wait only if the scan found one.
WakerCommit ==
    /\ wakerPC = "commit"
    /\ sourceCommitted' = TRUE
    /\ resolved' = (resolved \/ wakerSawWait)
    /\ lockOwner' = "none"
    /\ wakerPC' = "done"
    /\ UNCHANGED <<waiterPC, waitCommitted, parked, wakerSawWait>>

Finished == waiterPC \in {"resumed", "parked"} /\ wakerPC = "done"

Next ==
    \/ WaiterRead \/ WaiterRegister \/ WaiterRecheck \/ WaiterCommit
    \/ WakerWrite \/ WakerScan \/ WakerCommit
    \/ (Finished /\ UNCHANGED vars)          \* allow the run to end

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

(***************************************************************************)
(* The property                                                             *)
(*                                                                          *)
(* Once both transactions have committed, a task that parked must have been *)
(* woken if its source arrived. A parked task whose source is committed and  *)
(* whose wait was never resolved is stuck forever: pgtask has no sweep that  *)
(* recovers it. recover_result_wait_timeouts only visits waits that carry a  *)
(* timeout, and recover_expired only visits tasks in state `running`.        *)
(***************************************************************************)
NoLostWakeup ==
    ~( /\ Finished
       /\ parked
       /\ sourceCommitted
       /\ ~resolved )

\* Anything that parked must eventually be rescheduled.
EventuallyRunnable == <>[](Finished => (~parked \/ resolved))

(***************************************************************************)
(* Vacuity guards                                                           *)
(*                                                                          *)
(* NoLostWakeup would also hold in a model where the task never parks at    *)
(* all, which would make a clean TLC run worthless. Each of these is the     *)
(* NEGATION of a state we need to be reachable, checked as an invariant, so  *)
(* TLC reporting it "violated" is the proof that the state is reachable.     *)
(* A run where any of these passes means the model has gone inert.           *)
(***************************************************************************)

\* The waiter can actually park.
CoverParks == ~parked

\* The waiter can find the source already there and keep running.
CoverResumes == ~(waiterPC = "resumed")

\* A parked task can actually be woken.
CoverResolved == ~resolved

(***************************************************************************)
(* The mechanism, stated directly.                                          *)
(*                                                                          *)
(* The waiter is exposed from the moment it has decided to park until it     *)
(* commits; the waker is exposed from its scan until its commit. A lost      *)
(* wake-up is possible exactly when those two windows can overlap.           *)
(*                                                                          *)
(* Under SignalWait this holds, and that IS the safety argument: the lock    *)
(* makes the overlap unreachable. Under ResultWait it is violated, and the   *)
(* violation is the bug. So the same predicate is checked as an invariant in *)
(* one configuration and as a reachability witness in the other.             *)
(***************************************************************************)
NoConcurrentRegistration ==
    ~( /\ waiterPC \in {"register", "recheck", "commit"}
       /\ wakerPC \in {"scan", "commit"} )

=============================================================================
