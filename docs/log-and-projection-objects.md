# One Object for the log, one per projection version

Why a stream's log and its read models live in separate Durable Objects,
with an optimistic append between them, instead of one Object with one
transaction. Decided 2026-11-11; the one-Object variant is the `main`
branch.

## The shape

- `Log`, named after the stream: the `events` table, `append` guarded by
  the expected head, `events` after a number. Never changes.
- `Projection`, named `<stream>/<version>`: a read model, its cursor,
  and the domain's `decide`. A command catches up, decides, appends with
  the cursor as the expected head, and folds the appended events into
  the read model with the new cursor. A refusal from the log means the
  head moved in between: catch up, decide again.

The guarantee is the one a single transaction gave: a decision lands
only if the state it was made on is still the head of the log.

## Why

- **Projection versions are structural.** A changed projection is a new
  Object that catches up from the log while the old one serves; cutover
  is addressing it, rollback is addressing the old one. No pause, no
  rebuild in place, no CPU ceiling to fit a rebuild into.
- **The log is never at risk.** No projection code, however wrong, can
  touch it, and the log Object's code does not change with the domain.
- **Several read models per stream**, each its own Object, each a
  consumer of the same log; and several writers, since any Object may
  append with the same rule.

## What it costs

- One more hop per command, and the two Objects may sit in different
  data centers; location hints are best effort.
- The crash window moves: an event can be in the log before the
  projection has applied it. The cursor heals it: a projection catches
  up before it decides and before it answers a query.
- A projection that throws no longer rolls an event back. The event is a
  fact; the fix is a corrected projection under a new version.
- Two Objects alive per stream, so duration and requests roughly double.

## What it opens

- **Regional read models.** A projection named `<stream>/<version>/<region>`,
  created with a location hint, answers reads near a global team; the
  event number is the bookmark that keeps read-your-writes honest.
- **Regional writers.** The same projection may decide and append, one
  hop to the log instead of two, the log arbitrating between regions as
  it does between versions.
