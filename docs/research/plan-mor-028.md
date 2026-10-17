# MOR-028: update contract suite (InMemoryDriver vs real MongoDB)

## Design

- `morphium-core/src/test/java/de/caluga/test/mongo/suite/contract/`: a table of raw update statements (`UpdateContract.Case`) with the result of a real MongoDB (documents afterwards, ordered by `_id`, or an error code). The expectations were written from the MongoDB semantics and verified against `mongo:8` (v8.3.x) first; all expectations matched `mongo:8` on the first run, so the table is the contract, not a recording of the InMemoryDriver.
- Every case runs as an `update` command and, if it is not a multi update, as `findAndModify` (the path of the lease and counter use cases), on every driver of `MultiDriverTestBase.getMorphiumInstances()`. Statements are deep copied per run, so a driver cannot influence the next run.
- Numbers keep their type in the comparison (`1`, `1L`, `1.0` differ), documents ignore the key order, lists keep their order.
- Families: `FieldOperatorContractTest` ($set/$unset, $inc/$mul, $min/$max/$rename), `ArrayOperatorContractTest` ($push/$addToSet, $pull/$pop, positional and `arrayFilters`), `UpsertAndErrorContractTest` (upsert seeding, `$setOnInsert`, replacement, statements a real MongoDB rejects), `StatementIsolationContractTest` (a driver neither modifies nor keeps the statement).
- Deviations are explicit: `Case.inMem("MOR-nnn", pinnedResult)` (or `inMemFindAndModify` for one path) states what the InMemoryDriver does today and names the ledger task that removes it. `UpdateContractLedgerTest` fails if a pinned deviation names a task that is not in the ledger or already `done`, so a fix has to remove its pins (and the case then runs against the common expectation).
- Without an external MongoDB the suite runs on the InMemoryDriver only (default build, green); with `-Pexternal -Dmorphium.driver=pooled,inmem -Dmorphium.uri=...` on both.

## Found deviations (InMemoryDriver), by ledger task

MOR-051, MOR-053, MOR-054 and MOR-058 are fixed (their pins are gone from the suite); the rows stay as the record of what the suite found.

| Task | Deviation |
|---|---|
| MOR-051 | no path conflict check (set a + set a.b, set + inc, set + unset, parent set + child unset, push + pull) |
| MOR-053 | no type checks of `$inc`, `$mul`, `$pull`, `$pop`, `$push`, `$addToSet`; the statement is applied partly |
| MOR-054 | unknown operator, mixed fields, `_id` change, empty field name, `$rename` onto itself or with a non-string target, unused `arrayFilters` identifier |
| MOR-055 | `$inc` overflow wraps (int), `$mul` int by double, `$min`/`$max` across types, `$addToSet` equality (1 vs 1.0, field order) |
| MOR-056 | upsert does not seed the equality of a single-branch `$or` |
| MOR-057 | an inserted map is stored by reference |
| MOR-058 | `findAndModify` accepts a `$`-prefixed field name and `$set` below a scalar or array that the update command rejects |

Not covered by the table: `$currentDate`, `$bit`, pipeline updates, collation, `hint`, transactions, `Query.set`/`push` and the other Morphium write paths (they are covered by their own tests; this suite is the raw statement layer under them).
