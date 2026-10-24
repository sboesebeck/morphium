# Conditional single-document writes

A lease, a job claim, a "run this once" marker, an optimistic lock and a rate limit look different, but they are one database operation: an
**atomic compare-and-set on one document**.

| Part | What it is | In Morphium |
|---|---|---|
| Key | names the document; unique | the `@Id`, or a field with a unique index |
| Guard | a predicate on the current state; the write happens only if it holds | the filter of the query (`q.f(...)`, `q.or(...)`) |
| Effect | the change | `query.modify().set(...).inc(...).setOnInsert(...)` |
| Outcome | what happened | `tryFindOneAndUpdate(ReturnDocument.AFTER)` returns an `UpdateOutcome`: `INSERTED`, `UPDATED`, `NOT_MATCHED`, `DUPLICATE_KEY` |

The database gives two guarantees and nothing else is needed: an operation on one document is atomic, and a unique index decides the race of two inserts.
With `upsert()` a guard that does not hold makes the write try an insert; that insert collides with the existing document on the key and comes back as
`DUPLICATE_KEY`, which is the typed way to say "somebody else got there first". Without `upsert()` it is `NOT_MATCHED`.

## Recipes

| Use | Guard | Effect | Outcome you look at |
|---|---|---|---|
| Lease, lock, leader election | the deadline has passed, or I am the owner | owner, deadline, fencing counter up | `DUPLICATE_KEY` = held by somebody else |
| Job claim (visibility timeout) | state is `IDLE`, or the claim has expired | state `RUNNING`, attempts up, deadline | `NOT_MATCHED` = nothing to claim |
| Run once (idempotency key) | none; the key is the filter (`upsert()`, `setOnInsert`) | create | `INSERTED` = first, `UPDATED` = already done (nothing was changed) |
| State transition (`OPEN` to `CLOSED`) | `state == OPEN` | `state = CLOSED` | `UPDATED` = done by this call, `NOT_MATCHED` = it was not open (a second call is a no-op) |
| Optimistic lock | `@Version == n` | version up | `NOT_MATCHED` = somebody changed it |
| Counter with a limit | counter `< limit` | counter up | `NOT_MATCHED` = limit reached |

```java
// run once: the first caller of a key wins
UpdateOutcome<Done> o = morphium.createQueryFor(Done.class).f("key").eq(requestId)
        .modify().setOnInsert("startedAt", Instant.now()).upsert().tryFindOneAndUpdate(ReturnDocument.AFTER);
if (o.wasInserted()) { process(); }

// job claim: take one idle job, or one whose claim has expired
UpdateOutcome<Job> c = morphium.createQueryFor(Job.class).f("state").eq(IDLE)   // add q.or(...) for the expired claims
        .modify().set("state", RUNNING).set("claimedUntil", until).inc("attempts", 1)
        .tryFindOneAndUpdate(ReturnDocument.AFTER);
if (c.getKind() == UpdateOutcome.Kind.UPDATED) { work(c.getDocument()); }
```

### Create or join

A case that exists once per unit (`state == OPEN`, a **partial unique index** on `unit`) is created by the first caller and joined by the others:

```java
UpdateOutcome<Case> o = morphium.createQueryFor(Case.class).f("unit").eq(unit).f("state").eq("OPEN")
        .modify().setOnInsert("id", UUID.randomUUID().toString()).setOnInsert("state", "OPEN").addToSet("waiting", requestId)
        .upsert().tryFindOneAndUpdate(ReturnDocument.AFTER);
```

If an OPEN case exists, the filter matches and the caller joins it (`UPDATED`). If none exists, the upsert inserts (`INSERTED`). If two callers see none at the same moment, the second insert hits the unique
index and comes back as `DUPLICATE_KEY`: **repeat once**, the case of the winner exists now and the filter matches it. Treating `DUPLICATE_KEY` as a failure (an empty result, an exception) loses a request
although the guarantee "never two OPEN cases" held. A partial index cannot be declared with `@Index` (its options string has no nested filter): create it with `Morphium.ensureIndex(type, collection, key,
Map.of("unique", true, "partialFilterExpression", Map.of("state", "OPEN")))`.

## Which class to use

- The builder (`modify()` and `UpdateOutcome`) is the primitive; everything above is written with it. Start there.
- [`LeaseManager`](../api-reference.md) is the packaged recipe for **time limited ownership**: key, owner, deadline, optional acquisition time and fencing token, renew and release.
- Fencing: a counter that every grant counts up. Work done under a lease carries the token, and the receiver refuses a token older than the one it has seen. Do not let a renewal change
  it, or the owner would refuse its own writes.
- A claim with states and attempts is not a lease with another name; it has its own guard. Write it with the builder (the job claim above); no packaged recipe exists because the one consumer that was
  looked at has none: its writes are conditional transitions and upserts.

## Limits

- One document. Two documents need a transaction, and a duplicate key inside a real MongoDB transaction aborts it, so do not run the `upsert()` forms inside `@MorphiumTransactional`.
- The time in the guard is the clock of the caller, not of the server. Use a ttl that is large compared to the clock skew between hosts.
- A guard that fails together with `upsert()` is what ends in `DUPLICATE_KEY`; a filter that is only the key never does. A unique index on the key is what makes the second inserter lose. Entity indexes are created by `Morphium.ensureIndicesFor`; an upsert does not create them.
