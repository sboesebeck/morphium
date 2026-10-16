# Plan: MOR-011 typed lease primitive (short)

## Problem

Applications write the same lease by hand: a conditional upsert with the filter `_id = k AND (expiresAt < now OR owner = me)`, set owner and expiry, and "held" is a duplicate key. With MOR-006 (builder) and MOR-010 (`UpdateOutcome`) the pieces exist, but the filter, the field wiring and the classification are still copied around.

## Design (small, incubating until 6.3)

`LeaseManager<T>` in `de.caluga.morphium.query`, created with `LeaseManager.of(morphium, Class<T>, ownerField, expiresAtField)` (Java field names). It does nothing the builder cannot do; it is the recipe, typed:

- `tryAcquire(Object leaseId, String owner, Duration ttl)` and the same with an explicit `Instant now`: acquires a free or expired lease and renews a lease the owner holds. One atomic conditional upsert through `Query.modify()...tryFindOneAndUpdate(AFTER)`; `INSERTED` and `UPDATED` map to `ACQUIRED`, `DUPLICATE_KEY` to `HELD`. Result `LeaseResult<T>` (final class): `getStatus()` (`ACQUIRED`/`HELD`), `isAcquired()`, `getLease()` (the document after the write, `null` if held).
- `release(Object leaseId, String owner)`: sets the expiry of the lease of this owner to the epoch (the document stays, so a version and the history survive), returns whether the owner held it. A lease of somebody else or an unknown id is not touched and returns `false`.
- The owner and expiry fields are checked at creation: they must exist; the expiry must be an `Instant` or a `java.util.Date` (the value is passed in the type of the field); the owner is a `String`. The lease id is the `@Id` of the entity (a natural key that is not the `@Id` needs a unique index and a different filter: out of scope, documented).
- The clock is the caller's (`now` parameter, default `Instant.now()`): leases between hosts are only as exact as their clocks; the Javadoc says so. A `ttl` must be positive.
- No events, no thread, no heartbeat scheduler: renewing is calling `tryAcquire` again.

## Tests (first)

`LeaseManagerValidationTest` (both drivers: invalid fields/types/ttl/null owner), `LeaseManagerTest` (both drivers): acquire inserts, a second owner is HELD and the holder is untouched, the holder renews (deadline moves, still ACQUIRED), takeover after expiry, release by the owner frees it for another, release by a stranger does nothing, release of an unknown id, a `Date` expiry field, an `@Version` entity, a `MorphiumId` is not needed. Concurrency: eight contenders on a fresh lease have exactly one winner, repeated for several ids; eight contenders on an expired lease have exactly one winner; renewing owner and contenders interleaved never produce two holders.

Not in this task: natural-key leases, a heartbeat, fencing tokens (the `@Version` of the lease document is the natural fencing token and is documented as such).
