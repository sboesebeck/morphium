# Plan: MOR-063 migration lock of the Quarkus extension (short)

`MorphiumMigrationRunner.acquireLock` takes the lock with one upsert (`_id = migration_lock AND expires_at <= now`, `Query.set(update, true, false)`). A held lock makes the insert half collide on `_id`. Before MOR-059 that came back as a `writeErrors` entry in the reply (`n == 0`, no upsert: held); since MOR-059 the write raises, and the runner recognised it with `e.getMessage().contains("duplicate key")`.

Decision (MOR-059 review, challenger finding 1):

- The decision of the lock write moves into the package-private `lockWasAcquired(Supplier<Map>)`: a reply with a match or an upsert is acquired, none (or no reply, with the raise switch off) is held; a `MorphiumDriverException` with `isDuplicateKey()` (code 11000 or in the write errors) is held; a `WriteConcernNotSatisfiedException` is acquired (the lock IS written; aborting would leave it held until it expires) and logged as a warning; everything else, also a `RuntimeException` whose text says "duplicate key", is rethrown.
- `renewLock` stays as it is: its `catch (Exception)` already treats a failing renewal as best effort and the matched count of the reply still detects a lost lock.

Tests (first): `MorphiumMigrationRunnerLockTest` (pure, the classification, including a duplicate key whose message does not say so and a RuntimeException that does), `MorphiumMigrationRunnerLockContentionTest` (two runners and six concurrent contenders on the InMemoryDriver and, with `-Dmorphium.uri=mongodb://host:port/db`, on a real MongoDB: one winner, the others see "Migration lock is held by", takeover after the TTL).
