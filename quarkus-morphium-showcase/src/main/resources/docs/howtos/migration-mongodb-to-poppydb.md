# Migrating from MongoDB to PoppyDB

A guide for moving a workload that's currently on real MongoDB onto PoppyDB — either fully, or as
a lightweight sibling deployment for a specific role (message broker, cache, session store). This
is a one-way migration guide (Mongo → PoppyDB); see the
[Production Deployment Playbook](./poppydb-deployment.md) for running PoppyDB once you're there,
and [PoppyDB § Comparison: PoppyDB vs InMemory Driver](../poppydb.md#comparison-poppydb-vs-inmemory-driver)
if you're choosing between PoppyDB and Morphium's embedded driver instead of real MongoDB.

## 1. Decide if this migration makes sense

PoppyDB is not a MongoDB replacement for every workload — it's a wire-protocol-compatible,
in-memory server with a specific, honest set of trade-offs. Read
[PoppyDB § When NOT to Use](../poppydb.md#when-not-to-use) and
[§ Use Cases](../poppydb.md#use-cases) before migrating anything. As a rule of thumb:

**Good fit:**
- Ephemeral messaging (Morphium's built-in queue), cache, or session storage — data that can
  tolerate loss between snapshots (see the
  [loss model](../poppydb.md#5-message-broker-for-short-lived-messages-production)).
- A dataset that comfortably fits in RAM with headroom (see
  [Capacity Planning](./poppydb-deployment.md#4-capacity-planning)).
- A workload that doesn't depend on GridFS, sharding, advanced full-text search, or
  MongoDB-Atlas-specific features (see [§ Limitations](../poppydb.md#limitations)).

**Not a fit (keep on real MongoDB, or migrate only part of the workload):**
- Durable system-of-record data that must survive a total cluster outage without loss.
- Datasets larger than you can comfortably fit in a JVM heap.
- Anything relying on sharding, GridFS, or advanced geospatial/full-text search.

It's common to migrate *part* of a workload — e.g. move messaging and session storage to PoppyDB
while leaving the durable business data on real MongoDB. Nothing requires an all-or-nothing cut.

## 2. Check for feature dependencies first

Before moving data, grep your application (and any ops tooling) for use of features PoppyDB
doesn't support: GridFS, `$text` search beyond basic matching, sharding-related commands,
distributed transactions, Atlas-specific APIs. See
[PoppyDB § Limitations](../poppydb.md#limitations) for the current list — it changes over time, so
check the current docs rather than assuming this guide is exhaustive.

Also check your MongoDB version's specific behaviors your application may implicitly depend on
(exact error codes/messages, specific aggregation edge cases) — PoppyDB's InMemoryDriver aims for
close compatibility (see the correctness fixes tracked in `CHANGELOG.md`) but is not a byte-for-byte
clone; test against a staging PoppyDB instance with your actual application before cutting over.

## 3. Move the data

Two approaches, depending on how much control you need over the migration:

### Option A: Standard MongoDB tools (`mongodump`/`mongorestore`)

Since PoppyDB speaks the real MongoDB wire protocol and implements `listCollections`,
`listIndexes`, `find`, `insert`, and `createIndexes` (see
[PoppyDB § Supported Admin Commands](../poppydb.md#supported-admin-commands)), the standard
MongoDB Database Tools should work directly against it:

```bash
# Dump from the source MongoDB
mongodump --uri "mongodb://source-host:27017/mydb" --out ./dump

# Restore into PoppyDB (same wire protocol, no MongoDB installation needed on this side)
mongorestore --uri "mongodb://poppydb-host:27017/mydb" ./dump
```

**Verify this against your specific dataset in staging before relying on it** — this path has not
been exhaustively tested against every `mongodump`/`mongorestore` edge case (e.g. `--oplog` mode,
which depends on real oplog semantics PoppyDB doesn't replicate the same way; views; specific BSON
type edge cases). It is the least-effort option and a reasonable first thing to try, not a
guaranteed drop-in.

### Option B: Morphium-level migration (more control, guaranteed compatible)

Since both ends go through the same Morphium object mapper, this approach sidesteps any wire-level
tool compatibility question entirely — useful if you need to filter, transform, or validate data
during the move, or if Option A doesn't cover your case:

```java
MorphiumConfig sourceCfg = new MorphiumConfig();
sourceCfg.connectionSettings().setDatabase("mydb").addHost("source-mongo-host", 27017);
Morphium source = new Morphium(sourceCfg);

MorphiumConfig targetCfg = new MorphiumConfig();
targetCfg.connectionSettings().setDatabase("mydb").addHost("poppydb-host", 27017);
Morphium target = new Morphium(targetCfg);

for (String collectionName : source.listCollections()) {
    // Adjust the entity class per collection, or iterate generically via the driver
    // if your entities aren't statically known here.
    List<MyEntity> batch = source.createQueryFor(MyEntity.class, collectionName).asList();
    target.storeList(batch);
}

// Recreate indexes explicitly if you didn't already ensure them via annotations on `target`:
target.ensureIndicesFor(MyEntity.class);
```

For large collections, page through with `Query.setLimit()`/`skip()` or a sorted cursor instead of
loading everything into memory at once, and consider running per-collection in parallel.

## 4. Validate the migration

- **Document counts**: compare `db.stats()`/`collStats` per collection on both sides.
- **Content hash**: PoppyDB implements `dbHash` (per-collection MD5 in canonical document order,
  see [PoppyDB § Supported Admin Commands](../poppydb.md#supported-admin-commands)) specifically
  for this kind of comparison — but note it only compares *PoppyDB-to-PoppyDB* hashes meaningfully
  if both sides compute it the same way; for a Mongo-vs-PoppyDB comparison, compare document counts
  and spot-check a sample of documents by `_id` instead, or dump both sides and diff.
- **Indexes**: confirm `listIndexes` on the target matches the source (unique constraints and TTL
  indexes are the ones that silently misbehave if missed — nothing enforces them until they're
  actually created).
- **Application smoke test**: run your test suite (or a subset of real read/write traffic) against
  the migrated PoppyDB instance before cutting production traffic over.

## 5. Cutover

1. Put the source MongoDB into a brief read-only/quiesced state (application-level freeze, or a
   maintenance window) so no writes land after your last data copy.
2. Re-run the migration (Option A or B) for anything written since your last full copy, or do a
   final incremental sync if your migration tooling supports it.
3. Switch application configuration to point at PoppyDB.
4. Keep the source MongoDB available (read-only) for a rollback window — see below.

## 6. Rollback plan

Keep the original MongoDB instance running and untouched (do not decommission it immediately).
If an issue surfaces after cutover:

1. Switch application configuration back to MongoDB.
2. Any writes that landed on PoppyDB during the cutover window need to be replayed back to
   MongoDB manually (Option B's approach works in reverse for this) — plan for this *before*
   cutover if your workload can't tolerate losing that window's writes, or keep the cutover window
   short enough that manual replay is practical.

Only decommission the source MongoDB once you've run on PoppyDB successfully for a deliberate
observation period (long enough to hit your normal peak load and any periodic jobs).

## See Also

- [PoppyDB Production Deployment Playbook](./poppydb-deployment.md)
- [PoppyDB](../poppydb.md) — full feature reference, Limitations, Use Cases
- [Messaging Implementations](./messaging-implementations.md) — if the migration is for
  Morphium messaging specifically
