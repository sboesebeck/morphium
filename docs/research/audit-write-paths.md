# Audit of the Morphium write paths (MOR-005)

For each public write path of `Query` and `Morphium` (and the bulk context): does it **raise** when the server rejects the statement, and does it **translate** the
Java field name to the stored name. Measured on a real MongoDB (`mongo:8`, v8.3.x, `PooledDriver`) by `WritePathAuditContractTest`
(`morphium-core/src/test/java/de/caluga/test/mongo/suite/contract/`); the translation column is also measured on the InMemoryDriver (same result).

## Method

- **Raises**: the arguments of each path are chosen so that the server rejects the statement: a unique index on `code`, `counter_value` and `tag_list` (document `a` is
  updated into a collision with document `b`), a path below a scalar (`scalar_value.sub`), `pull`/`pullAll` on a field that is no array. A path *raises* if the call
  throws. In every case the document is unchanged afterwards (the server did reject the statement); a path that does not raise therefore reports success for a write that
  did not happen. `n/m`: not measured on its own (same implementation as a measured path, or the Java-name defect hides the collision).
- **Translates**: the path is called with the Java name (`counterValue`, `tagList`, `lastSeen`, a dotted path through an `@Embedded` document `box.innerValue`) on
  an entity with camel case translation; the stored document is read raw. A path *translates* if the stored name has the effect and no Java name appears.

## Table

| Path | Raises on a rejected write | Translates the field name |
|---|---|---|
| `Query.set(field)` | **no** | yes |
| `Query.set(map)` | **no** | yes |
| `Query.setEnum(map)` | **no** | yes |
| `Query.set(enum)` | **no** | yes |
| `Query.set(dotted path)` | **no** | yes |
| `Query.unset(field)` | **no** | yes |
| `Query.unset(enum)` | **no** | yes |
| `Query.inc(field,Integer)` | **no** | yes |
| `Query.inc(field,Long)` | **no** | yes |
| `Query.inc(field,Double)` | **no** | yes |
| `Query.inc(field,Number)` | **no** | yes |
| `Query.inc(enum)` | **no** | yes |
| `Query.inc(dotted path)` | **no** | yes |
| `Query.dec(field,Integer)` | **no** | yes |
| `Query.dec(field,Number)` | **no** | yes |
| `Morphium.inc(query,map)` | **no** | yes |
| `Morphium.inc(enumMap,query)` | n/m | yes |
| `Morphium.dec(query,map)` | **no** | yes |
| `Query.push(field)` | **no** | yes |
| `Query.push(enum)` | **no** | yes |
| `Query.pushAll(field)` | **no** | yes |
| `Query.pull(field)` | **no** | yes |
| `Query.pullAll(field)` | **no** | yes |
| `Morphium.addToSet` | **no** | yes |
| `Morphium.currentDate(field)` | **no** | yes |
| `Morphium.currentDate(enum)` | n/m | yes |
| `Morphium.currentDate(dotted path)` | n/m | yes |
| `Morphium.setInEntity(field)` | **no** | yes |
| `Morphium.inc(entity)` | yes | yes |
| `Morphium.dec(entity)` | yes | yes |
| `Morphium.unsetInEntity` | **no** | yes |
| `Morphium.push(entity)` | **no** | yes |
| `Bulk.addSetRequest` | **no** | yes |
| `Bulk.addIncRequest` | **no** | yes |
| `Bulk.addPushRequest` | **no** | yes |
| `Bulk.addUnsetRequest` | **no** | yes |
| `Bulk.addCurrentDateRequest` | **no** | yes |

## Findings

The translation column shows the state after the fixes of MOR-060 to MOR-062; the raise column is the state of the audit (MOR-059 is open).

1. **Almost no write path raises** (ledger task MOR-059, priority 1). The update command reports a rejected statement in its reply (`writeErrors`) and does not throw; only
   `Morphium.inc(entity, ...)` and `dec(entity, ...)` look at the reply. All other paths (`Query.set/unset/inc/dec/push/pushAll/pull/pullAll`, `addToSet`, `currentDate`,
   the entity variants of set/unset/push, the Map variants of inc/dec, the bulk context) return normally. Example: `query.set("counterValue", 2)` that violates a unique index returns
   as if the write had happened. `Query.updateMapped` (the count terminals of the update builder) is the model for the fix. GitHub #408 (a `writeConcernError` is ignored) is the same
   family; the `writeConcernError` part is not measured here.
2. **Java names were written untranslated** (MOR-060, fixed): `Morphium.inc(Query, Map)`, `inc(Map<Enum,Number>, Query)` and `dec(Query, Map)` copy the keys into `$inc` (confirms the finding of MOR-042
   about `MorphiumWriterImpl.inc(Query, Map)`); `Query.set`, `Query.inc` and `Morphium.currentDate` translate only the first segment of a dotted path, so `box.innerValue` is written next to the
   stored `box.inner_value`. The update builder translates the same paths correctly (`resolveFieldPath`).
3. **`dec` with a Map incremented** (MOR-062, fixed): `dec(Query, Map)` and `dec(Map<Enum,Number>, Query)` pass the amounts to `inc` without negating them.
4. **`Query.dec(field, Integer)` stored a double** (MOR-061, fixed): the call resolves to the `Number` overload of `Morphium.dec`, which subtracts `amount.doubleValue()`.
5. The bulk context takes stored names (it builds raw update documents) and does translate the names given here (`counterValue` is stored as `counter_value`).

The InMemoryDriver raises in some of the cases where a real MongoDB does not (it throws for a unique index violation of `$set` itself) and does not enforce unique multikey indexes on `$push`;
that is a difference of the driver, not of the write path, and why the raise column is measured on a real MongoDB only.
