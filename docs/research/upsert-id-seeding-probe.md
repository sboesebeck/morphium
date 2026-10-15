# Upsert `_id` seeding probe (WP-P of the MOR-001/002/003 plan)

Question: for which `findAndModify` filter shapes does the server seed the `_id` of an upserted document from the filter, so that an
additional `$setOnInsert._id` with a different value is rejected (code 66, immutable field `_id`)? This decides when
`Query.findOneAndUpdate` must NOT add its generated `_id` (MOR-003).

Method: `UpsertSeedingProbeTest` (`morphium-core/src/test/java/de/caluga/test/mongo/suite/repro/`) sends each shape as a raw
`findAndModify` with `upsert=true` on an empty collection, once with `$setOnInsert._id = "gen"` (variant *pinned*) and once without any `_id`
in the update (variant *plain*). Run: real MongoDB `mongo:8` (standalone, `PooledDriver`) and `InMemoryDriver`, `X`/`Y` are strings.
Run it with `mvn -pl morphium-core test -Pexternal -Dmorphium.driver=pooled,inmem -Dmorphium.uri=<uri> -Dtest=UpsertSeedingProbeTest`
and `grep REPRO-RESULT`.

## Result on a real MongoDB (`PooledDriver`)

"Seeded" = plain variant stores `_id = X` (or the filter value) and the pinned variant fails with code 66.

| Shape | Server seeds `_id`? | plain variant `_id` | pinned variant |
|---|---|---|---|
| `{_id:X}`, `{_id:{$eq:X}}` | yes | `X` | 66 |
| `{_id:{$in:[X]}}` (one element) | **yes** | `X` | 66 |
| `{$and:[{_id:X}]}`, `{$and:[{_id:{$eq:X}}]}`, `{$and:[{_id:X},{owner:A}]}` | yes | `X` | 66 |
| `{$and:[{_id:{$in:[X]}}]}` (one element) | **yes** | `X` | 66 |
| `{$and:[{$and:[{_id:X}]}]}` | yes | `X` | 66 |
| `{$and:[{_id:X},{$or:[{a:1},{b:2}]}]}` | yes | `X` | 66 |
| `{$or:[{_id:X}]}` (single branch) | **yes** | `X` | 66 |
| `{$and:[{$or:[{_id:X}]}]}` | **yes** | `X` | 66 |
| `{"_id.a":1}` (dotted) | **yes** | `{a:1}` | 66 |
| `{_id:{a:1}}` (literal document) | yes | `{a:1}` | 66 |
| `{$and:[{_id:null}]}` | yes | `null` | 66 |
| `{_id:"x"}` / `{$and:[{_id:"x"}]}` with collation (strength 2) | yes | `x` | 66 |
| `{$and:[{_id:{$in:[X,Y]}}]}` | no | ObjectId | ok, `gen` |
| `{$or:[{_id:X},{_id:Y}]}` | no | ObjectId | ok, `gen` |
| `{_id:{$gt:"A"}}`, `{$and:[{_id:{$gt:"A"}}]}` | no | ObjectId | ok, `gen` |
| `{_id:/^X/}` | no | ObjectId | ok, `gen` |
| `{$nor:[{_id:X}]}` | no | ObjectId | ok, `gen` |
| `{$and:[{_id:X},{_id:Y}]}` and even `{$and:[{_id:X},{_id:X}]}` | error code 54 in both variants | - | - |

Rule that fits all rows: the server seeds `_id` from an **equality** on `_id` (plain value incl. `null` and a literal document, `$eq`, `$in`
with exactly one element, or a dotted `_id.x` key), reached through `$and` (any depth) and through single-branch `$or`. Multi-valued
`$in`, multi-branch `$or`, range operators, regex and `$nor` do not seed. Two equalities on `_id` in one `$and` are rejected by the server
(code 54) even when equal; this is not something MOR-003 can or should fix.

## Result on the InMemoryDriver (divergences)

The InMemoryDriver never fails on the pinned variant (it stores `gen`), and its plain variant seeds only from top-level equality, `$eq`
and `$and` branches (including nested `$and`). It does **not** seed from: single-element `$in`, single-branch `$or` (also nested in
`$and`), and it stores a `Pattern` object as the `_id` for `{_id:/^X/}` (a bug on its own: real MongoDB generates an ObjectId). For
`{$and:[{_id:X},{_id:Y}]}` it silently takes the last value (`Y`) instead of failing with code 54. These are InMem bugs to track with
MOR-030 / GitHub #399; MOR-003 does not change them.

## Consequences for MOR-003

1. The plan's draft rule (equality inside `$and` only; `$in` and `$or` never pin) would still produce code 66 for single-element `$in`,
   single-branch `$or` and dotted `_id` keys. The challenger's objection B1 is confirmed. The rule must be: "an equality on `_id` is
   reachable through `$and`/single-branch `$or`", as in the table above.
2. Top-level `containsKey("_id")` keeps meaning "do not add a generated id" (unchanged behaviour). Today it misses the dotted key
   `_id.a` at the top level, which the server seeds; the inspector must treat top-level keys starting with `_id.` as pinning.
3. Shapes that do not seed (`$gt`, regex, multi-`$in`, multi-`$or`, `$nor`) inside combinators must still get the generated id; at the
   top level the existing behaviour (no generated id, server assigns an ObjectId) stays.
4. The ledger acceptance text of MOR-003 ("`_id` inside `$and` / `$or`") is narrowed to: equality on `_id` reachable through `$and` and
   single-branch `$or`, one-element `$in`, and dotted `_id.x`; multi-branch `$or` is explicitly out of scope because the server does
   not seed there.
