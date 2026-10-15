# Serialisation of update operator maps: decision

## Decision

Five options were considered for serialising update operator maps; the table gives the verdict, the evidence and the owning task for each.

| Option | Verdict | Evidence | Task |
|---|---|---|---|
| A, mapper-aware update builder | Adopt as target API (not in this change) | Update-path patterns 2, 5 and 7 of docs/research/other-clients.md; needed for Jakarta Data `UPDATE ... SET` (docs/research/jakarta-data-1.1.md, section 4) | MOR-006 |
| B, marshal operator maps in `Query.findOneAndUpdate` | Adopt now | Hypothesis 1 of docs/research/reproduction-results.md (six shapes fail); patterns 1 and 4 of docs/research/other-clients.md | MOR-002 and MOR-003 |
| C, POJO fallback hook in `BsonEncoder` | Reject | The InMemoryDriver does not use the encoder, so it would still diverge (hypothesis 4); the driver layer would depend on the mapper; the encoder knows no entity type, so it cannot translate keys | None (MOR-029 gives the InMemoryDriver the same up-front rejection) |
| D, embedded classes inherit `translateCamelCase` from the owning entity | Opt-in only, never the default | Hypothesis 2; no other client makes embedded naming depend on its owner (pattern 15 of docs/research/other-clients.md); a silent change would rename stored keys | MOR-015 and MOR-016 |
| E, startup guard for an `@Embedded` under an `@Entity(translateCamelCase=false)` | Adopt | Cheap, non-breaking, would have caught hypothesis 2 | MOR-014 |

## Room for the builder (option A)

MOR-002 introduces a package-private `UpdateDocumentMapper` in `de.caluga.morphium.query` and splits `Query.findOneAndUpdate` into a mapping step and a private `findOneAndUpdateMapped(Map alreadyMapped, boolean upsert, boolean returnNew)`. The builder (MOR-006) and the options-object overload (MOR-008) call the second part directly, so they can pass in a document they have already mapped. Mapping is idempotent, so a double pass is harmless. Nested and positional paths (`a.b`, `$`, `$[x]`) and `@Reference` fields are not handled by MOR-002.

## Finding: marshallIfNecessary must not become the mapper API

The existing `ObjectMapperImpl.marshallIfNecessary(Object)` must NOT be exposed on `MorphiumObjectMapper` for update values. Reasons:

- Custom-mapped values go through `serialize()`, which wraps a non-Map result as `{"value": x}` and adds `class_name` to Map results (ObjectMapperImpl.java:310-312, ObjectMapperImpl.java:391, ObjectMapperImpl.java:394).
- An `Iterable` is serialised with `serializeIterable(it, null, null)` (ObjectMapperImpl.java:333-335) and enums then become `{class_name, name}` (ObjectMapperImpl.java:785-786, ObjectMapperImpl.java:920-924), while a raw `List<Enum>` reaches the encoder today as a plain string (BsonEncoder.java:354-357).
- Arrays: elements are marshalled first and then passed to `serializeIterable` (ObjectMapperImpl.java:318-331), so array elements get no `class_name` while Iterable elements get one (ObjectMapperImpl.java:775-778).
- Its only callers outside the class are the two aggregators, so it is not a general-purpose API. A new `marshallUpdateValue(Object)` that rewrites only POJOs and enums is used instead.

## Phasing

1. PR 1: values and the `_id` pin rule - MOR-002, MOR-003.
2. PR 2: key translation for operator maps behind a configuration switch - MOR-041.
3. PR 3: writer path delegation - MOR-042.

The `_id` pin rule is based on the measurements in docs/research/upsert-id-seeding-probe.md.


---

> **Superseded in part (reconciliation with upstream #335, see plan-reconcile-335.md):** the update-value walk has two positions (whole field / element) and follows `store()` for custom-mapped values; `BigDecimal`, `Character` and `Timestamp` are no longer left as they are, and a single embedded value carries `class_name` in both positions.
