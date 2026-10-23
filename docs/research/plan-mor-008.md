# Plan: MOR-008 options object for findAndModify (short)

## Design

- `FindAndModifyOptions` (final class, fluent setters, incubating until 6.3) in `de.caluga.morphium.query`: `upsert(boolean)`, `returnDocument(ReturnDocument)` (**required**, no default, like the builder), `sort(Map)`, `projection(Map)`, `hint(String|Map)`, `collation(Collation)`, `arrayFilters(List|Map...)`, `maxTime(Duration)`. An option that is not set falls back to what the query already carries (sort, collation, array filters), exactly as `findOneAndUpdate(Map, boolean, boolean)` does today. `projection`, `hint` and `maxTime` come only from the options (the query-level ones stay ignored by the legacy path and rejected by the builder: no behaviour change for existing callers).
- Raw map path: `Query.findOneAndUpdate(Map update, FindAndModifyOptions)`; the old `(Map, boolean upsert, boolean returnNew)` delegates to it. Same mapping of the update document as before.
- Builder: `UpdateBuilder.findOneAndUpdate(FindAndModifyOptions)` and `tryFindOneAndUpdate(FindAndModifyOptions)`; `upsert` is the OR of the builder's `upsert()` and the option (the builder cannot be un-upserted); sort, projection, hint, collation, maxTime of the options replace those of the query; the validations of the builder (empty update, `setOnInsert` without upsert, `@WriteBuffer`, limit) stay. The count terminals keep rejecting projection and hint.
- Projection keys are Java or stored names and are translated with `resolveFieldPath` (an inclusion projection also keeps `class_name`); the returned entity is partial.
- `FindAndModifyMongoCommand` gets `maxTimeMS`. The InMemoryDriver applies the projection (`fields`) to the returned document, accepts `hint` and `maxTimeMS` and ignores them (documented), honours sort, collation and array filters as before.
- The result of an upsert that inserts with `BEFORE` is `null` (no pre-image): documented in the Javadoc of `ReturnDocument`, the options and both terminals; `tryFindOneAndUpdate` (MOR-010) tells it from "nothing matched".

## Tests (first)

`FindAndModifyOptionsTest` (pure: defaults, fluent setters, validation) and `ModifyOptionsTest` (both drivers): upsert with both return documents (BEFORE of an insert is `null`, covered on a real MongoDB), options sort overrides query sort, projection (include, exclude, Java names translated, `_id` kept, class_name), hint by name and by key document, array filters, `maxTime` accepted, collation (real MongoDB only: the InMemoryDriver does not implement collations), `returnDocument` required, legacy overload equals the options form, builder terminals with options (projection and hint on the builder, builder `upsert()` wins over `upsert(false)`, `tryFindOneAndUpdate(options)` outcome), count terminals still reject projection and hint.

Not in this task: the Jakarta Data annotations (MOR-021), `findOneAndReplace`/`findOneAndDelete` options.
