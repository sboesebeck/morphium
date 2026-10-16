package de.caluga.morphium.query;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.CreationTime;
import de.caluga.morphium.annotations.LastChange;
import de.caluga.morphium.annotations.Version;
import de.caluga.morphium.writer.BufferedMorphiumWriterImpl;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builds ONE atomic update of the document a {@link Query} selects and executes it as {@code findAndModify}. Obtained with
 * {@link Query#modify()}:
 * <pre>{@code
 * // lease: conditional upsert; a second contender gets a MorphiumDriverException with isDuplicateKey() == true
 * Lease l = morphium.createQueryFor(Lease.class).f("id").eq(id).or(expired, mine).modify()
 *               .set("ownerName", me).set("expiresAt", now.plus(ttl)).upsert()
 *               .findOneAndUpdate(ReturnDocument.AFTER);
 * // counter
 * long hits = morphium.createQueryFor(Counter.class).f("id").eq(key).modify().inc("hits", 1).upsert()
 *               .findOneAndUpdate(ReturnDocument.AFTER).hits;
 * }</pre>
 * <b>Nothing is written until {@link #findOneAndUpdate(ReturnDocument)} is called</b>; unlike the eager {@code Query.set(...)}/{@code delete()},
 * the calls on this builder only collect. The builder is mutable, not thread safe (one builder per thread) and can be executed more than once.
 * <h3>What the builder does</h3>
 * <ul>
 * <li><b>Field names</b> are Java names, stored names or dotted paths ({@code child.fileName}); they are always translated to the names the fields are
 * stored under (see {@link de.caluga.morphium.AnnotationAndReflectionHelper#resolveFieldPath(Class, String)}), independent of the setting
 * {@code translateUpdateFieldNames} and of {@link Query#setTranslateUpdateFieldNames(Boolean)}. Unknown fields and {@code @Aliases} names are written as given;
 * behind a list or map segment the path is used as given.</li>
 * <li><b>Values</b> are marshalled at the call: embedded objects and entities become documents with {@code class_name}, enums are stored by name, java.time
 * values by their mapper; collections are copied, so changing a list after the call does not change the update. An entity for a {@code @Reference}
 * field and a write to an {@code @Encrypted} field are rejected at the call.</li>
 * <li><b>Merging:</b> two {@code set} calls give one {@code $set}. The same field twice is merged if the values are equal and rejected otherwise ({@code inc} twice
 * is always rejected); a path and its child, or the same path in two operators, are rejected at the call (MongoDB would answer error 40).</li>
 * <li><b>{@code @Version}:</b> unless the update touches the version field itself, it is incremented ({@code $inc 1}; an insert starts at 1) - or, if the update
 * only has {@code setOnInsert} operations, set to 1 on insert and left alone on an existing document. A later {@code store()} of an entity loaded before a
 * builder write is rejected with a {@code VersionMismatchException}. Disabled by {@link Query#disableAutoValues()}. An {@code @Version(fieldName=...)}
 * is rejected (MOR-044).</li>
 * <li><b>{@code @LastChange} / {@code @CreationTime}</b> (MOR-009): the last change is set by every update that modifies a document, the creation time only when the update
 * inserts ({@code $setOnInsert}); a setOnInsert-only update sets the last change on insert and leaves an existing document alone. A field that the update touches itself is
 * not overwritten, {@link Query#disableAutoValues()} or the thread switch turn both (and the version) off. Types: long/Long milliseconds, Date, LocalDateTime, Instant,
 * String (the {@code dateFormat} of the annotation); another type is rejected.</li>
 * </ul>
 * <h3>What the builder does NOT do</h3>
 * <ul>
 * <li>No pre/post update events are fired; {@code postLoad} is fired for the returned entity. The read cache of the type is cleared after the write.</li>
 * <li>It is no writer path: no asynchronous or buffered write (an entity with {@code @WriteBuffer} is rejected), no index or capped collection check on upsert.
 * A lease keyed by a natural key (not {@code _id}) needs a unique index on that key to have exactly one winner.</li>
 * <li>{@code @LastAccess} entities get a second, non-atomic update of their last-access field (MOR-052).</li>
 * <li>{@code $inc} is retried by the driver after a network error and may count twice; inside a real transaction a duplicate key aborts the transaction.</li>
 * </ul>
 * Rejected with {@link UnsupportedOperationException}: skip, a limit above 1, a hint and a projection on the query (and a sort or any limit for the count
 * terminals {@link #updateOne()} / {@link #updateMany()}). Honoured: filter, sort (for {@code findOneAndUpdate}), collation, array filters, database override and
 * collection name, write concern of the class.
 *
 * @param <T> the entity type
 * @apiNote incubating until 6.3: the name and package may change
 */
public final class UpdateBuilder<T> {
    private final Morphium morphium;
    private final Query<T> query;
    private final Class<? extends T> type;
    private final UpdateOperations operations = new UpdateOperations();
    private boolean upsert;

    UpdateBuilder(Morphium morphium, Query<T> query) {
        this.morphium = morphium;
        this.query = query;
        this.type = query.getType();
    }

    // ------------------------------------------------------------------ operations

    /** {@code $set}: sets the field to the value ({@code null} stores a BSON null; use {@link #unset(String...)} to remove it) */
    public UpdateBuilder<T> set(String field, Object value) {
        return add("$set", field, value);
    }

    public UpdateBuilder<T> set(Enum<?> field, Object value) {
        return add("$set", nameOf(field), value);
    }

    /** {@code $setOnInsert}: sets the field only if the update inserts a document (needs {@link #upsert()}), never touches an existing document */
    public UpdateBuilder<T> setOnInsert(String field, Object value) {
        return add("$setOnInsert", field, value);
    }

    public UpdateBuilder<T> setOnInsert(Enum<?> field, Object value) {
        return add("$setOnInsert", nameOf(field), value);
    }

    /** {@code $inc}: adds the amount (a missing field counts as 0); Integer, Long and Double are used as they are, Short and Byte become Integer, Float becomes Double */
    public UpdateBuilder<T> inc(String field, Number amount) {
        return add("$inc", field, normalise(amount));
    }

    public UpdateBuilder<T> inc(Enum<?> field, Number amount) {
        return add("$inc", nameOf(field), normalise(amount));
    }

    /** {@code $unset}: removes the fields */
    public UpdateBuilder<T> unset(String... fields) {
        if (fields == null || fields.length == 0) {
            throw new IllegalArgumentException("unset needs at least one field name");
        }

        for (String f : fields) {
            add("$unset", f, "");
        }

        return this;
    }

    public UpdateBuilder<T> unset(Enum<?>... fields) {
        if (fields == null || fields.length == 0) {
            throw new IllegalArgumentException("unset needs at least one field name");
        }

        for (Enum<?> f : fields) {
            add("$unset", nameOf(f), "");
        }

        return this;
    }

    /** insert a document if none matches the filter (the fields of equality predicates of the filter, including an {@code _id}, seed the new document) */
    public UpdateBuilder<T> upsert() {
        return upsert(true);
    }

    public UpdateBuilder<T> upsert(boolean upsert) {
        this.upsert = upsert;
        return this;
    }

    // ------------------------------------------------------------------ terminal

    /**
     * Executes the update as one {@code findAndModify} and returns the document.
     * <table border="1">
     * <caption>result</caption>
     * <tr><th>upsert</th><th>which</th><th>document matches</th><th>result</th></tr>
     * <tr><td>no</td><td>any</td><td>no</td><td>{@code null}</td></tr>
     * <tr><td>yes</td><td>BEFORE</td><td>no (inserted)</td><td>{@code null} - an insert has no pre-image</td></tr>
     * <tr><td>yes</td><td>AFTER</td><td>no (inserted)</td><td>the inserted document</td></tr>
     * <tr><td>any</td><td>BEFORE</td><td>yes</td><td>the document as it was</td></tr>
     * <tr><td>any</td><td>AFTER</td><td>yes</td><td>the updated document</td></tr>
     * <tr><td>yes</td><td>any</td><td>no, but the {@code _id} exists</td><td>{@code MorphiumDriverException}, {@code isDuplicateKey()}</td></tr>
     * </table>
     * Nothing is sent if the builder is invalid: it throws {@link IllegalStateException} for an empty builder (it would replace the document),
     * for {@code setOnInsert} operations without {@link #upsert()} (they could never do anything), for an entity with write buffering and for an
     * {@code @Version(fieldName=...)}; {@link UnsupportedOperationException} for skip, limit above 1, hint and projection.
     *
     * @param which the state to return; there is deliberately no default
     * @return the document, see the table
     */
    public T findOneAndUpdate(ReturnDocument which) {
        if (which == null) {
            throw new IllegalArgumentException("ReturnDocument must not be null");
        }

        validate(false, false);
        Map<String, Object> document = build();
        return query.findOneAndUpdateMapped(document, upsert, which == ReturnDocument.AFTER);
    }

    /**
     * Executes the update on ONE document of the filter (the first the server finds - there is no sort) as an {@code update} command and returns the counts.
     * Same validation, mapping, version and timestamp handling as {@link #findOneAndUpdate(ReturnDocument)}; additionally a sort on the query is rejected (MongoDB
     * supports it in an update statement only from 8.0, the InMemoryDriver ignores it) and a limit above 1.
     * <p>
     * A lost conditional upsert raises a {@link de.caluga.morphium.driver.MorphiumDriverException} with {@code isDuplicateKey()} on every driver (the update
     * command reports it in its reply, the query turns it into the exception), see {@link UpdateResult} for the counts.
     */
    public UpdateResult updateOne() {
        validate(true, false);
        return query.updateMapped(build(), upsert, false);
    }

    /**
     * Executes the update on ALL documents of the filter as one {@code update} command and returns the counts. A filter-less builder updates every document. On
     * an error the documents before it are updated (ordered multi update, no rollback), the exception message names the counts. {@code @Version} fields of all
     * matched documents are incremented. A limit, skip or sort on the query is rejected.
     */
    public UpdateResult updateMany() {
        validate(true, true);
        return query.updateMapped(build(), upsert, true);
    }

    /** the update document that the terminal sends (mapped keys, marshalled values, injected version); for diagnostics and tests. Does not validate. */
    public Map<String, Object> toUpdateDocument() {
        return build();
    }

    // ------------------------------------------------------------------ internals

    private UpdateBuilder<T> add(String operator, String field, Object value) {
        if (field == null || field.isEmpty()) {
            throw new IllegalArgumentException("The field name of " + operator + " must not be null or empty");
        }

        Map<String, Object> single = new LinkedHashMap<>();
        single.put(field, value);
        Map<String, Object> in = new LinkedHashMap<>();
        in.put(operator, single);
        Map<String, Object> mapped = new UpdateDocumentMapper(morphium, type, true).map(in);
        @SuppressWarnings("unchecked")
        Map<String, Object> fields = (Map<String, Object>) mapped.get(operator);
        Map.Entry<String, Object> e = fields.entrySet().iterator().next();
        operations.add(operator, e.getKey(), e.getValue());
        return this;
    }

    private static String nameOf(Enum<?> field) {
        if (field == null) {
            throw new IllegalArgumentException("The field must not be null");
        }

        return field.name();
    }

    private static Number normalise(Number amount) {
        if (amount == null) {
            throw new IllegalArgumentException("The amount of inc must not be null");
        }

        if (amount instanceof Integer || amount instanceof Long || amount instanceof Double) {
            return amount;
        }

        if (amount instanceof Short || amount instanceof Byte) {
            return amount.intValue();
        }

        if (amount instanceof Float) {
            return amount.doubleValue();
        }

        throw new IllegalArgumentException("inc supports Integer, Long, Double (and Short, Byte, Float, which are widened), not " + amount.getClass().getName());
    }

    private void validate(boolean countTerminal, boolean many) {
        if (operations.isEmpty()) {
            throw new IllegalStateException("The update is empty: add at least one set, setOnInsert, inc or unset (an empty update document would REPLACE the document)");
        }

        if (operations.hasOnlySetOnInsert() && !upsert) {
            throw new IllegalStateException("setOnInsert without upsert() can never do anything: call upsert() or use set()");
        }

        if (query.getSkip() != 0) {
            throw new UnsupportedOperationException("skip is not supported by the update builder");
        }

        if (many && query.getLimit() != 0) {
            throw new UnsupportedOperationException("a limit is not supported by updateMany: it updates every document of the filter");
        }

        if (query.getLimit() > 1) {
            throw new UnsupportedOperationException("a limit above 1 is not supported by the update builder: findAndModify and updateOne update one document");
        }

        if (countTerminal && query.getSort() != null && !query.getSort().isEmpty()) {
            throw new UnsupportedOperationException("a sort is not supported by updateOne/updateMany: MongoDB supports it in an update statement only from 8.0 and the InMemoryDriver ignores it. Use findOneAndUpdate");
        }

        if (query.getHint() != null) {
            throw new UnsupportedOperationException("a hint is not supported by the update builder (findAndModify with a hint is not wired yet, MOR-008)");
        }

        if (query.hasProjection()) {
            throw new UnsupportedOperationException("a projection is not supported by the update builder (MOR-008)");
        }

        if (morphium.getWriterForClass(type) instanceof BufferedMorphiumWriterImpl) {
            throw new IllegalStateException(type.getName() + " is written through the buffered writer (@WriteBuffer): a buffered store could overwrite an atomic update later. "
                                            + "Use Query.set for such entities or disable the write buffer for the thread");
        }
    }

    /** the operator document as it is sent: the operations plus the injected version */
    private Map<String, Object> build() {
        Map<String, Object> doc = operations.toOperatorMap();

        if (query.isAutoValuesEnabled() && morphium.isAutoValuesEnabledForThread()) {
            injectVersion(doc);
            injectTimestamps(doc);
        }

        // the keys and values are mapped already; the pass makes sure that what is sent is what the mapper would send
        return new UpdateDocumentMapper(morphium, type, true).map(doc);
    }

    /**
     * {@code @LastChange} is set by every update that modifies a document (and by an insert), {@code @CreationTime} only by an insert ({@code $setOnInsert}); a field
     * that the update touches itself is left alone. Values and types as {@code store()} writes them: long/Long milliseconds, Date, LocalDateTime (system zone), String
     * (the {@code dateFormat} of the annotation) - and Instant; any other type is rejected instead of silently storing null as {@code store()} does.
     */
    @SuppressWarnings("unchecked")
    private void injectTimestamps(Map<String, Object> doc) {
        List<String> lastChange = morphium.getARHelper().getFields(type, LastChange.class);
        List<String> creation = morphium.getARHelper().getFields(type, CreationTime.class);

        if (lastChange.isEmpty() && creation.isEmpty()) {
            return;
        }

        long now = System.currentTimeMillis();
        Set<String> done = new HashSet<>();
        // a document that is only inserted (setOnInsert-only update) gets its last change on insert, an existing one stays untouched
        String lastChangeOperator = operations.hasModifyingOperator() ? "$set" : "$setOnInsert";

        for (String name : lastChange) {
            String stored = morphium.getARHelper().resolveStoredFieldName(type, name);
            Field f = morphium.getARHelper().getField(type, name);

            if (operations.touches(stored) || !done.add(stored)) {
                continue;
            }

            Object value = timestamp(f, now, f.getAnnotation(LastChange.class) != null ? f.getAnnotation(LastChange.class).dateFormat() : "yyyy-MM-dd hh:mm:ss", "@LastChange");
            ((Map<String, Object>) doc.computeIfAbsent(lastChangeOperator, k -> new LinkedHashMap<String, Object>())).put(stored, value);
        }

        for (String name : creation) {
            String stored = morphium.getARHelper().resolveStoredFieldName(type, name);
            Field f = morphium.getARHelper().getField(type, name);

            if (operations.touches(stored) || !done.add(stored)) {
                continue;
            }

            Object value = timestamp(f, now, f.getAnnotation(CreationTime.class) != null ? f.getAnnotation(CreationTime.class).dateFormat() : "yyyy-MM-dd hh:mm:ss", "@CreationTime");
            ((Map<String, Object>) doc.computeIfAbsent("$setOnInsert", k -> new LinkedHashMap<String, Object>())).put(stored, value);
        }
    }

    private Object timestamp(Field f, long now, String dateFormat, String annotation) {
        Class<?> t = f.getType();

        if (t.equals(long.class) || t.equals(Long.class)) {
            return now;
        }

        if (t.equals(Date.class)) {
            return new Date(now);
        }

        if (t.equals(LocalDateTime.class)) {
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.systemDefault());
        }

        if (t.equals(Instant.class)) {
            return Instant.ofEpochMilli(now);
        }

        if (t.equals(String.class)) {
            return new SimpleDateFormat(dateFormat).format(now);
        }

        throw new IllegalStateException(annotation + " on " + type.getName() + "." + f.getName() + " has the type " + t.getName()
                                        + ", supported are long, Long, Date, LocalDateTime, Instant and String");
    }

    @SuppressWarnings("unchecked")
    private void injectVersion(Map<String, Object> doc) {
        List<String> versionFields = morphium.getARHelper().getFields(type, Version.class);

        if (versionFields.isEmpty()) {
            return;
        }

        String javaName = versionFields.get(0);
        Field f = morphium.getARHelper().getField(type, javaName);

        if (f != null && f.isAnnotationPresent(Version.class) && !".".equals(f.getAnnotation(Version.class).fieldName())) {
            throw new IllegalStateException("@Version(fieldName=...) on " + type.getName() + "." + f.getName() + " is not supported by the update builder: the name the version is stored under "
                                            + "and the name the filter uses differ (MOR-044). Remove the fieldName");
        }

        String stored = morphium.getARHelper().resolveStoredFieldName(type, javaName);

        if (operations.touches(stored)) {
            return; // an explicit touch of the version wins
        }

        if (operations.hasModifyingOperator()) {
            ((Map<String, Object>) doc.computeIfAbsent("$inc", k -> new LinkedHashMap<String, Object>())).put(stored, 1L);
        } else {
            ((Map<String, Object>) doc.computeIfAbsent("$setOnInsert", k -> new LinkedHashMap<String, Object>())).put(stored, 1L);
        }
    }
}
