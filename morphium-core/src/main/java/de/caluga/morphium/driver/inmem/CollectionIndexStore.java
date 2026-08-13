package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.driver.MorphiumDriverException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Per-collection index storage and incremental maintenance for the {@link InMemoryDriver}.
 *
 * <p><b>Thread-safety contract:</b> this class is <em>not</em> internally synchronized. Every
 * method here must be called while holding the owning collection's write lock - the same lock the
 * driver already takes around mutations of the collection's document list. Concurrent calls to any
 * method of one instance (from different threads without external locking) are not safe, including
 * pure reads racing a mutation.
 *
 * <p><b>Structure trade-off:</b> each index keeps both a {@code HashMap<IndexKey, ArrayList<...>>}
 * for O(1) equality lookups and a {@code TreeMap<IndexKey, ArrayList<...>>} (ordered by
 * {@link IndexKey#comparator(IndexDefinition)}) for range/ordered scans. Both maps are kept
 * pointing at the very same {@code ArrayList} instance per key, so they can never drift apart, but
 * the index still pays for two map entries (hash bucket + tree node) per distinct key. The TreeMap
 * alone could serve equality lookups too (O(log n)) - the HashMap trades that extra memory for O(1)
 * point lookups. If profiling later shows this is too fat, dropping the HashMap and doing equality
 * lookups via {@code TreeMap.get} is a one-line change per call site.
 *
 * <p><b>Document identity:</b> index entries hold references to the exact same
 * {@code Map<String,Object>} instances the caller passes in - never copies. Callers (the driver)
 * rely on this: an in-place update to a document is reflected by every index bucket that still
 * holds it without any action, which is exactly why {@link #onUpdate} only needs to touch indexes
 * whose extracted key actually changed.
 *
 * <p>The {@code _id} index (name {@value #ID_INDEX_NAME}, unique) always exists and cannot be
 * removed via {@link #removeIndex}.
 */
public class CollectionIndexStore {
    public static final String ID_INDEX_NAME = "_id_";

    private final Map<String, IndexEntry> indexesByName = new LinkedHashMap<>();

    /**
     * Counts index entries actually yielded by {@link #orderedScan}/{@link #rangeScan} iterators
     * (one increment per document handed out by {@code next()}). Package-private test hook (Phase
     * B1, Task 6): an index-order sort with {@code limit} must only ever touch about
     * {@code skip + limit} entries of a large index - this counter is how a test proves the scan
     * iterators are genuinely lazy and never materialize the whole range up front. Not atomic,
     * same single-threaded-under-collection-lock reasoning as every other member here.
     */
    long scannedEntries;

    public CollectionIndexStore() {
        Map<String, Object> idIndexMap = new LinkedHashMap<>();
        idIndexMap.put("_id", 1);
        idIndexMap.put("$options", Map.of("name", ID_INDEX_NAME, "unique", true));
        IndexDefinition idDefinition = IndexDefinition.fromIndexMap(idIndexMap);
        indexesByName.put(ID_INDEX_NAME, new IndexEntry(idDefinition));
    }

    /**
     * Bulk-builds a new index over {@code existingDocs} and registers it. Validate-then-apply: if
     * {@code def} is unique and a duplicate key is found among {@code existingDocs}, nothing is
     * registered - the store is left exactly as it was before the call.
     *
     * @throws MorphiumDriverException carrying MongoDB duplicate-key shape (code {@code 11000})
     *                                 if {@code def} is unique and two documents share a key
     */
    public void addIndex(IndexDefinition def, Iterable<Map<String, Object>> existingDocs) {
        String name = indexNameOf(def);
        IndexEntry entry = new IndexEntry(def);

        for (Map<String, Object> doc : existingDocs) {
            IndexKey key = IndexKey.extract(doc, def);
            if (collidesOnUnique(entry, key, doc)) {
                throw duplicateKeyException(name, key);
            }
            entry.add(key, doc);
        }

        indexesByName.put(name, entry);
    }

    /**
     * Bulk-seeds the built-in unique {@code _id_} index from a collection's existing documents.
     *
     * <p>Needed by {@link InMemoryDriver} when it (re)builds a persistent store over an
     * already-populated collection: {@link #addIndex} seeds the <em>secondary</em> indexes, but the
     * {@code _id_} index is created empty by the constructor and is otherwise only ever filled
     * incrementally by {@link #onInsert}. A from-scratch rebuild (forced by an index add/drop,
     * collection rename, transaction commit, or drop - see {@code invalidateIndexStore}) therefore
     * has to seed it explicitly here, or every {@code _id} equality lookup (e.g. {@code findById},
     * the messaging lock-then-refetch cycle) would silently return nothing for every document that
     * already existed before the rebuild. Validate-then-nothing-special: a genuine collection never
     * holds two documents with the same {@code _id}, so a duplicate here signals a corrupt caller.
     *
     * @throws MorphiumDriverException carrying MongoDB duplicate-key shape (code {@code 11000}) if
     *                                 two documents share an {@code _id}
     */
    public void seedIdIndex(Iterable<Map<String, Object>> existingDocs) {
        IndexEntry idEntry = indexesByName.get(ID_INDEX_NAME);
        for (Map<String, Object> doc : existingDocs) {
            IndexKey key = IndexKey.extract(doc, idEntry.definition);
            if (idEntry.hasBucket(key)) {
                throw duplicateKeyException(ID_INDEX_NAME, key);
            }
            idEntry.add(key, doc);
        }
    }

    /**
     * Drops an index by name. A no-op if no such index exists.
     *
     * @throws IllegalArgumentException if {@code name} is the {@code _id} index - it always exists
     */
    public void removeIndex(String name) {
        if (ID_INDEX_NAME.equals(name)) {
            throw new IllegalArgumentException("Cannot remove the " + ID_INDEX_NAME + " index");
        }
        indexesByName.remove(name);
    }

    /**
     * The subset of {@link #definitions()} a query planner may serve lookups from: everything
     * except indexes that have gone multikey. A terminal {@code List} is stored as ONE key rather
     * than one entry per element ({@link IndexKey#extract}), so a lookup key built from a scalar
     * query value never matches it and an index-backed query would silently answer "no documents"
     * where an unindexed collection answers correctly (#289). Excluding such an index sends the
     * query back to the scan, which evaluates MongoDB's array semantics properly - correct, just
     * not accelerated. Restoring acceleration needs real per-element multikey indexing, which
     * {@code IndexKey} already flags as a follow-up.
     */
    public Collection<IndexDefinition> planningDefinitions() {
        List<IndexDefinition> defs = new ArrayList<>(indexesByName.size());
        for (IndexEntry entry : indexesByName.values()) {
            if (!entry.multikey) {
                defs.add(entry.definition);
            }
        }
        return Collections.unmodifiableList(defs);
    }

    /** All currently registered index definitions, including the {@code _id} index. */
    public Collection<IndexDefinition> definitions() {
        List<IndexDefinition> defs = new ArrayList<>(indexesByName.size());
        for (IndexEntry entry : indexesByName.values()) {
            defs.add(entry.definition);
        }
        return Collections.unmodifiableList(defs);
    }

    /**
     * Same as {@link #definitions()}, but keyed by each index's internal store name - the same
     * name {@link #rangeScan}/{@link #orderedScan} address. {@link #definitions()} alone drops
     * that association, which a caller needs when it has to locate and scan a *specific* index
     * (e.g. {@code InMemoryDriver}'s TTL expiry-queue bootstrap, which must find the TTL field's
     * own secondary index by its {@link IndexDefinition#expireAfterSeconds()} and then range-scan
     * it by name).
     */
    public Map<String, IndexDefinition> definitionsByName() {
        Map<String, IndexDefinition> out = new LinkedHashMap<>(indexesByName.size());
        for (Map.Entry<String, IndexEntry> e : indexesByName.entrySet()) {
            out.put(e.getKey(), e.getValue().definition);
        }
        return Collections.unmodifiableMap(out);
    }

    /**
     * Adds {@code doc} to every index. Validate-then-apply across all indexes: unique keys are
     * checked first, and only if every check passes is {@code doc} actually inserted into any
     * index's structures.
     *
     * @throws MorphiumDriverException carrying MongoDB duplicate-key shape (code {@code 11000})
     *                                 if {@code doc}'s key collides with an existing document in
     *                                 some unique index
     */
    public void onInsert(Map<String, Object> doc) {
        Map<IndexEntry, IndexKey> keys = new LinkedHashMap<>(indexesByName.size());

        for (IndexEntry entry : indexesByName.values()) {
            IndexKey key = IndexKey.extract(doc, entry.definition);
            keys.put(entry, key);
            if (collidesOnUnique(entry, key, doc)) {
                throw duplicateKeyException(indexNameOf(entry.definition), key);
            }
        }

        for (Map.Entry<IndexEntry, IndexKey> e : keys.entrySet()) {
            e.getKey().add(e.getValue(), doc);
        }
    }

    /**
     * Whether inserting {@code doc} under {@code key} would violate {@code entry}'s unique
     * constraint. Beyond the plain "some other document already holds this key", two MongoDB rules
     * take documents out of an index entirely - and a document that is not in the index cannot
     * collide in it:
     *
     * <ul>
     *   <li>{@code sparse}: a document containing none of the indexed fields;
     *   <li>{@code partialFilterExpression}: a document not matching that query. Note this cuts
     *       both ways - the incoming document is exempt if it does not match, and an already
     *       stored document sitting in the same bucket does not count as a collision partner if
     *       <em>it</em> does not match (the filter may well select on a field that is not part of
     *       the index key, so uncovered and covered documents share buckets).
     * </ul>
     */
    private boolean collidesOnUnique(IndexEntry entry, IndexKey key, Map<String, Object> doc) {
        IndexDefinition def = entry.definition;

        if (!def.unique() || (def.sparse() && key.allMissing()) || !coveredByPartialFilter(def, doc)) {
            return false;
        }

        if (def.partialFilterExpression() == null) {
            return entry.hasBucket(key);
        }

        List<Map<String, Object>> bucket = entry.bucket(key);
        if (bucket == null) {
            return false;
        }
        for (Map<String, Object> other : bucket) {
            if (other != doc && coveredByPartialFilter(def, other)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether {@code doc} is part of {@code def}'s index at all, as far as its
     * {@code partialFilterExpression} is concerned. Always true for an index without one.
     *
     * <p>Documents outside the filter are still <em>stored</em> in the index here (like sparse
     * ones): lookups must stay complete, and only the uniqueness check honours the filter.
     */
    private static boolean coveredByPartialFilter(IndexDefinition def, Map<String, Object> doc) {
        Map<String, Object> filter = def.partialFilterExpression();
        return filter == null || QueryHelper.matchesQuery(filter, doc, null);
    }

    /** Removes {@code doc} (matched by reference identity) from every index. */
    public void onRemove(Map<String, Object> doc) {
        for (IndexEntry entry : indexesByName.values()) {
            IndexKey key = IndexKey.extract(doc, entry.definition);
            entry.remove(key, doc);
        }
    }

    /**
     * Updates every index for a document that changed from {@code before} to {@code after}.
     * {@code after} is expected to be the very same live object that is already referenced by
     * every index bucket (the driver mutates documents in place) - only indexes whose extracted
     * {@link IndexKey} actually changed between {@code before} and {@code after} are touched at
     * all. Validate-then-apply: unique keys for changed indexes are checked first, and only if
     * every check passes are any structures mutated.
     *
     * <p><b>CALLER OBLIGATION ON FAILURE - read before wiring this into the driver.</b> When this
     * method throws, the store's buckets are untouched: the document is still registered under its
     * OLD keys and not under the new ones. But if the caller has already mutated the live document
     * in place before calling (the driver's usual pattern), the doc's <em>fields</em> now carry
     * the NEW values while the <em>index</em> still files it under the OLD keys - a mismatch the
     * store cannot detect or heal by itself, which silently corrupts every subsequent index lookup
     * for that document. Therefore, if {@code onUpdate} throws, the caller MUST either revert the
     * in-place mutation of the document (restore the {@code before} state), or avoid the problem
     * entirely by validating against the store BEFORE mutating the document. Failing to do so
     * permanently desyncs index and data.
     *
     * @throws MorphiumDriverException carrying MongoDB duplicate-key shape (code {@code 11000})
     *                                 if a changed key collides with a different existing document
     *                                 in some unique index; see the caller obligation above
     */
    public void onUpdate(Map<String, Object> before, Map<String, Object> after) {
        List<IndexEntry> changedEntries = new ArrayList<>();
        List<IndexKey> oldKeys = new ArrayList<>();
        List<IndexKey> newKeys = new ArrayList<>();

        for (IndexEntry entry : indexesByName.values()) {
            IndexKey oldKey = IndexKey.extract(before, entry.definition);
            IndexKey newKey = IndexKey.extract(after, entry.definition);
            if (!oldKey.equals(newKey)) {
                changedEntries.add(entry);
                oldKeys.add(oldKey);
                newKeys.add(newKey);
            }
        }

        for (int i = 0; i < changedEntries.size(); i++) {
            IndexEntry entry = changedEntries.get(i);
            if (!entry.definition.unique()) {
                continue;
            }
            IndexKey newKey = newKeys.get(i);
            if (entry.definition.sparse() && newKey.allMissing()) {
                continue;
            }
            if (!coveredByPartialFilter(entry.definition, after)) {
                continue;
            }
            List<Map<String, Object>> bucket = entry.bucket(newKey);
            if (bucket != null) {
                for (Map<String, Object> other : bucket) {
                    if (other != after && coveredByPartialFilter(entry.definition, other)) {
                        throw duplicateKeyException(indexNameOf(entry.definition), newKey);
                    }
                }
            }
        }

        for (int i = 0; i < changedEntries.size(); i++) {
            IndexEntry entry = changedEntries.get(i);
            entry.remove(oldKeys.get(i), after);
            entry.add(newKeys.get(i), after);
        }
    }

    /**
     * True if a document with {@code id} as its {@code _id} is currently registered in the
     * built-in unique {@code _id_} index - a single O(1) hash lookup, no scan. {@code id} is
     * normalized the same way stored keys are (see {@link IndexKey#of}), so a
     * {@code MorphiumId} caller matches a stored {@code ObjectId} and vice versa. Callers must
     * pass a non-null {@code id}: stored null/absent {@code _id}s are filed under
     * {@link IndexKey#MISSING}, which a raw {@code null} here would never match.
     */
    public boolean containsId(Object id) {
        IndexEntry idEntry = indexesByName.get(ID_INDEX_NAME);
        return idEntry.hasBucket(IndexKey.of(Collections.singletonList(id)));
    }

    /** Documents whose extracted key on the named index equals {@code key}, in insertion order. */
    public List<Map<String, Object>> equalityLookup(String indexName, IndexKey key) {
        IndexEntry entry = requireEntry(indexName);
        List<Map<String, Object>> bucket = entry.bucket(key);
        return bucket == null ? Collections.emptyList() : new ArrayList<>(bucket);
    }

    /**
     * Scans the named index between {@code from} and {@code to} (each bound optional - pass
     * {@code null} for an open end), honouring {@code fromInclusive}/{@code toInclusive}, in the
     * index's natural order or reversed if {@code descending}.
     *
     * <p>The returned iterator is <b>lazy</b> - see {@link #orderedScan} for the shared validity
     * contract (only valid under the collection lock, invalidated by any store mutation).
     */
    public Iterator<Map<String, Object>> rangeScan(String indexName, IndexKey from, boolean fromInclusive,
            IndexKey to, boolean toInclusive, boolean descending) {
        IndexEntry entry = requireEntry(indexName);
        NavigableMap<IndexKey, ArrayList<Map<String, Object>>> view = entry.ordered;

        if (from != null && to != null) {
            view = view.subMap(from, fromInclusive, to, toInclusive);
        } else if (from != null) {
            view = view.tailMap(from, fromInclusive);
        } else if (to != null) {
            view = view.headMap(to, toInclusive);
        }

        return flatten(view, descending);
    }

    /**
     * Every document in the named index, in the index's natural order or reversed.
     *
     * <p>The returned iterator is <b>lazy</b>: it streams the live {@code TreeMap} bucket
     * structures on demand instead of materializing the whole range up front (an earlier version
     * copied every scanned entry into a list before returning - that eager copy defeated the whole
     * point of limit-driven consumers, which want to stop after {@code skip + limit} entries of a
     * potentially huge index). The flip side is a validity contract, which is really just this
     * class's general thread-safety contract applied to a longer-lived object: the iterator is
     * only valid while the owning collection's lock (see the class Javadoc) is held continuously
     * from the call until the last {@code next()}, and any mutation of this store invalidates it
     * (a structural change to the underlying {@code TreeMap} fails fast with
     * {@code ConcurrentModificationException}; an in-bucket-only change may not be detected).
     * {@code InMemoryDriver.find} complies: it consumes scan iterators fully inside the region
     * where it holds the collection's read lock, and never mutates while reading.
     */
    public Iterator<Map<String, Object>> orderedScan(String indexName, boolean descending) {
        IndexEntry entry = requireEntry(indexName);
        return flatten(entry.ordered, descending);
    }

    /**
     * Lazily streams every document of {@code view}'s buckets in key order ({@code descending}
     * walks the {@code descendingMap} view - both directions are O(1) to set up). One
     * {@link #scannedEntries} increment per document actually handed out by {@code next()}, which
     * is what makes the counter a faithful "entries touched" measure for laziness tests.
     */
    private Iterator<Map<String, Object>> flatten(NavigableMap<IndexKey, ArrayList<Map<String, Object>>> view,
            boolean descending) {
        NavigableMap<IndexKey, ArrayList<Map<String, Object>>> ordered = descending ? view.descendingMap() : view;
        Iterator<ArrayList<Map<String, Object>>> buckets = ordered.values().iterator();

        return new Iterator<>() {
            private Iterator<Map<String, Object>> currentBucket = Collections.emptyIterator();

            @Override
            public boolean hasNext() {
                // Empty buckets are removed eagerly (see IndexEntry.remove), but stay defensive:
                // skip any empty bucket rather than reporting a phantom element.
                while (!currentBucket.hasNext() && buckets.hasNext()) {
                    currentBucket = buckets.next().iterator();
                }
                return currentBucket.hasNext();
            }

            @Override
            public Map<String, Object> next() {
                if (!hasNext()) {
                    throw new java.util.NoSuchElementException();
                }
                scannedEntries++;
                return currentBucket.next();
            }
        };
    }

    private IndexEntry requireEntry(String indexName) {
        IndexEntry entry = indexesByName.get(indexName);
        if (entry == null) {
            throw new IllegalArgumentException("Unknown index: " + indexName);
        }
        return entry;
    }

    private static String indexNameOf(IndexDefinition def) {
        if (def.name() != null) {
            return def.name();
        }
        StringBuilder name = new StringBuilder();
        for (String field : def.fields()) {
            if (name.length() > 0) {
                name.append('_');
            }
            name.append(field).append('_').append(def.direction(field));
        }
        return name.toString();
    }

    private static MorphiumDriverException duplicateKeyException(String indexName, IndexKey key) {
        MorphiumDriverException ex = new MorphiumDriverException(
                "E11000 duplicate key error index: " + indexName + " dup key: " + key);
        ex.setMongoCode(11000);
        return ex;
    }

    /**
     * Holds one index's dual structures. The {@code HashMap} and {@code TreeMap} always share the
     * very same {@code ArrayList} instance for a given key - see the class Javadoc's structure
     * trade-off note.
     */
    private static final class IndexEntry {
        final IndexDefinition definition;
        final Map<IndexKey, ArrayList<Map<String, Object>>> byKey = new HashMap<>();
        final TreeMap<IndexKey, ArrayList<Map<String, Object>>> ordered;
        /**
         * Set once any indexed document holds a {@code List} for one of this index's fields -
         * mongod's "multikey" property, learned from the data rather than declared. Since
         * {@link IndexKey#extract} keeps such a list as ONE key instead of expanding it per
         * element, no lookup key built from a scalar query value can match it, and serving a
         * query from this index would silently return nothing (#289). It is therefore excluded
         * from {@link #planningDefinitions()} and the query falls back to the scan, which
         * evaluates MongoDB's array semantics correctly. Never cleared: once multikey, an index
         * stays suspect for its lifetime, exactly as in mongod.
         */
        boolean multikey;

        IndexEntry(IndexDefinition definition) {
            this.definition = definition;
            this.ordered = new TreeMap<>(IndexKey.comparator(definition));
        }

        boolean hasBucket(IndexKey key) {
            List<Map<String, Object>> bucket = byKey.get(key);
            return bucket != null && !bucket.isEmpty();
        }

        List<Map<String, Object>> bucket(IndexKey key) {
            return byKey.get(key);
        }

        void add(IndexKey key, Map<String, Object> doc) {
            // Every path that populates an index goes through here (createIndex's bulk build, the
            // _id index build, onInsert, onUpdate), which makes this the one place that reliably
            // sees whether a document turns this index multikey - see the field's javadoc (#289).
            if (!multikey && key.hasListValue()) {
                multikey = true;
            }
            ArrayList<Map<String, Object>> bucket = byKey.get(key);
            if (bucket == null) {
                bucket = new ArrayList<>();
                byKey.put(key, bucket);
                ordered.put(key, bucket);
            }
            bucket.add(doc);
        }

        void remove(IndexKey key, Map<String, Object> doc) {
            ArrayList<Map<String, Object>> bucket = byKey.get(key);
            if (bucket == null) {
                return;
            }
            Iterator<Map<String, Object>> it = bucket.iterator();
            while (it.hasNext()) {
                if (it.next() == doc) {
                    it.remove();
                    break;
                }
            }
            if (bucket.isEmpty()) {
                byKey.remove(key);
                ordered.remove(key);
            }
        }
    }
}
