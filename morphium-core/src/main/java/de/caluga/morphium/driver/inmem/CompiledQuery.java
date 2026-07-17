package de.caluga.morphium.driver.inmem;

import java.text.Collator;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.bson.types.ObjectId;

import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;

/**
 * Compiles a query document into a tree of predicate nodes ONCE, so that matching it against many
 * documents (e.g. scanning a collection) does not repeat per-document work the interpreter
 * ({@link QueryHelper#matchesQueryInterpreted}) does on every single call: splitting dotted paths
 * via regex, compiling regex patterns, building $in/$nin HashSets, parsing {@link Expr} trees, and
 * resolving the collation Collator.
 *
 * <p>CORRECTNESS CONTRACT: {@link #matches(Map)} must behave bit-identically to
 * {@code QueryHelper.matchesQueryInterpreted} for every query/document pair, including its known
 * quirks (see {@code // KNOWN-DIVERGENCE} markers below for the narrow, deliberately-not-replicated
 * corners). This is enforced by the differential test {@code CompiledQueryTest}.
 *
 * <p>Operators that are rare, exotic, or already effectively free of per-document redundant work
 * (geo predicates, bitwise predicates, $jsonSchema, $text, $where, $comment, and any operator this
 * class does not special-case) are NOT hand-compiled; a leaf node for those delegates straight to
 * {@link QueryHelper#matchesFieldCondition}, which is byte-for-byte the same code the interpreter
 * runs. This keeps the interpreter as the single source of truth for those operators while still
 * caching the split path / lookup once per field where relevant.
 */
public final class CompiledQuery {
    private final Node root;

    private CompiledQuery(Node root) {
        this.root = root;
    }

    public static CompiledQuery compile(Map<String, Object> query) {
        return compile(query, null);
    }

    public static CompiledQuery compile(Map<String, Object> query, Map<String, Object> collation) {
        Ctx ctx = new Ctx(collation);
        Node node = (query == null || query.isEmpty()) ? TrueNode.INSTANCE : compileQueryNode(query, ctx);
        return new CompiledQuery(node);
    }

    public boolean matches(Map<String, Object> doc) {
        return root.test(doc);
    }

    // ------------------------------------------------------------------
    // Identity-keyed bounded LRU used by QueryHelper.matchesQuery(query, doc, collation).
    // Keyed by query-map IDENTITY (not deep-equals): drivers/aggregators re-pass the very same
    // Map instance for every document of one operation, so identity is both sufficient and much
    // cheaper than hashing/deep-equality of a potentially large query document.
    // ------------------------------------------------------------------
    private static final int LRU_CAPACITY = 256;

    static final AtomicLong CACHE_HITS = new AtomicLong();
    static final AtomicLong CACHE_MISSES = new AtomicLong();

    private static final class IdentityKey {
        private final Map<String, Object> query;

        IdentityKey(Map<String, Object> query) {
            this.query = query;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof IdentityKey && ((IdentityKey) o).query == query;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(query);
        }
    }

    private static final Map<IdentityKey, CompiledQuery> CACHE = java.util.Collections.synchronizedMap(
    new LinkedHashMap<>(LRU_CAPACITY, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<IdentityKey, CompiledQuery> eldest) {
            return size() > LRU_CAPACITY;
        }
    });

    /**
     * Used by {@link QueryHelper#matchesQuery}. Queries with a non-empty collation are compiled
     * fresh (uncached) - collation is a second dynamic axis on top of the query map identity, and
     * caching by query-identity alone would silently reuse the wrong collation for a differently
     * collated call using the same query map instance.
     */
    static boolean matchesQueryCached(Map<String, Object> query, Map<String, Object> toCheck, Map<String, Object> collation) {
        if (collation != null && !collation.isEmpty()) {
            return compile(query, collation).matches(toCheck);
        }

        IdentityKey key = new IdentityKey(query);
        CompiledQuery cached = CACHE.get(key);

        if (cached != null) {
            CACHE_HITS.incrementAndGet();
            return cached.matches(toCheck);
        }

        CACHE_MISSES.incrementAndGet();
        CompiledQuery compiled = compile(query);
        CACHE.put(key, compiled);
        return compiled.matches(toCheck);
    }

    // ------------------------------------------------------------------
    // Compile-time context: the collation map is resolved to two Collator variants once, because
    // the interpreter itself is inconsistent about when it treats a non-null-but-empty collation
    // map as "no collation" (checks collation.isEmpty()) vs. "use default ROOT collation" (does
    // not check isEmpty()) - see EqLiteralNode below. Both are cached here rather than re-derived
    // per node evaluation.
    // ------------------------------------------------------------------
    static final class Ctx {
        final Map<String, Object> collationMap;
        /** Collator honoring the {@code collation != null && !collation.isEmpty()} interpreter check. */
        final Collator collChecked;
        /** Collator honoring only the {@code collation != null} interpreter check (no isEmpty()). */
        final Collator collUnchecked;

        Ctx(Map<String, Object> collationMap) {
            this.collationMap = collationMap;
            this.collChecked = (collationMap != null && !collationMap.isEmpty()) ? QueryHelper.getCollator(collationMap) : null;
            this.collUnchecked = (collationMap != null) ? QueryHelper.getCollator(collationMap) : null;
        }
    }

    // ------------------------------------------------------------------
    // Node model
    // ------------------------------------------------------------------
    interface Node {
        boolean test(Map<String, Object> doc);
    }

    private static final class TrueNode implements Node {
        static final TrueNode INSTANCE = new TrueNode();
        @Override
        public boolean test(Map<String, Object> doc) {
            return true;
        }
    }

    private enum Kind { HARD, WHERE, NORMAL }

    private static final class KeyedNode {
        final Kind kind;
        final Node node;
        KeyedNode(Kind kind, Node node) {
            this.kind = kind;
            this.node = node;
        }
    }

    /**
     * Mirrors the top-level loop of {@code matchesQueryInterpreted}: $and/$or/$nor/$not/$expr
     * return immediately the moment they are evaluated (later keys, in map iteration order, are
     * never reached); a normal field/$jsonSchema/$textSearch condition ANDs (false short-circuits
     * immediately, success sets ret=true and continues); $where is the one operator that does
     * NEITHER - it just overwrites `ret` and continues, so a failing $where can be silently undone
     * by a later successful key, and a passing $where can be silently undone by a later failing
     * key. That is almost certainly an interpreter bug, but per the correctness gate we replicate
     * it exactly rather than "fixing" it.
     *
     * KNOWN-DIVERGENCE: the WHERE kind's overwrite-and-continue behavior above (a $where result can
     * be silently undone by a later key, in either direction) diverges from real MongoDB, which
     * ANDs all top-level predicates. Preserved to match the interpreter.
     */
    private static final class SequenceNode implements Node {
        final Kind[] kinds;
        final Node[] nodes;

        SequenceNode(List<KeyedNode> keyed) {
            kinds = new Kind[keyed.size()];
            nodes = new Node[keyed.size()];
            for (int i = 0; i < keyed.size(); i++) {
                kinds[i] = keyed.get(i).kind;
                nodes[i] = keyed.get(i).node;
            }
        }

        @Override
        public boolean test(Map<String, Object> doc) {
            boolean ret = false;

            for (int i = 0; i < nodes.length; i++) {
                switch (kinds[i]) {
                    case HARD:
                        return nodes[i].test(doc);

                    case WHERE:
                        ret = nodes[i].test(doc);
                        break;

                    case NORMAL:
                        if (!nodes[i].test(doc)) {
                            return false;
                        }
                        ret = true;
                        break;
                }
            }

            return ret;
        }
    }

    private static Node compileQueryNode(Map<String, Object> query, Ctx ctx) {
        if (query.isEmpty()) {
            return TrueNode.INSTANCE;
        }

        List<KeyedNode> keyed = new ArrayList<>(query.size());

        for (String key : query.keySet()) {
            keyed.add(compileTopLevelKey(key, query, ctx));
        }

        if (keyed.size() == 1) {
            return keyed.get(0).node;
        }

        return new SequenceNode(keyed);
    }

    @SuppressWarnings("unchecked")
    private static KeyedNode compileTopLevelKey(String key, Map<String, Object> query, Ctx ctx) {
        Object value = query.get(key);

        switch (key) {
            case "$and": {
                List<Node> children = compileList((List<Map<String, Object>>) value, ctx);
                return new KeyedNode(Kind.HARD, doc -> {
                    for (Node c : children) {
                        if (!c.test(doc)) {
                            return false;
                        }
                    }
                    return true;
                });
            }

            case "$or": {
                List<Node> children = compileList((List<Map<String, Object>>) value, ctx);
                return new KeyedNode(Kind.HARD, doc -> {
                    for (Node c : children) {
                        if (c.test(doc)) {
                            return true;
                        }
                    }
                    return false;
                });
            }

            case "$nor": {
                List<Node> children = compileList((List<Map<String, Object>>) value, ctx);
                return new KeyedNode(Kind.HARD, doc -> {
                    for (Node c : children) {
                        if (c.test(doc)) {
                            return false;
                        }
                    }
                    return true;
                });
            }

            case "$not": {
                Node inner = compileQueryNode((Map<String, Object>) value, ctx);
                return new KeyedNode(Kind.HARD, doc -> !inner.test(doc));
            }

            case "$expr": {
                Expr expr = Expr.parse(value);
                return new KeyedNode(Kind.HARD, doc -> {
                    Object result = expr.evaluate(doc);
                    if (result instanceof Expr) {
                        result = ((Expr) result).evaluate(doc);
                    }
                    return Boolean.TRUE.equals(result);
                });
            }

            case "$jsonSchema": {
                if (!(value instanceof Map)) {
                    return new KeyedNode(Kind.NORMAL, doc -> false);
                }
                Map<String, Object> schema = (Map<String, Object>) value;
                return new KeyedNode(Kind.NORMAL, doc -> QueryHelper.matchesJsonSchema(schema, doc));
            }

            case "$where": {
                // The interpreter reads query.get("$where") from the enclosing query map at match
                // time - the raw script string is static per compiled query, so capture it once.
                return new KeyedNode(Kind.WHERE, doc -> QueryHelper.runWhere(query, doc));
            }

            case "$textSearch": {
                return new KeyedNode(Kind.NORMAL, doc -> QueryHelper.matchesTextSearch(value, doc));
            }

            default:
                if (key.startsWith("$") && !QueryHelper.isKnownOperator(key)) {
                    throw new IllegalArgumentException("unknown top level operator: " + key);
                }

                return new KeyedNode(Kind.NORMAL, compileFieldKey(key, query, ctx));
        }
    }

    private static List<Node> compileList(List<Map<String, Object>> list, Ctx ctx) {
        List<Node> nodes = new ArrayList<>(list.size());
        for (Map<String, Object> q : list) {
            nodes.add(compileQueryNode(q, ctx));
        }
        return nodes;
    }

    // ------------------------------------------------------------------
    // Field-level compilation: mirrors the "map field query / array index query" pre-pass
    // (matchesQueryInterpreted lines ~151-245) plus, when not consumed by it, matchesFieldCondition.
    // ------------------------------------------------------------------
    @SuppressWarnings("unchecked")
    private static Node compileFieldKey(String key, Map<String, Object> query, Ctx ctx) {
        Object value = query.get(key);
        Node standard = compileStandardFieldCondition(key, query, ctx);

        if (!(value instanceof Map)) {
            return standard;
        }

        Map<String, Object> queryMap = (Map<String, Object>) value;
        // Precompiled entries mirror queryMap's own iteration order exactly (query maps are
        // treated as immutable for the lifetime of a compiled query, matching the identity-cache
        // assumption elsewhere in this class).
        List<Map.Entry<String, Object>> entries = new ArrayList<>(queryMap.entrySet());

        List<MapFieldEntry> mapFieldEntries = new ArrayList<>(entries.size());
        for (Map.Entry<String, Object> e : entries) {
            mapFieldEntries.add(new MapFieldEntry(e.getKey(), e.getValue()));
        }

        List<ArrayIndexEntry> arrayIndexEntries = new ArrayList<>(entries.size());
        for (Map.Entry<String, Object> e : entries) {
            arrayIndexEntries.add(compileArrayIndexEntry(e.getKey(), e.getValue(), ctx));
        }

        return doc -> {
            Object docVal = doc.get(key);

            if (docVal instanceof Map) {
                if (matchesMapFieldQuery((Map<String, Object>) docVal, mapFieldEntries)) {
                    return true;
                }
                // not consumed -> fall through to standard handling
            } else if (docVal instanceof List) {
                if (matchesArrayIndexQuery((List<?>) docVal, arrayIndexEntries)) {
                    return true;
                }
                // not consumed -> fall through
            }

            return standard.test(doc);
        };
    }

    private static final class MapFieldEntry {
        final String mapKey;
        final Object expected;
        MapFieldEntry(String mapKey, Object expected) {
            this.mapKey = mapKey;
            this.expected = expected;
        }
    }

    private static boolean matchesMapFieldQuery(Map<String, Object> checkMap, List<MapFieldEntry> entries) {
        for (MapFieldEntry e : entries) {
            // Note: the interpreter compares with a null Collator here regardless of the outer
            // collation - preserved exactly.
            if (!checkMap.containsKey(e.mapKey) || !QueryHelper.compareValues(checkMap.get(e.mapKey), e.expected, null)) {
                return false;
            }
        }
        return true;
    }

    private static final class ArrayIndexEntry {
        final Integer idx; // null if mapKey doesn't parse as an int -> always fails
        final boolean isExists;
        final boolean expectExists;
        final Node direct;   // compiled indexQuery, used when arrayElement instanceof Map
        final Node wrapped;  // compiled {value: indexQuery}, used otherwise

        ArrayIndexEntry(Integer idx, boolean isExists, boolean expectExists, Node direct, Node wrapped) {
            this.idx = idx;
            this.isExists = isExists;
            this.expectExists = expectExists;
            this.direct = direct;
            this.wrapped = wrapped;
        }
    }

    @SuppressWarnings("unchecked")
    private static ArrayIndexEntry compileArrayIndexEntry(String mapKey, Object indexQuery, Ctx ctx) {
        Integer idx;
        try {
            idx = Integer.parseInt(mapKey);
        } catch (NumberFormatException e) {
            return new ArrayIndexEntry(null, false, false, null, null);
        }

        if (indexQuery instanceof Map && ((Map<?, ?>) indexQuery).containsKey("$exists")) {
            Object existsValue = ((Map<?, ?>) indexQuery).get("$exists");
            boolean expectExists = Boolean.TRUE.equals(existsValue) || "true".equals(existsValue) || Integer.valueOf(1).equals(existsValue);
            return new ArrayIndexEntry(idx, true, expectExists, null, null);
        }

        Node direct = (indexQuery instanceof Map)
        ? compileQueryNode((Map<String, Object>) indexQuery, ctx)
        : null;
        Node wrapped = compileQueryNode(Doc.of("value", indexQuery), ctx);
        return new ArrayIndexEntry(idx, false, false, direct, wrapped);
    }

    private static boolean matchesArrayIndexQuery(List<?> checkList, List<ArrayIndexEntry> entries) {
        for (ArrayIndexEntry e : entries) {
            if (e.idx == null) {
                return false;
            }

            int idx = e.idx;
            boolean outOfBounds = idx < 0 || idx >= checkList.size();

            if (e.isExists) {
                if (outOfBounds) {
                    if (e.expectExists) {
                        return false;
                    }
                    // exists:false on out-of-bounds index -> this entry passes, continue
                    continue;
                } else {
                    if (e.expectExists) {
                        continue;
                    }
                    return false;
                }
            }

            if (outOfBounds) {
                return false;
            }

            Object arrayElement = checkList.get(idx);

            if (arrayElement instanceof Map) {
                if (e.direct == null || !e.direct.test((Map<String, Object>) arrayElement)) {
                    return false;
                }
            } else {
                Map<String, Object> syntheticDoc = Doc.of("value", arrayElement);
                if (!e.wrapped.test(syntheticDoc)) {
                    return false;
                }
            }
        }
        return true;
    }

    // ------------------------------------------------------------------
    // Standard field condition (matchesFieldCondition equivalent)
    // ------------------------------------------------------------------
    private static final Set<String> HAND_COMPILED_OPS = Set.of(
        "$eq", "$ne", "$gt", "$gte", "$lt", "$lte", "$in", "$nin", "$exists",
        "$mod", "$size", "$all", "$type", "$regex", "$regularExpression", "$not", "$elemMatch", "$expr"
    );

    @SuppressWarnings("unchecked")
    private static Node compileStandardFieldCondition(String key, Map<String, Object> query, Ctx ctx) {
        Object value = query.get(key);

        if (!(value instanceof Map)) {
            return compileEqLiteral(key, query, ctx);
        }

        Map<String, Object> commandMap = (Map<String, Object>) value;
        List<String> operators = new ArrayList<>();
        for (String opKey : commandMap.keySet()) {
            if (!"$options".equals(opKey)) {
                operators.add(opKey);
            }
        }

        if (operators.isEmpty()) {
            // e.g. {field: {$options: "i"}} alone - the interpreter's iterator loop would then
            // pick "$options" itself as commandKey (its while-skip only triggers if there IS a
            // next() after it), landing on the "$options: break;" -> "return true" fallthrough.
            return doc -> true;
        }

        boolean allHandCompiled = true;
        for (String op : operators) {
            if (!HAND_COMPILED_OPS.contains(op)) {
                allHandCompiled = false;
                break;
            }
        }

        if (!allHandCompiled) {
            // Rare/exotic operator(s) on this field (geo*, bits*, $text, $jsonSchema, $where,
            // $comment, or an unknown-but-KNOWN_OPERATORS entry) - delegate the WHOLE field
            // condition to the interpreter's own field-level method. This is byte-for-byte
            // identical behavior by construction, including its own multi-operator splitting.
            return doc -> QueryHelper.matchesFieldCondition(key, query, doc, ctx.collationMap);
        }

        if (operators.size() == 1) {
            return compileSingleOp(key, operators.get(0), commandMap, ctx);
        }

        // KNOWN-DIVERGENCE: for a field with MULTIPLE operators (e.g. {a: {$gt:1, $lt:5}}), the
        // interpreter re-enters the top-level matchesQuery loop (via a synthetic 1-key sub-query)
        // for EACH operator - which means the "map field query / array index query" pre-pass at
        // the top of matchesQueryInterpreted also re-runs for each operator, using the SAME
        // top-level document field. That pre-pass can only ever succeed if the document field is
        // itself a Map/List whose keys/indices happen to collide with operator names like "$gt"
        // (impossible for ordinary BSON documents - "$"-prefixed field names are not valid Mongo
        // field names). We do not replicate that re-entrant pre-pass here; multi-operator fields
        // go straight to hand-compiled per-operator evaluation, ANDed together. This only differs
        // from the interpreter for the deliberately pathological case of a document field literally
        // containing "$"-prefixed keys/values that would coincidentally satisfy that pre-pass.
        List<Node> opNodes = new ArrayList<>(operators.size());
        for (String op : operators) {
            opNodes.add(compileSingleOp(key, op, commandMap, ctx));
        }
        return doc -> {
            for (Node n : opNodes) {
                if (!n.test(doc)) {
                    return false;
                }
            }
            return true;
        };
    }

    // ---- checkValue resolution (mirrors matchesFieldCondition lines ~450-522) ----
    @SuppressWarnings("unchecked")
    private static Object resolveOperatorCheckValue(String[] pth, Map<String, Object> toCheck) {
        Object checkValue;

        if (pth.length >= 2 && toCheck.get(pth[0]) instanceof Map) {
            Map<String, Object> mapField = (Map<String, Object>) toCheck.get(pth[0]);
            if (pth.length == 2 && mapField.containsKey(pth[1])) {
                return mapField.get(pth[1]);
            }
            checkValue = toCheck;
        } else {
            checkValue = toCheck;
        }

        for (String p : pth) {
            if (checkValue == null) {
                break;
            }
            if (checkValue instanceof Map) {
                checkValue = ((Map<String, Object>) checkValue).get(p);
            } else if (checkValue instanceof List) {
                try {
                    int idx = Integer.parseInt(p);
                    checkValue = ((List<Object>) checkValue).get(idx);
                } catch (Exception e) {
                    List<Object> lst = new ArrayList<>();
                    for (Object o : (List<Object>) checkValue) {
                        if (o instanceof Map) {
                            lst.add(((Map<String, Object>) o).get(p));
                        }
                    }
                    checkValue = lst;
                }
            }
            // else: neither Map nor List - checkValue is left unchanged for remaining path
            // segments, matching the interpreter (a quirk, not "fixed" here).
        }

        return checkValue;
    }

    private static Object resolveCheckValue(String key, String[] path, Map<String, Object> doc) {
        if (path == null) {
            return doc.get(key);
        }
        return resolveOperatorCheckValue(path, doc);
    }

    @SuppressWarnings("unchecked")
    private static Node compileSingleOp(String key, String op, Map<String, Object> commandMap, Ctx ctx) {
        Object operand = commandMap.get(op);
        String[] path = key.contains(".") ? key.split("\\.") : null;
        Collator coll = ctx.collChecked;

        switch (op) {
            case "$eq": {
                return doc -> {
                    Object checkValue = resolveCheckValue(key, path, doc);
                    if (checkValue == null && operand == null) {
                        return true;
                    }
                    if (checkValue == null || operand == null) {
                        return false;
                    }
                    if (checkValue instanceof List) {
                        for (Object v : (List<Object>) checkValue) {
                            if (QueryHelper.compareValues(v, operand, coll)) {
                                return true;
                            }
                        }
                        return false;
                    }
                    return QueryHelper.compareValues(checkValue, operand, coll);
                };
            }

            case "$ne": {
                return doc -> {
                    Object checkValue = resolveCheckValue(key, path, doc);
                    if (checkValue instanceof List) {
                        for (Object v : (List<Object>) checkValue) {
                            if (QueryHelper.compareValues(v, operand, coll)) {
                                return false;
                            }
                        }
                        return true;
                    }
                    if (checkValue == null && operand == null) {
                        return false;
                    }
                    return !QueryHelper.compareValues(checkValue, operand, coll);
                };
            }

            case "$lte":
            case "$lt": {
                int offset = "$lt".equals(op) ? -1 : 0;
                return doc -> {
                    Object checkValue = resolveCheckValue(key, path, doc);
                    List<Object> lst;
                    if (checkValue instanceof List) {
                        lst = (List<Object>) checkValue;
                    } else if (checkValue != null) {
                        lst = List.of(checkValue);
                    } else {
                        return false;
                    }
                    for (Object cv : lst) {
                        if (cv == null) {
                            continue;
                        }
                        if (QueryHelper.compareLessThan(cv, operand, offset, coll)) {
                            return true;
                        }
                    }
                    return false;
                };
            }

            case "$gte":
            case "$gt": {
                int offset = "$gt".equals(op) ? 1 : 0;
                return doc -> {
                    Object checkValue = resolveCheckValue(key, path, doc);
                    List<Object> lst;
                    if (checkValue instanceof List) {
                        lst = (List<Object>) checkValue;
                    } else if (checkValue != null) {
                        lst = List.of(checkValue);
                    } else {
                        return false;
                    }
                    for (Object cv : lst) {
                        if (cv == null) {
                            continue;
                        }
                        if (QueryHelper.compareGreaterThan(cv, operand, offset, coll)) {
                            return true;
                        }
                    }
                    return false;
                };
            }

            case "$mod": {
                List<?> arr = (List<?>) operand;
                int div = ((Number) arr.get(0)).intValue();
                int rem = ((Number) arr.get(1)).intValue();
                return doc -> {
                    Number n = (Number) resolveCheckValue(key, path, doc);
                    return n.intValue() % div == rem;
                };
            }

            case "$exists": {
                boolean expectExists = Boolean.TRUE.equals(operand) || "true".equals(operand) || Integer.valueOf(1).equals(operand);
                boolean dotted = key.contains(".");
                String camelCase = dotted ? null : QueryHelper.convertSnakeToCamelCase(key);
                String snakeCase = dotted ? null : QueryHelper.convertCamelToSnakeCase(key);
                return doc -> {
                    boolean exists;
                    if (dotted) {
                        QueryHelper.LookupResult lookup = QueryHelper.resolveValuesForPath(doc, path, 0);
                        exists = lookup.pathExists;
                    } else {
                        exists = QueryHelper.fieldExists(doc, key);
                        if (!exists && key.contains("_")) {
                            exists = QueryHelper.fieldExists(doc, camelCase);
                        }
                        if (!exists && !key.contains("_")) {
                            exists = QueryHelper.fieldExists(doc, snakeCase);
                        }
                    }
                    return expectExists ? exists : !exists;
                };
            }

            case "$nin": {
                List<?> ninList = QueryHelper.asValueList(operand);
                Set<Object> ninSet = new HashSet<>(ninList.size());
                for (Object v : ninList) {
                    ninSet.add(QueryHelper.normalizeId(v));
                }
                return doc -> {
                    Object checkValue = resolveCheckValue(key, path, doc);
                    boolean found;
                    if (coll != null) {
                        found = false;
                        for (Object v : ninList) {
                            Object normalized = QueryHelper.normalizeId(v);
                            if (checkValue instanceof List) {
                                for (Object element : (List<Object>) checkValue) {
                                    if (QueryHelper.compareValues(element, normalized, coll)) {
                                        found = true;
                                        break;
                                    }
                                }
                            } else if (QueryHelper.compareValues(checkValue, normalized, coll)) {
                                found = true;
                            }
                            if (found) break;
                        }
                    } else if (checkValue instanceof List) {
                        found = false;
                        for (Object element : (List<Object>) checkValue) {
                            if (ninSet.contains(QueryHelper.normalizeId(element))) {
                                found = true;
                                break;
                            }
                        }
                    } else {
                        found = ninSet.contains(QueryHelper.normalizeId(checkValue));
                    }
                    return !found;
                };
            }

            case "$in": {
                List<?> inList = QueryHelper.asValueList(operand);
                Set<Object> inSet = new HashSet<>(inList.size());
                for (Object v : inList) {
                    inSet.add(QueryHelper.normalizeId(v));
                }
                return doc -> {
                    Object checkValue = resolveCheckValue(key, path, doc);
                    if (coll != null) {
                        for (Object v : inList) {
                            Object normalized = QueryHelper.normalizeId(v);
                            if (checkValue instanceof List) {
                                for (Object element : (List<Object>) checkValue) {
                                    if (QueryHelper.compareValues(element, normalized, coll)) {
                                        return true;
                                    }
                                }
                            } else if (QueryHelper.compareValues(checkValue, normalized, coll)) {
                                return true;
                            }
                        }
                        return false;
                    }
                    if (checkValue instanceof List) {
                        for (Object element : (List<Object>) checkValue) {
                            if (inSet.contains(QueryHelper.normalizeId(element))) {
                                return true;
                            }
                        }
                        return false;
                    }
                    return inSet.contains(QueryHelper.normalizeId(checkValue));
                };
            }

            case "$expr": {
                // Field-level $expr assumes the operand is ALREADY a parsed Expr instance (the
                // interpreter casts directly, no Expr.parse call) - preserved as-is.
                Expr e = (Expr) operand;
                return doc -> {
                    Object ev = e.evaluate(doc);
                    return ev != null && (ev.equals(Boolean.TRUE) || ev.equals(1) || ev.equals("true"));
                };
            }

            case "$not": {
                Map<String, Object> notInner = (Map<String, Object>) operand;
                CompiledQuery notCompiled = compile(Doc.of(key, notInner), ctx.collationMap);
                return doc -> !notCompiled.matches(doc);
            }

            case "$regex":
            case "$regularExpression": {
                // buildRegexPattern itself reads $options straight out of commandMap for the
                // $regularExpression map form; the interpreter's own base-flags computation (from
                // commandMap's $options, for the plain $regex string form) happens one level up in
                // matchesFieldCondition and is passed in as baseFlags - replicate that here.
                int opts = 0;
                if (commandMap.containsKey("$options")) {
                    opts = QueryHelper.applyRegexOptions(commandMap.get("$options").toString().toLowerCase(java.util.Locale.ROOT), 0);
                }
                Pattern pattern = QueryHelper.buildRegexPattern(commandMap, opts);
                return doc -> {
                    Object valtoCheck = resolveRegexCheckValue(key, path, doc);
                    if (valtoCheck == null || pattern == null) {
                        return false;
                    }
                    if (valtoCheck instanceof List) {
                        for (Object element : (List<Object>) valtoCheck) {
                            if (element != null && pattern.matcher(element.toString()).find()) {
                                return true;
                            }
                        }
                        return false;
                    }
                    return pattern.matcher(valtoCheck.toString()).find();
                };
            }

            case "$type": {
                de.caluga.morphium.MongoType type;
                if (operand instanceof Number) {
                    type = de.caluga.morphium.MongoType.findByValue(((Number) operand).intValue());
                } else if (operand instanceof String) {
                    type = de.caluga.morphium.MongoType.findByTxt((String) operand);
                } else {
                    type = null; // interpreter logs an error and returns false
                }
                de.caluga.morphium.MongoType finalType = type;
                return doc -> {
                    if (finalType == null) {
                        return false;
                    }
                    Object checkValue = resolveCheckValue(key, path, doc);
                    List<Object> elements = new ArrayList<>();
                    if (checkValue instanceof List) {
                        elements.addAll((List<Object>) checkValue);
                    } else {
                        elements.add(checkValue);
                    }
                    for (Object o : elements) {
                        if (QueryHelper.matchesType(o, finalType)) {
                            return true;
                        }
                    }
                    return false;
                };
            }

            case "$all": {
                List<?> queryValues = (operand instanceof List) ? (List<?>) operand : null;
                return doc -> {
                    if (queryValues == null) {
                        return false;
                    }
                    Object checkValue = resolveCheckValue(key, path, doc);
                    if (checkValue == null || !(checkValue instanceof List)) {
                        return false;
                    }
                    Set<Object> checkSet = new HashSet<>((List<Object>) checkValue);
                    for (Object o : queryValues) {
                        if (!checkSet.contains(o)) {
                            return false;
                        }
                    }
                    return true;
                };
            }

            case "$size": {
                return doc -> {
                    Object checkValue = resolveCheckValue(key, path, doc);
                    if (checkValue == null) {
                        return operand.equals(0);
                    }
                    if (!(checkValue instanceof List)) {
                        return false;
                    }
                    return operand.equals(((List<?>) checkValue).size());
                };
            }

            case "$elemMatch": {
                if (!(operand instanceof Map)) {
                    return doc -> false;
                }
                Map<String, Object> queryMap = (Map<String, Object>) operand;
                // The interpreter tries BOTH interpretations with null collation regardless of
                // the outer collation - preserved exactly.
                CompiledQuery direct = compile(queryMap, null);
                CompiledQuery wrapped = compile(Doc.of("value", queryMap), null);
                return doc -> {
                    Object checkValue = resolveCheckValue(key, path, doc);
                    if (checkValue == null || !(checkValue instanceof List)) {
                        return false;
                    }
                    for (Object o : (List<Object>) checkValue) {
                        Map<String, Object> el = (o instanceof Map) ? (Map<String, Object>) o : Doc.of("value", o);
                        if (direct.matches(el) || wrapped.matches(el)) {
                            return true;
                        }
                    }
                    return false;
                };
            }

            default:
                throw new IllegalStateException("unreachable: " + op);
        }
    }

    /** Mirrors matchesFieldCondition's own (simpler, Map-only) dotted-path walk used by $regex. */
    @SuppressWarnings("unchecked")
    private static Object resolveRegexCheckValue(String key, String[] path, Map<String, Object> doc) {
        if (path == null) {
            return doc.get(key);
        }
        Object val = doc;
        Object result = null;
        for (int i = 0; i < path.length; i++) {
            if (!(val instanceof Map)) {
                return null;
            }
            Object candidate = ((Map<String, Object>) val).get(path[i]);
            if (candidate == null) {
                return null;
            }
            if (!(candidate instanceof Map) && i < path.length - 1) {
                return null;
            } else if (i < path.length - 1) {
                val = candidate;
            }
            if (i == path.length - 1) {
                result = candidate;
            }
        }
        return result;
    }

    // ------------------------------------------------------------------
    // Implicit / literal equality (matchesFieldCondition else-branch, ~lines 1193-1282)
    // ------------------------------------------------------------------
    @SuppressWarnings("unchecked")
    private static Node compileEqLiteral(String key, Map<String, Object> query, Ctx ctx) {
        Object expected = query.get(key);

        if (key.contains(".")) {
            String[] path = key.split("\\.");
            Collator coll = ctx.collChecked; // dotted branch DOES check collation.isEmpty()
            return doc -> {
                QueryHelper.LookupResult lookup = QueryHelper.resolveValuesForPath(doc, path, 0);
                if (!lookup.pathExists) {
                    return expected == null;
                }
                if (expected == null) {
                    if (lookup.values.isEmpty()) {
                        return true;
                    }
                    for (Object candidate : lookup.values) {
                        if (candidate == null) {
                            return true;
                        }
                    }
                    return false;
                }
                for (Object candidate : lookup.values) {
                    if (candidate instanceof List) {
                        for (Object element : (List<Object>) candidate) {
                            if (QueryHelper.compareValues(element, expected, coll)) {
                                return true;
                            }
                        }
                        continue;
                    }
                    if (QueryHelper.compareValues(candidate, expected, coll)) {
                        return true;
                    }
                }
                return false;
            };
        }

        // KNOWN-DIVERGENCE(interpreter quirk, preserved not fixed): these two non-dotted branches
        // check `collation != null` WITHOUT the `!collation.isEmpty()` guard the other branches
        // use, so a non-null-but-EMPTY collation map still resolves a real (ROOT-locale) Collator
        // here while it resolves to "no collation" everywhere else in matchesFieldCondition.
        Collator collUnchecked = ctx.collUnchecked;
        Map<String, Object> collationMap = ctx.collationMap;

        return doc -> {
            Object docValue = doc.get(key);

            if (docValue == null && expected == null) {
                return true;
            }
            if (docValue == null) {
                return false;
            }

            if (docValue instanceof MorphiumId || docValue instanceof ObjectId) {
                if (expected == null) {
                    return false;
                }
                return docValue.toString().equals(expected.toString());
            }

            if (docValue instanceof List) {
                List<?> lst = (List<?>) docValue;
                if (collationMap != null && expected instanceof String) {
                    if (collUnchecked != null) {
                        for (Object v2 : lst) {
                            if (v2 instanceof String && collUnchecked.equals((String) v2, (String) expected)) {
                                return true;
                            }
                        }
                        return false;
                    }
                }
                return lst.contains(expected);
            }

            if (collationMap != null && docValue instanceof String && expected instanceof String) {
                if (collUnchecked != null) {
                    return collUnchecked.equals((String) docValue, (String) expected);
                }
            }

            return docValue.equals(expected);
        };
    }
}
