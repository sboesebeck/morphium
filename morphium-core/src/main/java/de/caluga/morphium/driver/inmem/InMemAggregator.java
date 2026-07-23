package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.*;
import de.caluga.morphium.aggregation.*;
import de.caluga.morphium.aggregation.internal.FieldNameTranslation;
import de.caluga.morphium.aggregation.internal.UntranslatedRefWarner;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.SingleBatchCursor;
import de.caluga.morphium.driver.commands.AggregateMongoCommand;
import de.caluga.morphium.driver.commands.ExplainCommand.ExplainVerbosity;
import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

@SuppressWarnings("CommentedOutCode")
public class InMemAggregator<T, R> implements Aggregator<T, R> {
    private final Logger log = LoggerFactory.getLogger(InMemAggregator.class);
    private final List<Map<String, Object>> params = new ArrayList<>();
    private final List<Group<T, R>> groups = new ArrayList<>();
    private Class <? extends T > type;
    private Morphium morphium;
    private Class <? extends R > rType;
    private String collectionName;
    private boolean useDisk = false;
    private boolean explain = false;
    private Collation collation;
    private final UntranslatedRefWarner refWarner = new UntranslatedRefWarner();
    private final FieldNameTranslation fieldNames;

    public InMemAggregator(Morphium morphium, Class <? extends T > type, Class <? extends R > resultType) {
        this.morphium = morphium;
        setSearchType(type);
        setResultType(resultType);
        fieldNames = new FieldNameTranslation(morphium, type);
    }

    private String tf(String field) {
        return fieldNames.tf(field);
    }

    @Override
    public Aggregator<T, R> setTranslateAggregationFieldNames(Boolean translate) {
        fieldNames.setOverride(translate);
        return this;
    }

    @Override
    public boolean isTranslateAggregationFieldNames() {
        return fieldNames.isEnabled();
    }

    @Override
    public Collation getCollation() {
        return collation;
    }

    @Override
    public boolean isUseDisk() {
        return useDisk;
    }

    @Override
    public void setUseDisk(boolean useDisk) {
        this.useDisk = useDisk;
    }

    @Override
    public AggregateMongoCommand getAggregateCmd() {
        return new AggregateMongoCommand(null) {
            @Override
            public List<Map<String, Object>> execute() throws MorphiumDriverException {
                return aggregateMap();
            }
            @Override
            public MorphiumCursor executeIterable(int batchsize) throws MorphiumDriverException {
                return new SingleBatchCursor(aggregateMap());
            }
        };
    }

    @Override
    public boolean isExplain() {
        return explain;
    }

    @Override
    public void setExplain(boolean explain) {
        this.explain = explain;
    }

    @Override
    public Morphium getMorphium() {
        return morphium;
    }

    @Override
    public void setMorphium(Morphium m) {
        morphium = m;
    }

    @Override
    public Class <? extends T> getSearchType() {
        return type;
    }

    @Override
    public void setSearchType(Class <? extends T > type) {
        this.type = type;
    }

    @Override
    public Class <? extends R > getResultType() {
        return rType;
    }

    @Override
    public void setResultType(Class <? extends R > type) {
        rType = type;
    }

    @Override
    public Aggregator<T, R> project(Map<String, Object> m) {
        Map<String, Object> p = new LinkedHashMap<>();

        boolean translate = isTranslateAggregationFieldNames();

        for (Map.Entry<String, Object> e : m.entrySet()) {
            String key = tf(e.getKey());
            if (!key.equals(e.getKey())) {
                refWarner.recordProjectKeyTranslation(e.getKey(), key);
            }
            Object value = e.getValue();
            if (translate) {
                if (value instanceof Expr) {
                    value = Expr.parse(fieldNames.translateRefs(((Expr) value).toQueryObject()));
                } else {
                    value = fieldNames.translateRefs(value);
                }
            }
            p.put(key, value);
        }

        Map<String, Object> map = UtilsMap.of("$project", p);
        addOperator(map);
        return this;
    }

    @Override
    public Aggregator<T, R> project(String fld, Expr e) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(fld, e);
        return project(map);
    }

    @Override
    public Aggregator<T, R> project(String... m) {
        Map<String, Object> map = new LinkedHashMap<>();

        for (String sm : m) {
            map.put(sm, 1);
        }

        return project(map);
    }
    //
    //    @Override
    //    public Aggregator<T, R> project(Map<String, Object> m) {
    //        Map<String, Object> o = UtilsMap.of("$project", m);
    //        addOperator(o);
    //        return this;
    //    }

    @Override
    public Aggregator<T, R> addFields(Map<String, Object> m) {
        Map<String, Object> ret = new LinkedHashMap<>();
        boolean translate = isTranslateAggregationFieldNames();

        for (Map.Entry<String, Object> e : m.entrySet()) {
            Object value = e.getValue();
            if (value instanceof Expr) {
                if (translate) {
                    value = Expr.parse(fieldNames.translateRefs(((Expr) value).toQueryObject()));
                }
            } else {
                if (translate) {
                    value = fieldNames.translateRefs(value);
                }
                // Convert raw aggregation expressions to Expr objects
                value = Expr.parse(value);
            }

            ret.put(translate ? tf(e.getKey()) : e.getKey(), value);
        }

        Map<String, Object> o = UtilsMap.of("$addFields", ret);
        addOperator(o);
        return this;
    }

    @Override
    public Aggregator<T, R> match(Query<T> q) {
        Map<String, Object> o = UtilsMap.of("$match", q.toQueryObject());

        if (collectionName == null) {
            collectionName = q.getCollectionName();
        }

        addOperator(o);
        return this;
    }

    @Override
    public Aggregator<T, R> matchSubQuery(Query<?> q) {
        Map<String, Object> o = UtilsMap.of("$match", q.toQueryObject());
        addOperator(o);
        return this;
    }

    @Override
    public Aggregator<T, R> match(Expr q) {
        addOperator(UtilsMap.of("$match", UtilsMap.of("$expr", q)));
        return this;
    }

    @Override
    public Aggregator<T, R> limit(int num) {
        Map<String, Object> o = UtilsMap.of("$limit", num);
        addOperator(o);
        return this;
    }

    @Override
    public Aggregator<T, R> skip(int num) {
        Map<String, Object> o = UtilsMap.of("$skip", num);
        addOperator(o);
        return this;
    }

    @Override
    public Aggregator<T, R> unwind(String listField) {
        Map<String, Object> o = UtilsMap.of("$unwind", tf(listField));
        addOperator(o);
        return this;
    }

    public Aggregator<T, R> unwind(Expr field) {
        Map<String, Object> o = UtilsMap.of("$unwind", field);
        addOperator(o);
        return this;
    }

    @Override
    public Aggregator<T, R> sort(String... prefixed) {
        Map<String, Integer> m = new LinkedHashMap<>();

        for (String i : prefixed) {
            String fld = i;
            int val = 1;

            if (i.startsWith("-")) {
                fld = i.substring(1);
                val = -1;
            } else if (i.startsWith("+")) {
                fld = i.substring(1);
                val = 1;
            }

            if (i.startsWith("$")) {
                fld = fld.substring(1);

                if (!fld.contains(".")) {
                    fld = morphium.getARHelper().getMongoFieldName(type, fld);
                }
            }

            m.put(fld, val);
        }

        sort(m);
        return this;
    }

    @Override
    public Aggregator<T, R> sort(Map<String, Integer> sort) {
        Map<String, Integer> s = isTranslateAggregationFieldNames() ? fieldNames.translateKeys(sort) : sort;
        Map<String, Object> o = UtilsMap.of("$sort", s);
        addOperator(o);
        return this;
    }

    @Override
    public String getCollectionName() {
        if (collectionName == null) {
            collectionName = morphium.getMapper().getCollectionName(type);
        }

        return collectionName;
    }

    @Override
    public Aggregator<T, R> setCollectionName(String cn) {
        collectionName = cn;
        return this;
    }

    @Override
    public Group<T, R> group(Map<String, Object> id) {
        return new Group<>(this, id);
    }

    @Override
    public Group<T, R> group(Expr id) {
        return new Group<>(this, id);
    }

    @Override
    public Group<T, R> group(String id) {
        if (id != null && id.startsWith("$")) {
            id = "$" + tf(id.substring(1));
        }
        Group<T, R> gr = new Group<>(this, id);
        groups.add(gr);
        return gr;
    }

    /** single entry point for pipeline stages: every stage helper routes through here.
     * Invariant: any flag-based translation must happen BEFORE this call, so the
     * untranslated-reference check only sees the stage as it will be executed. */
    @Override
    public void addOperator(Map<String, Object> o) {
        refWarner.warnUntranslatedRefs(o, log);
        params.add(o);
    }

    @Override
    public List<R> aggregate() {
        return deserializeList();
    }

    @Override
    public long getCount() {
        var originalPipeline = new ArrayList<>(getPipeline());
        addOperator(UtilsMap.of("$count", "num"));
        List<Map<String, Object>> res = doAggregation();
        params.clear();
        params.addAll(originalPipeline);

        if (res.isEmpty()) {
            return 0;
        }

        if (res.get(0).get("num") instanceof Integer) {
            return ((Integer) res.get(0).get("num")).longValue();
        }

        return (Long) res.get(0).get("num");
    }

    @Override
    public MorphiumAggregationIterator<T, R> aggregateIterable() {
        AggregationIterator<T, R> agg = new AggregationIterator<>();
        agg.setAggregator(this);
        return agg;
    }

    @Override
    public void aggregate(final AsyncOperationCallback<R> callback) {
        if (callback == null) {
            morphium.queueTask(this::doAggregation);
        } else {
            morphium.queueTask(() -> {
                long start = System.currentTimeMillis();

                try {
                    List<R> result = deserializeList();
                    callback.onOperationSucceeded(AsyncOperationType.READ, null, System.currentTimeMillis() - start, result, null, InMemAggregator.this);
                } catch (Exception e) {
                    // Previously this logged and returned without ever calling the callback, so
                    // a caller waiting on onOperationSucceeded/onOperationError would hang forever
                    // on failure. Always call back - with the error - so the caller can react.
                    log.error("Aggregation failed", e);
                    callback.onOperationError(AsyncOperationType.READ, null, System.currentTimeMillis() - start, e.getMessage(), e, null, InMemAggregator.this);
                }
            });
        }
    }

    @SuppressWarnings({"RedundantThrows", "unchecked"})
    private List<R> deserializeList() throws MorphiumDriverException {
        List<Map<String, Object>> r = doAggregation();
        List<R> result = new ArrayList<>();

        if (getResultType().equals(Map.class)) {
            //noinspection unchecked
            result = (List<R>) r;
        } else {
            for (Map<String, Object> dbObj : r) {
                result.add(morphium.getMapper().deserialize(getResultType(), dbObj));
            }
        }

        return result;
    }

    @Override
    public void aggregateMap(AsyncOperationCallback<Map<String, Object>> callback) {
        if (callback == null) {
            // Fire-and-forget: still run off the caller's thread via the driver's executor.
            morphium.queueTask(this::aggregateMap);
            return;
        }

        morphium.queueTask(() -> {
            long start = System.currentTimeMillis();

            try {
                List<Map<String, Object>> result = aggregateMap();
                callback.onOperationSucceeded(AsyncOperationType.READ, null, System.currentTimeMillis() - start, result, null, InMemAggregator.this);
            } catch (Exception e) {
                // Must always call back - success or failure - never complete silently: a caller
                // blocking on the callback (e.g. via a CountDownLatch) must not hang forever
                // because a stage/expression error was swallowed here.
                log.error("Aggregation failed", e);
                callback.onOperationError(AsyncOperationType.READ, null, System.currentTimeMillis() - start, e.getMessage(), e, null, InMemAggregator.this);
            }
        });
    }

    @Override
    public List<Map<String, Object>> getPipeline() {
        for (Group<T, R> g : groups) {
            g.end();
        }

        groups.clear();
        return params;
    }

    @Override
    public Aggregator<T, R> count(String fld) {
        addOperator(UtilsMap.of("$count", fld));
        return this;
    }

    @Override
    public Aggregator<T, R> count(Enum fld) {
        return count(fld.name());
    }

    /**
     * Categorizes incoming documents into groups, called buckets, based on a
     * specified expression and
     * bucket boundaries and outputs a document per each bucket. Each output
     * document contains an _id field
     * whose value specifies the inclusive lower bound of the bucket. The output
     * option specifies
     * the fields included in each output document.
     * <p>
     * $bucket only produces output documents for buckets that contain at least one
     * input document.
     * wear)
     *
     * @param groupBy: Expression to group by, usually a field name
     * @param boundaries: Boundaries for the buckets
     * @param preset: the default, needs to be a literal
     * @param output: definition of output documents and accumulator
     * @return
     */
    @Override
    public Aggregator<T, R> bucket(Expr groupBy, List<Expr> boundaries, Expr preset, Map<String, Expr> output) {
        Map<String, Object> out = new LinkedHashMap<>();

        for (Map.Entry<String, Expr> e : output.entrySet()) {
            out.put(e.getKey(), e.getValue());
        }

        List<Object> bn = new ArrayList<>(boundaries);
        Map<String, Object> m = UtilsMap.of("$bucket", UtilsMap.of("groupBy", (Object) groupBy, "boundaries", bn, "default", preset, "output", Utils.getQueryObjectMap(output)));
        addOperator(m);
        return this;
    }

    @Override
    public Aggregator<T, R> bucketAuto(Expr groupBy, int numBuckets, Map<String, Expr> output, BucketGranularity granularity) {
        Map<String, Object> out = null;

        if (output != null) {
            out = new LinkedHashMap<>();

            for (Map.Entry<String, Expr> e : output.entrySet()) {
                out.put(e.getKey(), e.getValue());
            }
        }

        Map<String, Object> bucketAuto = UtilsMap.of("groupBy", groupBy);
        bucketAuto.put("buckets", numBuckets);
        Map<String, Object> map = UtilsMap.of("$bucketAuto", bucketAuto);

        if (out != null) {
            bucketAuto.put("output", out);
        }

        if (granularity != null) {
            bucketAuto.put("granularity", granularity.getValue());
        }

        addOperator(map);
        return this;
    }

    //$collStats:
    //    {
    //      latencyStats: { histograms: <boolean> },
    //      storageStats: { scale: <number> },
    //      count: {},
    //      queryExecStats: {}
    //    }
    @Override
    public Aggregator<T, R> collStats(Boolean latencyHistograms, Double scale, boolean count, boolean queryExecStats) {
        @SuppressWarnings("unchecked")
        Map<String, Object> m = new LinkedHashMap();

        if (latencyHistograms != null) {
            m.put("latencyStats", UtilsMap.of("histograms", latencyHistograms));
        }

        if (scale != null) {
            m.put("storageStats", UtilsMap.of("scale", scale));
        }

        if (count) {
            m.put("count", new HashMap<>());
        }

        if (queryExecStats) {
            m.put("queryExecStats", new HashMap<>());
        }

        addOperator(UtilsMap.of("$collStats", m));
        return this;
    }

    //{ $currentOp: { allUsers: <boolean>, idleConnections: <boolean>, idleCursors: <boolean>, idleSessions: <boolean>, localOps: <boolean> } }
    @Override
    public Aggregator<T, R> currentOp(boolean allUsers, boolean idleConnections, boolean idleCursors, boolean idleSessions, boolean localOps) {
        addOperator(UtilsMap.of("$currentOp", UtilsMap.of("allUsers", allUsers, "idleConnections", idleConnections, "idleCursors", idleCursors, "idleSessions", idleSessions, "localOps", localOps)));
        return this;
    }

    @Override
    public Aggregator<T, R> facetExpr(Map<String, Expr> facets) {
        Map<String, Object> map = Utils.getQueryObjectMap(facets);
        addOperator(UtilsMap.of("$facet", map));
        return this;
    }

    @Override
    public Aggregator<T, R> facet(Map<String, Aggregator> facets) {
        Map<String, Object> map = new HashMap<>();

        for (Map.Entry<String, Aggregator> e : facets.entrySet()) {
            map.put(e.getKey(), e.getValue().getPipeline());
        }

        addOperator(UtilsMap.of("$facet", map));
        return this;
    }

    @Override
    public Aggregator<T, R> geoNear(Map<GeoNearFields, Object> param) {
        Map<String, Object> map = new LinkedHashMap<>();

        for (Map.Entry<GeoNearFields, Object> e : param.entrySet()) {
            map.put(e.getKey().name(), ((ObjectMapperImpl) morphium.getMapper()).marshallIfNecessary(e.getValue()));
        }

        addOperator(UtilsMap.of("$geoNear", map));
        return this;
    }

    @Override
    public Aggregator<T, R> graphLookup(Class<?> fromType, Expr startWith, Enum connectFromField, Enum connectToField, String as, Integer maxDepth, String depthField, Query restrictSearchWithMatch) {
        return graphLookup(morphium.getMapper().getCollectionName(fromType), startWith, fieldNames.tf(fromType, connectFromField.name()), fieldNames.tf(fromType, connectToField.name()), as, maxDepth, depthField, restrictSearchWithMatch);
    }

    @Override
    public Aggregator<T, R> graphLookup(Class<?> fromType, Expr startWith, String connectFromField, String connectToField, String as, Integer maxDepth, String depthField, Query restrictSearchWithMatch) {
        boolean translate = isTranslateAggregationFieldNames();
        return graphLookup(morphium.getMapper().getCollectionName(fromType), startWith,
            translate ? fieldNames.tf(fromType, connectFromField) : connectFromField,
            translate ? fieldNames.tf(fromType, connectToField) : connectToField,
            as, maxDepth, depthField, restrictSearchWithMatch);
    }

    @Override
    public Aggregator<T, R> graphLookup(String fromCollection, Expr startWith, String connectFromField, String connectToField, String as, Integer maxDepth, String depthField,
                                        Query restrictSearchWithMatch) {
        Expr startWithExpr = isTranslateAggregationFieldNames()
            ? Expr.parse(fieldNames.translateRefs(startWith.toQueryObject())) : startWith;
        Map<String, Object> add = UtilsMap.of("from", (Object) fromCollection, "startWith", startWithExpr, "connectFromField", connectFromField, "connectToField", connectToField, "as", as);

        if (maxDepth != null) {
            add.put("maxDepth", maxDepth);
        }

        if (depthField != null) {
            add.put("depthField", depthField);
        }

        if (restrictSearchWithMatch != null) {
            add.put("restrictSearchWithMatch", restrictSearchWithMatch);
        }

        addOperator(UtilsMap.of("$graphLookup", add));
        return this;
    }

    @Override
    public Aggregator<T, R> indexStats() {
        addOperator(UtilsMap.of("$indexStats", new HashMap<>()));
        return this;
    }

    @Override
    public Aggregator<T, R> listLocalSessionsAllUsers() {
        addOperator(UtilsMap.of("$listLocalSessions", UtilsMap.of("allUsers", true)));
        return this;
    }

    @Override
    public Aggregator<T, R> listLocalSessions() {
        addOperator(UtilsMap.of("$listLocalSessions", new HashMap<>()));
        return this;
    }

    @Override
    public Aggregator<T, R> listLocalSessions(List<String> users, List<String> dbs) {
        List<Map<String, Object>> usersList = new ArrayList<>();

        for (int i = 0; i < users.size(); i++) {
            int j = i;

            if (j > dbs.size()) {
                j = dbs.size() - 1;
            }

            usersList.add(UtilsMap.of(users.get(i), dbs.get(j)));
        }

        addOperator(UtilsMap.of("$listLocalSessions", UtilsMap.of("users", usersList)));
        return this;
    }

    @Override
    public Aggregator<T, R> listSessionsAllUsers() {
        addOperator(UtilsMap.of("$listSessions", UtilsMap.of("allUsers", true)));
        return this;
    }

    @Override
    public Aggregator<T, R> listSessions() {
        addOperator(UtilsMap.of("$listSessions", new HashMap<>()));
        return this;
    }

    @Override
    public Aggregator<T, R> listSessions(List<String> users, List<String> dbs) {
        List<Map<String, Object>> usersList = new ArrayList<>();

        for (int i = 0; i < users.size(); i++) {
            int j = i;

            if (j > dbs.size()) {
                j = dbs.size() - 1;
            }

            usersList.add(UtilsMap.of(users.get(i), dbs.get(j)));
        }

        addOperator(UtilsMap.of("$listSessions", UtilsMap.of("users", usersList)));
        return this;
    }

    /**
     * $lookup:
     * {
     * from: &lt;collection to join&gt;,
     * localField: &lt;field from the input documents&gt;,
     * foreignField: &lt;field from the documents of the "from" collection&gt;,
     * as: &lt;output array field&gt;
     * }
     *
     * @return
     */

    public Aggregator<T, R> lookup(Class fromType, Enum localField, Enum foreignField, String outputArray, List<Expr> pipeline, Map<String, Expr> let) {
        return lookup(getMorphium().getMapper().getCollectionName(fromType), getMorphium().getARHelper().getMongoFieldName(getSearchType(), localField.name()),
                      getMorphium().getARHelper().getMongoFieldName(fromType, foreignField.name()), outputArray, pipeline, let);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Aggregator<T, R> lookup(String fromCollection, String localField, String foreignField, String outputArray, List<Expr> pipeline, Map<String, Expr> let) {
        Map<String, Object> m = new HashMap<>(UtilsMap.of("from", fromCollection));

        if (localField != null) {
            m.put("localField", tf(localField));
        }

        if (foreignField != null) {
            m.put("foreignField", foreignField);
        }

        if (outputArray != null) {
            m.put("as", outputArray);
        }

        if (pipeline != null && pipeline.size() > 0) {
            //noinspection unchecked
            List lst = new ArrayList(pipeline);
            m.put("pipeline", lst);
        }

        if (let != null) {
            Map<String, Object> map = new HashMap<>();

            for (Map.Entry<String, Expr> e : let.entrySet()) {
                map.put(e.getKey(), e.getValue());
            }

            m.put("let", map);
        }

        addOperator(UtilsMap.of("$lookup", m));
        return this;
    }

    @Override
    public Aggregator<T, R> merge(String intoDb, String intoCollection, MergeActionWhenMatched matchAction, MergeActionWhenNotMatched notMatchedAction, String... onFields) {
        return merge(intoDb, intoCollection, null, null, matchAction, notMatchedAction, onFields);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Aggregator<T, R> merge(String intoCollection, Map<String, Expr> let, List<Map<String, Expr >> machedPipeline, MergeActionWhenNotMatched notMatchedAction, String... onFields) {
        return merge(morphium.getConfig().connectionSettings().getDatabase(), intoCollection, let, machedPipeline, MergeActionWhenMatched.merge, notMatchedAction, onFields);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Aggregator<T, R> merge(Class<?> intoCollection, Map<String, Expr> let, List<Map<String, Expr >> machedPipeline, MergeActionWhenMatched matchAction,
                                  MergeActionWhenNotMatched notMatchedAction, String... onFields) {
        return merge(morphium.getConfig().connectionSettings().getDatabase(), morphium.getMapper().getCollectionName(intoCollection), let, machedPipeline, MergeActionWhenMatched.merge, notMatchedAction, onFields);
    }

    @Override
    public Aggregator<T, R> merge(String intoDb, String intoCollection) {
        return merge(intoDb, intoCollection, null, null, MergeActionWhenMatched.merge, MergeActionWhenNotMatched.insert);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Aggregator<T, R> merge(Class<?> intoCollection) {
        return merge(morphium.getConfig().connectionSettings().getDatabase(), morphium.getMapper().getCollectionName(intoCollection), null, null, MergeActionWhenMatched.merge, MergeActionWhenNotMatched.insert);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Aggregator<T, R> merge(String intoCollection) {
        return merge(morphium.getConfig().connectionSettings().getDatabase(), intoCollection, null, null, MergeActionWhenMatched.merge, MergeActionWhenNotMatched.insert);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Aggregator<T, R> merge(String intoCollection, MergeActionWhenMatched matchAction, MergeActionWhenNotMatched notMatchedAction, String... onFields) {
        return merge(morphium.getConfig().connectionSettings().getDatabase(), intoCollection, null, null, matchAction, notMatchedAction, onFields);
    }

    @SuppressWarnings({"ConstantConditions", "unchecked"})
    private Aggregator<T, R> merge(String intoDb, String intoCollection, Map<String, Expr> let, List<Map<String, Expr >> pipeline, MergeActionWhenMatched matchAction,
                                   MergeActionWhenNotMatched notMatchedAction, String... onFields) {
        Class entity = morphium.getMapper().getClassForCollectionName(intoCollection);
        List<String> flds = new ArrayList<>();

        if (entity != null) {
            for (String f : onFields) {
                flds.add(morphium.getARHelper().getMongoFieldName(entity, f));
            }
        } else {
            log.warn("no entity know for collection " + intoCollection);
            log.warn("cannot check field names / properties");
            flds.addAll(Arrays.asList(onFields));
        }

        //morphium.getARHelper().getClassForTypeId(intoCollection);
        Map doc = new HashMap(UtilsMap.of("into", UtilsMap.of("db", intoDb, "coll", intoCollection)));

        if (let != null) {
            //noinspection unchecked
            doc.put("let", Utils.getNoExprMap((Map) let));
        }

        if (matchAction != null) {
            //noinspection unchecked
            doc.put("whenMatched", matchAction.name());
        }

        if (notMatchedAction != null) {
            //noinspection unchecked
            doc.put("whenNotMatched", notMatchedAction.name());
        }

        if (onFields != null && onFields.length != 0) {
            //noinspection unchecked
            doc.put("on", flds);
        }

        if (pipeline != null) {
            //noinspection unchecked
            doc.put("whenMatched", pipeline);
        }

        addOperator(UtilsMap.of("$merge", doc));
        return this;
    }

    @Override
    public Aggregator<T, R> out(String collectionName) {
        addOperator(UtilsMap.of("$out", UtilsMap.of("coll", collectionName)));
        return this;
    }

    @Override
    public Aggregator<T, R> out(Class<?> type) {
        return out(morphium.getMapper().getCollectionName(type));
    }

    @Override
    public Aggregator<T, R> out(String db, String collectionName) {
        addOperator(UtilsMap.of("$out", UtilsMap.of("coll", collectionName, "db", db)));
        return this;
    }

    @Override
    public Aggregator<T, R> planCacheStats(Map<String, Object> param) {
        addOperator(UtilsMap.of("$planCacheStats", new HashMap<>()));
        return this;
    }

    /**
     * redact needs to resolve to $$DESCEND, $$PRUNE, or $$KEEP
     * <p>
     * System Variable Description
     * $$DESCEND $redact returns the fields at the current document level, excluding
     * embedded documents. To include embedded documents and embedded documents
     * within arrays, apply the $cond expression to the embedded documents to
     * determine access for these embedded documents.
     * $$PRUNE $redact excludes all fields at this current document/embedded
     * document level, without further inspection of any of the excluded fields.
     * This applies even if the excluded field contains embedded documents that may
     * have different access levels.
     * $$KEEP $redact returns or keeps all fields at this current document/embedded
     * document level, without further inspection of the fields at this level. This
     * applies even if the included field contains embedded documents that may have
     * different access levels.
     */
    @Override
    public Aggregator<T, R> redact(Expr redact) {
        addOperator(UtilsMap.of("$redact", redact));
        return this;
    }

    @Override
    public Aggregator<T, R> replaceRoot(Expr newRoot) {
        addOperator(UtilsMap.of("$replaceRoot", UtilsMap.of("newRoot", newRoot)));
        return this;
    }

    @Override
    public Aggregator<T, R> replaceWith(Expr newDoc) {
        addOperator(UtilsMap.of("$replaceWith", newDoc));
        return this;
    }

    @Override
    public Aggregator<T, R> sample(int sampleSize) {
        addOperator(UtilsMap.of("$sample", UtilsMap.of("size", sampleSize)));
        return this;
    }

    /**
     * Adds new fields to documents. $set outputs documents that contain all
     * existing fields from the input documents and newly added fields.
     * <p>
     * The $set stage is an alias for $addFields.
     *
     * @param param
     * @return
     */
    @Override
    public Aggregator<T, R> set(Map<String, Expr> param) {
        Map<String, Object> qo = Utils.getQueryObjectMap(param);
        if (isTranslateAggregationFieldNames()) {
            qo = fieldNames.translateKeys(qo);
            qo.replaceAll((k, v) -> fieldNames.translateRefs(v));
        }
        Map<String, Object> o = UtilsMap.of("$set", qo);
        addOperator(o);
        return this;
    }

    /**
     * The $sortByCount stage is equivalent to the following $group + $sort
     * sequence:
     * <p>
     * <p>
     * { $group: { _id: &lt;expression&gt;, count: { $sum: 1 } } },
     * { $sort: { count: -1 } }
     *
     * @param sortby
     * @return
     */
    @Override
    public Aggregator<T, R> densify(String field, Number step) {
        return densify(field, step, "full");
    }

    @Override
    public Aggregator<T, R> densify(String field, Number step, Object bounds) {
        return densify(field, step, bounds, null, null);
    }

    @Override
    public Aggregator<T, R> densify(String field, Number step, Object bounds, String unit, List<String> partitionByFields) {
        Map<String, Object> range = new LinkedHashMap<>();
        range.put("step", step);

        if (unit != null) {
            range.put("unit", unit);
        }

        range.put("bounds", bounds);
        Map<String, Object> spec = new LinkedHashMap<>();
        spec.put("field", tf(field));
        spec.put("range", range);

        if (partitionByFields != null) {
            spec.put("partitionByFields", partitionByFields.stream().map(this::tf).collect(Collectors.toList()));
        }

        addOperator(UtilsMap.of("$densify", spec));
        return this;
    }

    @Override
    public Aggregator<T, R> documents(List<Map<String, Object>> documents) {
        addOperator(UtilsMap.of("$documents", documents));
        return this;
    }

    @Override
    public Aggregator<T, R> fill(Map<String, Object> output) {
        return fill(null, output);
    }

    @Override
    public Aggregator<T, R> fill(Map<String, Object> sortBy, Map<String, Object> output) {
        Map<String, Object> spec = new LinkedHashMap<>();

        if (sortBy != null) {
            spec.put("sortBy", fieldNames.translateKeys(sortBy));
        }

        spec.put("output", fieldNames.translateKeys(output));
        addOperator(UtilsMap.of("$fill", spec));
        return this;
    }

    @Override
    public Aggregator<T, R> setWindowFields(Object partitionBy, Map<String, Object> sortBy, Map<String, Object> output) {
        boolean translate = isTranslateAggregationFieldNames();
        Map<String, Object> spec = new LinkedHashMap<>();

        if (partitionBy != null) {
            Object pb = partitionBy instanceof Expr ? ((Expr) partitionBy).toQueryObject() : partitionBy;
            spec.put("partitionBy", translate ? fieldNames.translateRefs(pb) : pb);
        }

        if (sortBy != null) {
            spec.put("sortBy", fieldNames.translateKeys(sortBy));
        }

        Map<String, Object> out = fieldNames.translateKeys(output);
        spec.put("output", translate ? fieldNames.translateRefs(out) : out);
        addOperator(UtilsMap.of("$setWindowFields", spec));
        return this;
    }

    @Override
    public Aggregator<T, R> sortByCount(Expr sortby) {
        addOperator(UtilsMap.of("$sortByCount", sortby));
        return this;
    }

    @Override
    public Aggregator<T, R> unionWith(String collection) {
        addOperator(UtilsMap.of("$unionWith", collection));
        return this;
    }

    @Override
    public Aggregator<T, R> unionWith(Aggregator agg) {
        addOperator(UtilsMap.of("$unionWith", UtilsMap.of("coll", (Object) collectionName, "pipeline", agg.getPipeline())));
        return this;
    }

    @Override
    public Aggregator<T, R> unset(List<String> field) {
        addOperator(UtilsMap.of("$unset", field.stream().map(this::tf).collect(Collectors.toList())));
        return this;
    }

    @Override
    public Aggregator<T, R> unset(String... param) {
        addOperator(UtilsMap.of("$unset", Arrays.stream(param).map(this::tf).collect(Collectors.toList())));
        return this;
    }

    @Override
    public Aggregator<T, R> unset(Enum... field) {
        List<String> lst = Arrays.stream(field).map(e -> tf(e.name())).collect(Collectors.toList());
        addOperator(UtilsMap.of("$unset", lst));
        return this;
    }

    @Override
    public Aggregator<T, R> genericStage(String stageName, Object param) {
        if (!(param instanceof Expr)) {
            throw new IllegalArgumentException("inMemAggregation only works with Expr");
        }

        if (!stageName.startsWith("$")) {
            stageName = "$" + stageName;
        }

        addOperator(UtilsMap.of(stageName, param));
        return this;
    }

    @Override
    public Aggregator<T, R> collation(Collation collation) {
        this.collation = collation;
        return this;
    }

    /**
     * Builds a MorphiumDriverException shaped like a real mongod command error (numeric
     * mongoCode + message), so that stage/expression failures surface to callers (including
     * over the wire, e.g. PoppyDB) as a proper command error instead of being logged and
     * swallowed into an empty/partial result.
     */
    private MorphiumDriverException mongoCommandError(int code, String message) {
        MorphiumDriverException ex = new MorphiumDriverException(message);
        ex.setMongoCode(code);
        return ex;
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> execStep(Map<String, Object> step, List<Map<String, Object >> data) {
        if (step.keySet().size() != 1) {
            throw new IllegalArgumentException("Pipeline start wrong");
        }

        String stage = step.keySet().stream().findFirst().get();
        List<Map<String, Object>> ret = new ArrayList<>();

        switch (stage) {
            case "$unset":
                for (Map<String, Object> objm : data) {
                    Map<String, Object> newO = new HashMap<>(objm);

                    if (step.get(stage) instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<String> flds = (List<String>) step.get(stage);

                        for (String f : flds) {
                            newO.remove(morphium.getARHelper().getMongoFieldName(type, f));
                        }
                    } else {
                        newO.remove(step.get(stage));
                    }

                    ret.add(newO);
                }

                break;

            case "$project": {
                @SuppressWarnings("unchecked")
                Map<String, Object> op = (((Map<String, Object>) step.get(stage)));
                // #240: an explicit inclusion flag ({field:1}/true) on any non-_id field switches
                // $project into strict inclusion mode - MongoDB then returns ONLY _id plus the listed
                // and computed fields. Without any such flag we keep the historical lenient behaviour
                // (clone the document, add computed fields, honour {field:0} exclusions), which
                // existing computed-only projection pipelines depend on.
                boolean strictInclusion = false;
                for (Map.Entry<String, Object> e : op.entrySet()) {
                    if (!e.getKey().equals("_id") && isProjectInclusionFlag(e.getValue())) {
                        strictInclusion = true;
                        break;
                    }
                }

                for (Map<String, Object> o : data) {
                    Map<String, Object> obj;

                    if (strictInclusion) {
                        obj = new HashMap<>();
                        // _id is kept unless explicitly excluded with {_id:0}.
                        if (!(op.containsKey("_id") && isProjectExclusionFlag(op.get("_id"))) && o.containsKey("_id")) {
                            obj.put("_id", o.get("_id"));
                        }

                        for (Map.Entry<String, Object> e : op.entrySet()) {
                            String k = e.getKey();
                            Object value = e.getValue();

                            if (k.equals("_id")) {
                                // Computed _id ({_id:<expr>}/{_id:"$ref"}) still applies; the plain
                                // 0/1 flag was already handled above.
                                if (!isProjectInclusionFlag(value) && !isProjectExclusionFlag(value)) {
                                    obj.put(k, projectComputedValue(value, o));
                                }
                                continue;
                            }

                            if (isProjectInclusionFlag(value)) {
                                Object v = getByPath(o, k);
                                if (v != null || o.containsKey(k)) {
                                    obj.put(k, v);
                                }
                            } else if (isProjectExclusionFlag(value)) {
                                // {field:0} mixed with inclusions is invalid in MongoDB (except _id);
                                // ignore the exclusion rather than error.
                            } else {
                                obj.put(k, projectComputedValue(value, o));
                            }
                        }
                    } else {
                        obj = new HashMap<>(o);

                        for (String k : op.keySet()) {
                            Object value = op.get(k);

                            if (value instanceof String && ((String) value).startsWith("$")) {
                                String path = ((String) value).substring(1);
                                Object v = getByPath(obj, path);
                                obj.put(k, v);
                            } else if (value instanceof Expr.ValueExpr) {
                                Object evaluate = ((Expr) value).evaluate(obj);

                                if (Integer.valueOf(0).equals(evaluate)) {
                                    obj.remove(k);
                                }
                            } else if (value instanceof Expr) {
                                Object evaluate = ((Expr) value).evaluate(obj);
                                obj.put(k, evaluate);
                            } else if (value instanceof Integer) {
                                if (((Integer) value) == 0) {
                                    obj.remove(k);
                                }
                            } else if (value instanceof Map) {
                                //noinspection unchecked
                                for (String fld : ((Map<String, Object>) value).keySet()) {
                                    if (obj.get(fld) instanceof Expr) {
                                        obj.put(fld, ((Expr) obj.get(fld)).evaluate(obj));
                                    } else {
                                        log.error("InMemoryAggregation only works with Expr");
                                    }
                                }
                            }
                        }
                    }

                    ret.add(obj);
                }

                break;
            }

            case "$set":
            case "$addFields":
                for (Map<String, Object> o : data) {
                    Map<String, Object> obj = new HashMap<>(o);
                    ret.add(obj);
                    @SuppressWarnings("unchecked")
                    Map<String, Object> op = (((Map<String, Object>) step.get(stage)));

                    for (String k : op.keySet()) {
                        Object value = op.get(k);

                        if (value instanceof String && ((String) value).startsWith("$")) {
                            obj.put(k, obj.get(((String) value).substring(1)));
                        } else if (value instanceof Expr) {
                            obj.put(k, ((Expr) value).evaluate(obj));
                        } else if (value instanceof Map) {
                            // Convert aggregation expression Map to Expr and evaluate
                            //noinspection unchecked
                            Expr expr = Expr.parse(value);
                            obj.put(k, expr.evaluate(obj));
                        }
                    }
                }

                break;

            case "$count":
                // MongoDB emits no document at all when the stage input is empty
                if (!data.isEmpty()) {
                    ret.add(UtilsMap.of((String) step.get(stage), data.size()));
                }
                break;

            case "$group":
                @SuppressWarnings("unchecked")
                Map<String, Object> group = (Map<String, Object>) step.get(stage);
                Map<Object, Map<String, Object>> res = new HashMap<>();

                for (Map<String, Object> obj : data) {
                    Map<String, Object> o = new HashMap<>(obj);
                    Object id = group.get("_id");

                    if (id instanceof Map) {
                        //deal with combined Group IDs
                        //and expressions in IDs
                        if (((Map <?, ? >) id).keySet().toArray()[0].toString().startsWith("$")) {
                            //expr?
                            var expr = Expr.parse(id);
                            var result = expr.evaluate(obj);
                            res.putIfAbsent(result, new HashMap<>());
                            res.get(result).putIfAbsent("_id", result);
                            id = result;
                        } else {
                            log.info("ID is combined...");
                            Map newIdMap = new HashMap<>();

                            for (var e : ((Map <?, ? >) id).entrySet()) {
                                var k = e.getKey();

                                try {
                                    var kEx = Expr.parse(e.getKey());
                                    k = kEx.evaluate(o);

                                    while (k instanceof Expr) {
                                        k = ((Expr) k).evaluate(o);
                                    }
                                } catch (Exception ex) {
                                }

                                var v = e.getValue();

                                try {
                                    var vEy = Expr.parse(e.getValue());
                                    v = vEy.evaluate(o);

                                    while (v instanceof Expr) {
                                        v = ((Expr) v).evaluate(o);
                                    }
                                } catch (Exception ex) {
                                }

                                newIdMap.put(k, v);
                            }

                            id = newIdMap;
                            res.putIfAbsent(id, new HashMap<>());
                            res.get(id).putIfAbsent("_id", newIdMap);
                        }
                    } else {
                        if (id != null && id.toString().startsWith("$")) {
                            id = o.get(id.toString().substring(1));
                        }

                        res.putIfAbsent(id, new HashMap<>());
                        res.get(id).putIfAbsent("_id", id);
                    }

                    for (String fld : group.keySet()) {
                        if (fld.equals("_id")) {
                            continue;
                        }

                        Object opValue = group.get(fld);

                        if (opValue instanceof Map) {
                            //expression?
                            @SuppressWarnings("unchecked")
                            String op = ((Map<String, Object>) opValue).keySet().stream().findFirst().get();

                            switch (op) {
                                case "$addToSet":
                                case "$push":
                                    Object toPush = ((Map <?, ? >) opValue).get(op);
                                    Object setValue = null;

                                    if (toPush instanceof Map) {
                                        //pushing an ObjectMapperImpl
                                        setValue = new HashMap();

                                        for (var e : ((Map<String, Object>) toPush).entrySet()) {
                                            if (e.getValue() instanceof Expr) {
                                                ((Map) setValue).put(e.getKey(), ((Expr) e.getValue()).evaluate(o));
                                            } else if (e.getValue() instanceof String) {
                                                String v = e.getValue().toString();

                                                if (v.startsWith("$")) {
                                                    setValue = o.get(v.substring(1));
                                                } else {
                                                    setValue = v;
                                                }
                                            } else {
                                                setValue = e.getValue();
                                            }
                                        }
                                    } else {
                                        String v = (String) toPush;

                                        if (v.startsWith("$")) {
                                            setValue = o.get(v.substring(1));
                                        } else {
                                            setValue = toPush;
                                        }
                                    }

                                    res.get(id).putIfAbsent(fld, new ArrayList<>());

                                    if (op.equals("$push")) {
                                        //noinspection unchecked
                                        ((List) res.get(id).get(fld)).add(setValue);
                                    } else {
                                        //not using HashSet, it would break the contract
                                        List l = ((List) res.get(id).get(fld));

                                        if (!l.contains(setValue)) {
                                            //noinspection unchecked
                                            l.add(setValue);
                                        }
                                    }

                                    break;

                                case "$avg":
                                    res.get(id).putIfAbsent("$_calc_" + fld, UtilsMap.of("sum", 0, "count", 0));

                                    //res.get(id).putIfAbsent(fld, UtilsMap.of("sum", 0, "count", 0, "avg", 0));
                                    if (((Map <?, ? >) opValue).get(op).toString().startsWith("$")) {
                                        //field reference
                                        Number count = (Number)((Map) res.get(id).get("$_calc_" + fld)).get("count");
                                        count = count.intValue() + 1;
                                        //noinspection unchecked
                                        ((Map) res.get(id).get("$_calc_" + fld)).put("count", count);
                                        Number current = (Number)((Map) res.get(id).get("$_calc_" + fld)).get("sum");
                                        Number v = (Number) o.get(((Map <?, ? >) opValue).get(op).toString().substring(1));
                                        Number sum = current.doubleValue() + v.doubleValue();
                                        //noinspection unchecked
                                        ((Map) res.get(id).get("$_calc_" + fld)).put("sum", sum);
                                        //noinspection unchecked
                                        res.get(id).put(fld, sum.doubleValue() / count.doubleValue());
                                    } else {
                                        log.error("Average with no $-reference?");
                                    }

                                    break;

                                case "$first":
                                    res.get(id).putIfAbsent(fld, o.get(((Map <?, ? >) opValue).get(op).toString().substring(1)));
                                    break;

                                case "$last":
                                    res.get(id).put(fld, o.get(((Map <?, ? >) opValue).get(op).toString().substring(1)));
                                    break;

                                case "$max":
                                    if (((Map <?, ? >) opValue).get(op).toString().startsWith("$")) {
                                        Object oVal = o.get(((Map <?, ? >) opValue).get(op).toString().substring(1));
                                        res.get(id).putIfAbsent(fld, oVal);

                                        //noinspection unchecked
                                        if (((Comparable) res.get(id).get(fld)).compareTo(oVal) < 0) {
                                            res.get(id).put(fld, oVal);
                                        }
                                    }

                                    break;

                                case "$min":
                                    if (((Map <?, ? >) opValue).get(op).toString().startsWith("$")) {
                                        Object oVal = o.get(((Map <?, ? >) opValue).get(op).toString().substring(1));
                                        res.get(id).putIfAbsent(fld, oVal);

                                        //noinspection unchecked
                                        if (((Comparable) res.get(id).get(fld)).compareTo(oVal) > 0) {
                                            res.get(id).put(fld, oVal);
                                        }
                                    }

                                    break;

                                case "$sum":
                                    res.get(id).putIfAbsent(fld, 0);
                                    Number current = (Number) res.get(id).get(fld);

                                    if (((Map <?, ? >) opValue).get(op).toString().startsWith("$")) {
                                        //field reference
                                        Number v = (Number) o.get(((Map <?, ? >) opValue).get(op).toString().substring(1));
                                        res.get(id).put(fld, current.doubleValue() + v.doubleValue());
                                    } else if (((Map <?, ? >) opValue).get(op) instanceof Number) {
                                        Number v = (Number)((Map <?, ? >) opValue).get(op);
                                        res.get(id).put(fld, current.doubleValue() + v.doubleValue());
                                    } else if (((Map <?, ? >) opValue).get(op) instanceof Expr) {
                                        Number v = (Number)(o.get(((Expr)((Map <?, ? >) opValue).get(op)).evaluate(o)));
                                        res.get(id).put(fld, current.doubleValue() + v.doubleValue());
                                    }

                                    break;

                                case "$mergeObjects":
                                    // $mergeObjects: Merges multiple documents into a single document
                                    // Can be used to merge objects from an array or multiple field references
                                    Object mergeValue = ((Map<?, ?>) opValue).get(op);
                                    Map<String, Object> mergedDoc = new HashMap<>();

                                    if (mergeValue instanceof String) {
                                        // Field reference: $mergeObjects: "$fieldName"
                                        String fieldName = mergeValue.toString();
                                        if (fieldName.startsWith("$")) {
                                            fieldName = fieldName.substring(1);
                                        }
                                        Object fieldValue = o.get(fieldName);

                                        if (fieldValue instanceof Map) {
                                            mergedDoc.putAll((Map<String, Object>) fieldValue);
                                        } else if (fieldValue instanceof List) {
                                            // Merge all objects in the array
                                            for (Object item : (List<?>) fieldValue) {
                                                if (item instanceof Map) {
                                                    mergedDoc.putAll((Map<String, Object>) item);
                                                }
                                            }
                                        }
                                    } else if (mergeValue instanceof Map) {
                                        // Direct object or expression
                                        for (Map.Entry<String, Object> entry : ((Map<String, Object>) mergeValue).entrySet()) {
                                            Object val = entry.getValue();
                                            if (val instanceof Expr) {
                                                mergedDoc.put(entry.getKey(), ((Expr) val).evaluate(o));
                                            } else if (val instanceof String && val.toString().startsWith("$")) {
                                                mergedDoc.put(entry.getKey(), o.get(val.toString().substring(1)));
                                            } else {
                                                mergedDoc.put(entry.getKey(), val);
                                            }
                                        }
                                    } else if (mergeValue instanceof List) {
                                        // Array of objects to merge: $mergeObjects: [ "$obj1", "$obj2", ... ]
                                        for (Object item : (List<?>) mergeValue) {
                                            if (item instanceof String && item.toString().startsWith("$")) {
                                                Object fieldValue = o.get(item.toString().substring(1));
                                                if (fieldValue instanceof Map) {
                                                    mergedDoc.putAll((Map<String, Object>) fieldValue);
                                                }
                                            } else if (item instanceof Map) {
                                                mergedDoc.putAll((Map<String, Object>) item);
                                            }
                                        }
                                    }

                                    // Merge into existing result for this group
                                    if (!res.get(id).containsKey(fld)) {
                                        res.get(id).put(fld, new HashMap<String, Object>());
                                    }

                                    if (res.get(id).get(fld) instanceof Map) {
                                        ((Map<String, Object>) res.get(id).get(fld)).putAll(mergedDoc);
                                    } else {
                                        res.get(id).put(fld, mergedDoc);
                                    }
                                    break;

                                case "$stdDevPop":
                                case "$stdDevSamp": {
                                    // Collect the raw (unfiltered) per-document values for this group/field;
                                    // the actual pop/samp math runs once, after all documents have been seen,
                                    // in the finalize pass below (two-pass algorithm - see Expr.computeStdDevPop/Samp).
                                    String calcKey = "$_calc_" + fld;
                                    res.get(id).putIfAbsent(calcKey, new ArrayList<>());
                                    @SuppressWarnings("unchecked")
                                    List<Object> rawValues = (List<Object>) res.get(id).get(calcKey);
                                    Object rawSpec = ((Map<?, ?>) opValue).get(op);

                                    if (rawSpec instanceof String && rawSpec.toString().startsWith("$")) {
                                        rawValues.add(o.get(rawSpec.toString().substring(1)));
                                    } else if (rawSpec instanceof Expr) {
                                        rawValues.add(((Expr) rawSpec).evaluate(o));
                                    } else {
                                        rawValues.add(rawSpec);
                                    }

                                    break;
                                }

                                case "$accumulator":
                                    throw new RuntimeException(op + " not implemented yet,sorry");

                                default:
                                    if (opValue instanceof Map) {
                                        Map<String, Object> opMap = (Map<String, Object>) opValue;

                                        try {
                                            Expr expr = Expr.parse(opMap);
                                            res.get(id).put(fld, expr.evaluate(o));
                                        } catch (Exception e) {
                                            // Previously this silently dropped the field and only logged
                                            // "unknown accumulator" (log.error ran unconditionally, even on
                                            // success). A $group field must be an accumulator expression; if it
                                            // isn't one we recognize AND it doesn't parse/evaluate as a generic
                                            // expression either, that's the same condition real mongod reports as
                                            // "unknown group operator" (Location15952) - surface it the same way
                                            // instead of returning a document with the field missing.
                                            throw mongoCommandError(15952, "unknown group operator '" + op + "'");
                                        }
                                    } else {
                                        throw mongoCommandError(15952, "unknown group operator '" + op + "'");
                                    }

                                    break;
                            }
                        } else if (opValue instanceof String && opValue.toString().startsWith("$")) {
                            opValue = o.get(opValue.toString().substring(1));
                            res.get(id).put(fld, opValue);
                        }
                    }
                }

                // Finalize two-pass accumulators ($stdDevPop/$stdDevSamp): now that every document
                // has been folded into res, replace each group's temporary raw-value list with the
                // actual computed standard deviation and drop the internal "$_calc_" bookkeeping key.
                for (String fld : group.keySet()) {
                    if (fld.equals("_id")) {
                        continue;
                    }

                    Object opValue = group.get(fld);

                    if (!(opValue instanceof Map)) {
                        continue;
                    }

                    String op = ((Map<String, Object>) opValue).keySet().stream().findFirst().orElse(null);

                    if (!"$stdDevPop".equals(op) && !"$stdDevSamp".equals(op)) {
                        continue;
                    }

                    String calcKey = "$_calc_" + fld;

                    for (Map<String, Object> groupResult : res.values()) {
                        Object calc = groupResult.remove(calcKey);
                        @SuppressWarnings("unchecked")
                        List<Object> rawValues = calc instanceof List ? (List<Object>) calc : Collections.emptyList();
                        Double stdDev = "$stdDevPop".equals(op) ? Expr.computeStdDevPop(rawValues) : Expr.computeStdDevSamp(rawValues);
                        groupResult.put(fld, stdDev);
                    }
                }

                // Catch-all: drop any residual "$_calc_" bookkeeping keys before emitting the group
                // output. Two-pass accumulators ($stdDevPop/$stdDevSamp) consume their own key above;
                // single-pass ones like $avg keep a running "$_calc_<field>" (sum/count) that must not
                // leak into the result. Removing by prefix keeps future "$_calc_" accumulators clean too.
                for (Map<String, Object> groupResult : res.values()) {
                    groupResult.keySet().removeIf(k -> k.startsWith("$_calc_"));
                }

                ret.addAll(res.values());
                break;

            case "$skip":
            case "$limit":
                Object op = step.get(stage);

                if (op instanceof Expr) {
                    op = ((Expr) op).evaluate(new HashMap<>());
                }

                int idx = ((Number) op).intValue();

                if (stage.equals("$limit")) {
                    if (idx < data.size()) {
                        ret.addAll(data.subList(0, idx));
                    } else {
                        ret.addAll(data);
                    }
                } else {
                    ret.addAll(data.subList(idx, data.size() - idx));
                }

                break;

            case "$match":
                //noinspection unchecked
                Map<String, Object> colMap = collation == null ? null : collation.toQueryObject();

                // ret = data.stream().filter((doc) -> QueryHelper.matchesQuery((Map<String, Object>) step.get(stage), doc, colMap)).collect(Collectors.toList());

                for (var doc : data) {
                    if (QueryHelper.matchesQuery((Map<String, Object>) step.get(stage), doc, colMap)) {
                        ret.add(doc);
                    }
                }

                break;

            case "$unwind":
                op = step.get(stage);

                for (Map<String, Object> o : data) {
                    List lst;
                    String n;

                    if (op instanceof Map) {
                        op = ((Map) op).get("path");
                    }

                    if (op instanceof Expr) {
                        lst = (List)((Expr) op).evaluate(o);
                        n = ((Expr) op).toQueryObject().toString();
                    } else if (op instanceof String) {
                        //should be a reference
                        if (op.toString().startsWith("$")) {
                            op = op.toString().substring(1);
                        }

                        n = op.toString();
                        lst = (List) o.get(op.toString());
                    } else {
                        log.error("Wrong reference: " + op);
                        break;
                    }

                    if (lst == null) {
                        break;
                    }

                    for (Object value : lst) {
                        Map<String, Object> result = new HashMap<>(o);

                        if (n.startsWith("$")) {
                            n = n.substring(1);
                        }

                        if (result.containsKey(n)) {
                            result.put(n, value);
                        } else {
                            result.put(new AnnotationAndReflectionHelper(true).convertCamelCase(n), value);
                        }

                        ret.add(result);
                    }
                }

                break;

            case "$search":
                log.warn(
                                "The $search aggregation pipeline stage is only available for collections hosted on MongoDB Atlas cluster tiers running MongoDB version 4.2 or later. To learn more, see Atlas Search.");
                break;

            case "$sort":
                @SuppressWarnings("unchecked")
                Map<String, Object> keysToSortBy = (Map<String, Object>) step.get(stage);
                List<Map<String, Object>> sortedList = new ArrayList<>(data);
                sortedList.sort((o1, o2) -> {
                    for (String k : keysToSortBy.keySet()) {
                        @SuppressWarnings("unchecked")
                        int i = ((Comparable) o1.get(k)).compareTo(o2.get(k));

                        if (i != 0) {
                            if (keysToSortBy.get(k).equals(-1)) {
                                i = -i;
                            }

                            //TextIndex ignored, will be handeled like normal sort
                            return i;
                        }
                    }
                    return 0;
                });
                ret = sortedList;
                break;

            case "$lookup":
                // from: <collection to join>,
                //       localField: <field from the input documents>,
                //       foreignField: <field from the documents of the "from" collection>,
                //       as: <output array field>
                ret = new ArrayList<>();
                @SuppressWarnings("unchecked")
                Map<String, Object> lookup = (Map<String, Object>) step.get(stage);
                String collection = (String) lookup.get("from");
                String localField = (String) lookup.get("localField");
                String foreignField = (String) lookup.get("foreignField");
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> pipeline = (List<Map<String, Object >> ) lookup.get("pipeline");
                @SuppressWarnings("unchecked")
                Map<String, Object> let = (Map<String, Object>) lookup.get("let");
                String as = (String) lookup.get("as");

                if (pipeline != null || let != null) {
                    // Advanced $lookup with pipeline and let variables
                    for (Map<String, Object> doc : data) {
                        // Process let variables - create a context with variables from the current document
                        Map<String, Object> letContext = new HashMap<>();
                        if (let != null) {
                            for (Map.Entry<String, Object> letEntry : let.entrySet()) {
                                String varName = letEntry.getKey();
                                Object varExpr = letEntry.getValue();

                                // Evaluate the let expression against the current document
                                Object varValue;
                                if (varExpr instanceof Expr) {
                                    varValue = ((Expr) varExpr).evaluate(doc);
                                } else if (varExpr instanceof Map) {
                                    Expr expr = Expr.parse((Map<String, Object>) varExpr);
                                    varValue = expr.evaluate(doc);
                                } else if (varExpr instanceof String strValue) {
                                    if (strValue.startsWith("$$")) {
                                        varValue = letContext.get(strValue);
                                    } else if (strValue.startsWith("$")) {
                                        varValue = extractValueByPath(doc, strValue.substring(1));
                                    } else {
                                        varValue = strValue;
                                    }
                                } else {
                                    varValue = varExpr;
                                }
                                letContext.put("$$" + varName, varValue);
                            }
                        }

                        // Get foreign collection data
                        InMemoryDriver inMemDriver = (InMemoryDriver) morphium.getDriver();
                        Map<String, List<Map<String, Object>>> database = inMemDriver.getDatabase(morphium.getConfig().connectionSettings().getDatabase());
                        List<Map<String, Object>> foreignCollection = database.get(collection);

                        if (foreignCollection != null) {
                            // Process the pipeline with let context on the foreign collection data
                            List<Map<String, Object>> foreignData = new ArrayList<>(foreignCollection);
                            List<Map<String, Object>> pipelineResults = new ArrayList<>();
                            if (pipeline != null && !pipeline.isEmpty()) {
                                // Apply each pipeline stage with let variable substitution
                                for (Object pipelineItem : pipeline) {
                                    Map<String, Object> pipelineStage;
                                    if (pipelineItem instanceof Expr) {
                                        // Convert Expr to Map representation
                                        pipelineStage = (Map<String, Object>) ((Expr) pipelineItem).toQueryObject();
                                    } else if (pipelineItem instanceof Map) {
                                        pipelineStage = (Map<String, Object>) pipelineItem;
                                    } else {
                                        continue; // Skip unknown types
                                    }

                                    Map<String, Object> processedStage = substituteLetVariables(pipelineStage, letContext);
                                    foreignData = execStep(processedStage, foreignData);
                                }
                                pipelineResults = foreignData;
                            } else {
                                pipelineResults = foreignData;
                            }

                            // Add results to the document
                            Map<String, Object> resultDoc = new LinkedHashMap<>(doc);
                            resultDoc.put(as, pipelineResults);
                            ret.add(resultDoc);
                        } else {
                            // Foreign collection doesn't exist, add empty array
                            Map<String, Object> resultDoc = new LinkedHashMap<>(doc);
                            resultDoc.put(as, new ArrayList<>());
                            ret.add(resultDoc);
                        }
                    }
                    break;
                }

                for (Map<String, Object> doc : data) {
                    Object localValue = doc.get(localField);

                    // Use the InMemoryDriver's private find method via reflection or direct access
                    // Since we're in the InMemAggregator which is part of the inmem package,
                    // we can access the InMemoryDriver's data directly
                    InMemoryDriver inMemDriver = (InMemoryDriver) morphium.getDriver();

                    try {
                        // Get the foreign collection data directly from InMemoryDriver
                        Map<String, List<Map<String, Object>>> database = inMemDriver.getDatabase(morphium.getConfig().connectionSettings().getDatabase());
                        List<Map<String, Object>> foreignCollection = database.get(collection);

                        if (foreignCollection == null) {
                            foreignCollection = new ArrayList<>();
                        }

                        // Find matching documents
                        List<Map<String, Object>> matches = new ArrayList<>();
                        for (Map<String, Object> foreignDoc : foreignCollection) {
                            Object foreignValue = foreignDoc.get(foreignField);
                            if ((localValue == null && foreignValue == null) ||
                                    (localValue != null && localValue.equals(foreignValue))) {
                                matches.add(new HashMap<>(foreignDoc));
                            }
                        }

                        // Create new document with joined data
                        Map<String, Object> resultDoc = new HashMap<>(doc);
                        resultDoc.put(as, matches);
                        ret.add(resultDoc);
                    } catch (Exception e) {
                        throw new RuntimeException("$lookup failed: " + e.getMessage(), e);
                    }
                }

                break;

            case "$sample":
                int size = ((Number)((Map) step.get(stage)).get("size")).intValue();
                @SuppressWarnings("unchecked")
                List o = new ArrayList(data);
                Collections.shuffle(o);
                // like mongod: a size >= collection count returns all documents (in random
                // order) - mongosh tab completion samples with size 10 on every collection
                //noinspection unchecked
                ret = o.subList(0, Math.min(size, o.size()));
                break;

            case "$merge": {
                // { $merge: {
                //     into: <collection> -or- { db: <db>, coll: <collection> },
                //     on: <field> -or- [<field>, ...],                          // default: _id
                //     whenMatched: replace|keepExisting|merge|fail|<pipeline>,   // default: merge
                //     whenNotMatched: insert|discard|fail                        // default: insert
                // } }
                // $merge is TERMINAL: it writes to the target collection and yields no documents.
                @SuppressWarnings("unchecked")
                Map<String, Object> mergeSpec = (Map<String, Object>) step.get(stage);
                String mergeDb = morphium.getConfig().connectionSettings().getDatabase();
                String mergeColl;

                if (mergeSpec.get("into") instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> into = (Map<String, Object>) mergeSpec.get("into");

                    if (into.get("db") != null) {
                        mergeDb = String.valueOf(into.get("db"));
                    }

                    mergeColl = into.get("coll") == null ? null : String.valueOf(into.get("coll"));
                } else {
                    mergeColl = mergeSpec.get("into") == null ? null : String.valueOf(mergeSpec.get("into"));
                }

                if (mergeColl == null || mergeColl.isEmpty()) {
                    throw mongoCommandError(51178, "$merge 'into' must name a target collection");
                }

                // `on` defaults to _id; a single field may be given as a plain string.
                List<String> onFields = new ArrayList<>();
                Object onSpec = mergeSpec.get("on");

                if (onSpec instanceof List) {
                    for (Object onEntry : (List<?>) onSpec) {
                        onFields.add(String.valueOf(onEntry));
                    }
                } else if (onSpec != null) {
                    onFields.add(String.valueOf(onSpec));
                }

                if (onFields.isEmpty()) {
                    onFields.add("_id");
                }

                // whenMatched may be a custom update pipeline: it runs per match with the EXISTING
                // target document as input and the incoming document bound to $$new (unless a
                // custom `let` replaces the default {new: "$$ROOT"}, as mongod does).
                Object letSpec = mergeSpec.get("let");
                List<Map<String, Object>> matchedPipeline = null;
                String whenMatched = null;

                if (mergeSpec.get("whenMatched") instanceof List) {
                    matchedPipeline = parseMergeWhenMatchedPipeline((List<?>) mergeSpec.get("whenMatched"), letSpec);
                } else {
                    if (letSpec != null) {
                        // mongod rejects this with 51199 as well
                        throw mongoCommandError(51199, "$merge: let is only allowed when whenMatched is a pipeline");
                    }

                    whenMatched = mergeSpec.get("whenMatched") == null
                                  ? "merge" : String.valueOf(mergeSpec.get("whenMatched"));

                    if (!List.of("replace", "keepExisting", "merge", "fail").contains(whenMatched)) {
                        throw mongoCommandError(51190, "unknown $merge whenMatched action '" + whenMatched + "'");
                    }
                }

                String whenNotMatched = mergeSpec.get("whenNotMatched") == null
                                        ? "insert" : String.valueOf(mergeSpec.get("whenNotMatched"));

                if (!List.of("insert", "discard", "fail").contains(whenNotMatched)) {
                    throw mongoCommandError(51191, "unknown $merge whenNotMatched action '" + whenNotMatched + "'");
                }

                InMemoryDriver mergeDriver = (InMemoryDriver) morphium.getDriver();

                for (Map<String, Object> doc : data) {
                    Map<String, Object> onQuery = new HashMap<>();

                    for (String onField : onFields) {
                        Object v = getByPath(doc, onField);

                        if (v == null && !doc.containsKey(onField)) {
                            throw mongoCommandError(51132,
                                "$merge document is missing the 'on' field '" + onField + "'");
                        }

                        onQuery.put(onField, v);
                    }

                    List<Map<String, Object>> matches = mergeDriver.find(mergeDb, mergeColl, onQuery, null, null, 0, 0);

                    if (matches.isEmpty()) {
                        if ("discard".equals(whenNotMatched)) {
                            continue;
                        }

                        if ("fail".equals(whenNotMatched)) {
                            throw mongoCommandError(13113,
                                "$merge found no matching document in " + mergeDb + "." + mergeColl
                                + " and whenNotMatched is 'fail'");
                        }

                        mergeDriver.store(mergeDb, mergeColl,
                                          new ArrayList<>(List.of(new HashMap<>(doc))), null);
                        continue;
                    }

                    if (matches.size() > 1) {
                        // MongoDB requires the `on` fields to identify at most one document (enforced
                        // there by a unique index); refuse rather than picking one arbitrarily.
                        throw mongoCommandError(51268,
                            "$merge 'on' fields " + onFields + " matched " + matches.size()
                            + " documents in " + mergeDb + "." + mergeColl + " - they must be unique");
                    }

                    Map<String, Object> existing = matches.get(0);

                    if (matchedPipeline != null) {
                        Map<String, Object> vars = bindMergeLetVariables(letSpec, doc);
                        Map<String, Object> piped = applyMergeWhenMatchedPipeline(matchedPipeline, existing, vars);
                        piped.put("_id", existing.get("_id"));
                        mergeDriver.store(mergeDb, mergeColl, new ArrayList<>(List.of(piped)), null);
                        continue;
                    }

                    if ("keepExisting".equals(whenMatched)) {
                        continue;
                    }

                    if ("fail".equals(whenMatched)) {
                        throw mongoCommandError(13114,
                            "$merge matched an existing document in " + mergeDb + "." + mergeColl
                            + " and whenMatched is 'fail'");
                    }

                    // Both replace and merge keep the TARGET's _id; store() is a replace-by-_id, so
                    // "merge" has to build the union itself (incoming fields win over existing ones -
                    // the old scaffold had this precedence inverted).
                    // "merge" starts from the existing document (target-only fields survive),
                    // "replace" starts empty (they do not); the incoming fields are layered on top
                    // either way.
                    Map<String, Object> toStore = "merge".equals(whenMatched)
                                                  ? new HashMap<>(existing) : new HashMap<>();
                    toStore.putAll(doc);
                    toStore.put("_id", existing.get("_id"));
                    mergeDriver.store(mergeDb, mergeColl, new ArrayList<>(List.of(toStore)), null);
                }

                ret = new ArrayList<>();
                break;
            }

            case "$replaceRoot":
            case "$replaceWith":
                Object newRoot;

                if (step.get(stage) instanceof Map) {
                    newRoot = ((Map) step.get(stage)).get("newRoot");
                } else {
                    newRoot = step.get(stage);
                }

                if (newRoot instanceof String) {
                    if (newRoot.toString().startsWith("$")) {
                        //fieldRef
                        for (Map<String, Object> doc : data) {
                            //noinspection unchecked
                            ret.add((Map<String, Object>) Expr.field(newRoot.toString()).evaluate(doc));
                        }
                    } else {
                        throw new IllegalArgumentException("cannot replace root with single value");
                    }
                } else {
                    //parse expr!!!!!
                    Expr expr = Expr.parse(newRoot);

                    for (Map<String, Object> doc : data) {
                        //noinspection unchecked
                        ret.add((Map<String, Object>) expr.evaluate(doc));
                    }
                }

                break;

            case "$facet":
                op = step.get(stage);

                if (op instanceof Map) {
                    Map<String, Object> facetMap = (Map<String, Object>) op;
                    Map<String, Object> facetResult = new HashMap<>();

                    // Execute each facet pipeline on current data
                    for (Map.Entry<String, Object> facetEntry : facetMap.entrySet()) {
                        String facetName = facetEntry.getKey();
                        Object facetPipeline = facetEntry.getValue();

                        // Run the aggregation pipeline for this facet
                        List<Map<String, Object>> facetResults = new ArrayList<>();
                        if (facetPipeline instanceof List) {
                            // Process the pipeline steps directly on the data
                            List<Map<String, Object>> tempData = new ArrayList<>(data);
                            List<Map<String, Object>> facetSteps = (List<Map<String, Object>>) facetPipeline;

                            // Apply each stage in the pipeline to the data
                            for (Map<String, Object> facetStep : facetSteps) {
                                tempData = execStep(facetStep, tempData);
                            }
                            facetResults = tempData;
                        }
                        facetResult.put(facetName, facetResults);
                    }

                    ret = List.of(facetResult);
                }
                break;

            // NOTE: $planCacheStats/$redact/$unionWith/$currentOp/$listLocalSessions/$findAndModyfy/
            // $update were previously mis-grouped onto the $bucket case body and silently ran $bucket
            // logic (or returned an empty result) instead of a real implementation. They are not
            // implemented by the in-memory driver, so they now fall through to the default case and
            // surface as a proper "Unrecognized pipeline stage name" command error (code 40324).
            case "$bucket":
                op = step.get(stage);

                if (op instanceof Map) {
                    Map<String, Object> bucketParams = (Map<String, Object>) op;
                    Object groupByObj = bucketParams.get("groupBy");
                    List<Object> boundaries = (List<Object>) bucketParams.get("boundaries");
                    Object defaultBucket = bucketParams.get("default");
                    Map<String, Object> outputSpec = (Map<String, Object>) bucketParams.get("output");

                    // Evaluate boundary values if they are Expr objects
                    List<Object> evaluatedBoundaries = new ArrayList<>();
                    for (Object boundary : boundaries) {
                        if (boundary instanceof Expr) {
                            evaluatedBoundaries.add(((Expr) boundary).evaluate(new HashMap<>()));
                        } else {
                            evaluatedBoundaries.add(boundary);
                        }
                    }

                    // Evaluate default bucket if it's an Expr
                    Object evaluatedDefaultBucket = defaultBucket;
                    if (defaultBucket instanceof Expr) {
                        evaluatedDefaultBucket = ((Expr) defaultBucket).evaluate(new HashMap<>());
                    }

                    Map<Object, List<Map<String, Object>>> buckets = new LinkedHashMap<>();

                    // Initialize buckets
                    for (int i = 0; i < evaluatedBoundaries.size() - 1; i++) {
                        buckets.put(evaluatedBoundaries.get(i), new ArrayList<>());
                    }
                    if (evaluatedDefaultBucket != null) {
                        buckets.put(evaluatedDefaultBucket, new ArrayList<>());
                    }

                    // Assign documents to buckets
                    for (Map<String, Object> doc : data) {
                        Object groupValue;

                        if (groupByObj instanceof Expr) {
                            groupValue = ((Expr) groupByObj).evaluate(doc);
                        } else if (groupByObj instanceof String) {
                            String fieldName = groupByObj.toString();
                            if (fieldName.startsWith("$")) {
                                fieldName = fieldName.substring(1);
                            }
                            groupValue = doc.get(fieldName);
                        } else {
                            groupValue = groupByObj;
                        }

                        // Find the appropriate bucket
                        boolean assigned = false;
                        for (int i = 0; i < evaluatedBoundaries.size() - 1; i++) {
                            Object lowerBound = evaluatedBoundaries.get(i);
                            Object upperBound = evaluatedBoundaries.get(i + 1);

                            if (compareValues(groupValue, lowerBound) >= 0 &&
                                    compareValues(groupValue, upperBound) < 0) {
                                buckets.get(lowerBound).add(doc);
                                assigned = true;
                                break;
                            }
                        }

                        // Assign to default bucket if no match
                        if (!assigned && evaluatedDefaultBucket != null) {
                            buckets.get(evaluatedDefaultBucket).add(doc);
                        }
                    }

                    // Generate bucket results
                    ret = new ArrayList<>();
                    for (Map.Entry<Object, List<Map<String, Object>>> bucketEntry : buckets.entrySet()) {
                        if (!bucketEntry.getValue().isEmpty()) {
                            Map<String, Object> bucketResult = new HashMap<>();
                            bucketResult.put("_id", bucketEntry.getKey());

                            // Apply output specifications
                            if (outputSpec != null) {
                                for (Map.Entry<String, Object> outputEntry : outputSpec.entrySet()) {
                                    String outputField = outputEntry.getKey();
                                    Object outputExpr = outputEntry.getValue();

                                    Object result = computeGroupValue(outputExpr, bucketEntry.getValue());
                                    bucketResult.put(outputField, result);
                                }
                            } else {
                                // Default: add count field when no output specification is provided
                                bucketResult.put("count", bucketEntry.getValue().size());
                            }

                            ret.add(bucketResult);
                        }
                    }
                }
                break;

            case "$bucketAuto":
                op = step.get(stage);

                if (op instanceof Map) {
                    Map<String, Object> bucketAutoParams = (Map<String, Object>) op;
                    Object groupByObj = bucketAutoParams.get("groupBy");
                    Integer numBuckets = (Integer) bucketAutoParams.get("buckets");
                    Map<String, Object> outputSpec = (Map<String, Object>) bucketAutoParams.get("output");

                    if (numBuckets == null) numBuckets = 5; // Default

                    // Extract values for sorting and bucket creation
                    List<BucketValue> entries = new ArrayList<>();

                    for (Map<String, Object> doc : data) {
                        Object value;
                        if (groupByObj instanceof Expr) {
                            value = ((Expr) groupByObj).evaluate(doc);
                        } else if (groupByObj instanceof String) {
                            String fieldName = groupByObj.toString();
                            if (fieldName.startsWith("$")) {
                                fieldName = fieldName.substring(1);
                            }
                            value = doc.get(fieldName);
                        } else {
                            value = groupByObj;
                        }

                        if (value != null) {
                            entries.add(new BucketValue(value, doc));
                        }
                    }

                    // Sort values
                    entries.sort((a, b) -> compareValues(a.value, b.value));

                    // Create auto buckets with equal distribution
                    ret = new ArrayList<>();

                    if (!entries.isEmpty()) {
                        int totalDocs = entries.size();
                        int docsPerBucket = totalDocs / numBuckets;
                        int remainder = totalDocs % numBuckets;

                        int currentIndex = 0;
                        Object lastMaxValue = null;

                        for (int bucketNum = 0; bucketNum < numBuckets && currentIndex < totalDocs; bucketNum++) {
                            // Calculate bucket size (distribute remainder evenly across first buckets)
                            int currentBucketSize = docsPerBucket + (bucketNum < remainder ? 1 : 0);
                            int endIndex = Math.min(currentIndex + currentBucketSize, totalDocs);

                            if (bucketNum == numBuckets - 1) {
                                endIndex = totalDocs; // Last bucket gets all remaining documents
                            }

                            List<Map<String, Object>> bucketDocs = new ArrayList<>();
                            Object minValue = (lastMaxValue != null) ? lastMaxValue : entries.get(currentIndex).value;
                            Object maxValue = entries.get(endIndex - 1).value;

                            for (int j = currentIndex; j < endIndex; j++) {
                                bucketDocs.add(entries.get(j).doc);
                            }

                            if (!bucketDocs.isEmpty()) {
                                Map<String, Object> bucketResult = new HashMap<>();
                                Map<String, Object> idRange = new HashMap<>();
                                idRange.put("min", minValue);
                                idRange.put("max", maxValue);
                                bucketResult.put("_id", idRange);

                                // Apply output specifications
                                if (outputSpec != null) {
                                    for (Map.Entry<String, Object> outputEntry : outputSpec.entrySet()) {
                                        String outputField = outputEntry.getKey();
                                        Object outputExpr = outputEntry.getValue();

                                        Object result = computeGroupValue(outputExpr, bucketDocs);
                                        bucketResult.put(outputField, result);
                                    }
                                } else {
                                    // Default: add count field when no output specification is provided
                                    bucketResult.put("count", bucketDocs.size());
                                }

                                ret.add(bucketResult);
                                lastMaxValue = maxValue;
                            }

                            currentIndex = endIndex;
                        }
                    }
                }
                break;

            case "$sortByCount":
                op = step.get(stage);
                Map<Object, Integer> counts = new HashMap<>();

                // Count occurrences
                for (Map<String, Object> doc : data) {
                    Object value;
                    if (op instanceof Expr) {
                        value = ((Expr) op).evaluate(doc);
                    } else if (op instanceof String) {
                        String fieldName = op.toString();
                        if (fieldName.startsWith("$")) {
                            fieldName = fieldName.substring(1);
                        }
                        value = doc.get(fieldName);
                    } else {
                        value = op;
                    }

                    counts.put(value, counts.getOrDefault(value, 0) + 1);
                }

                // Sort by count (descending)
                List<Map<String, Object>> sortByCountResult = new ArrayList<>();
                counts.entrySet().stream()
                      .sorted((a, b) -> b.getValue().compareTo(a.getValue()))
                .forEach(entry -> {
                    Map<String, Object> result = new HashMap<>();
                    result.put("_id", entry.getKey());
                    result.put("count", entry.getValue());
                    sortByCountResult.add(result);
                });
                ret = sortByCountResult;
                break;

            case "$graphLookup":
                op = step.get(stage);

                if (op instanceof Map) {
                    Map<String, Object> graphParams = (Map<String, Object>) op;
                    String fromCollection = (String) graphParams.get("from");
                    Object startWithObj = graphParams.get("startWith");
                    String connectFromField = (String) graphParams.get("connectFromField");
                    String connectToField = (String) graphParams.get("connectToField");
                    String asField = (String) graphParams.get("as");
                    Integer maxDepth = (Integer) graphParams.get("maxDepth");

                    // Get the foreign collection data
                    InMemoryDriver inMemDriver = (InMemoryDriver) morphium.getDriver();
                    Map<String, List<Map<String, Object>>> database = inMemDriver.getDatabase(morphium.getConfig().connectionSettings().getDatabase());
                    List<Map<String, Object>> foreignCollection = database.get(fromCollection);

                    if (foreignCollection != null) {
                        for (Map<String, Object> doc : data) {
                            List<Map<String, Object>> graphResults = new ArrayList<>();

                            // Get start value
                            Object startValue;
                            if (startWithObj instanceof Expr) {
                                startValue = ((Expr) startWithObj).evaluate(doc);
                            } else if (startWithObj instanceof String) {
                                String fieldName = startWithObj.toString();
                                if (fieldName.startsWith("$")) {
                                    fieldName = fieldName.substring(1);
                                }
                                startValue = doc.get(fieldName);
                            } else {
                                startValue = startWithObj;
                            }

                            // Perform recursive lookup
                            Set<Object> visited = new HashSet<>();
                            Queue<Object> toVisit = new LinkedList<>();
                            toVisit.add(normalizeGraphValue(startValue));

                            int currentDepth = 0;
                            while (!toVisit.isEmpty() && (maxDepth == null || currentDepth < maxDepth)) {
                                Queue<Object> nextLevel = new LinkedList<>();

                                while (!toVisit.isEmpty()) {
                                    Object searchValue = toVisit.poll();
                                    if (visited.contains(searchValue)) continue;
                                    visited.add(searchValue);

                                    // Find matching documents
                                    for (Map<String, Object> foreignDoc : foreignCollection) {
                                        Object connectFromValue = foreignDoc.get(connectFromField);
                                        if (graphValuesEqual(searchValue, connectFromValue)) {
                                            graphResults.add(new HashMap<>(foreignDoc));

                                            // Add connectFromField value for next level
                                            Object connectToValue = foreignDoc.get(connectToField);
                                            if (connectToValue != null) {
                                                Object normalized = normalizeGraphValue(connectToValue);
                                                if (!visited.contains(normalized)) {
                                                    nextLevel.add(normalized);
                                                }
                                            }
                                        }
                                    }
                                }

                                toVisit = nextLevel;
                                currentDepth++;
                            }

                            // Add results to document
                            doc.put(asField, graphResults);
                        }
                    }

                    ret = new ArrayList<>(data);
                }
                break;

            case "$geoNear":
                op = step.get(stage);
                if (op instanceof Map) {
                    Map<String, Object> geoNearParams = (Map<String, Object>) op;
                    Object nearPoint = geoNearParams.get("near");
                    String keyField = (String) geoNearParams.get("key");
                    String distanceField = (String) geoNearParams.get("distanceField");
                    Map<String, Object> query = (Map<String, Object>) geoNearParams.get("query");

                    List<Map<String, Object>> geoResults = new ArrayList<>();

                    for (Map<String, Object> doc : data) {
                        // Apply query filter if provided
                        if (query != null && !QueryHelper.matchesQuery(query, doc, null)) {
                            continue;
                        }

                        // Calculate distance
                        Double distance = calculateDistance(doc, nearPoint, keyField);
                        if (distance != null) {
                            // Create a copy of the document and add distance field
                            Map<String, Object> resultDoc = new LinkedHashMap<>(doc);
                            setNestedValue(resultDoc, distanceField, distance);
                            geoResults.add(resultDoc);
                        }
                    }

                    // Sort by distance (closest first)
                    geoResults.sort((a, b) -> {
                        Double distA = getNestedValue(a, distanceField);
                        Double distB = getNestedValue(b, distanceField);
                        return Double.compare(distA != null ? distA : Double.MAX_VALUE,
                                             distB != null ? distB : Double.MAX_VALUE);
                    });

                    ret = geoResults;
                }
                break;
            case "$documents":
                ret = stageDocuments(step.get(stage));
                break;

            case "$densify":
                ret = stageDensify(asSpecMap(stage, step.get(stage)), data);
                break;

            case "$fill":
                ret = stageFill(asSpecMap(stage, step.get(stage)), data);
                break;

            case "$setWindowFields":
                ret = stageSetWindowFields(asSpecMap(stage, step.get(stage)), data);
                break;

            case "$out":
                ret = stageOut(step.get(stage), data);
                break;

            case "$collStats":
                ret = stageCollStats(asSpecMap(stage, step.get(stage)));
                break;

            case "$listSessions":
                // The in-memory driver keeps no server-side session catalogue, so an empty result
                // set is the honest answer (a real mongod without sessions reports the same).
                ret = new ArrayList<>();
                break;

            case "$currentOp":
                // db-level stage (mongosh's db.currentOp()). The in-memory driver executes
                // commands synchronously on the caller's thread, so there is never a concurrent
                // op to report - empty is the honest answer. PoppyDB answers this stage
                // server-side from its live op registry before it ever reaches this aggregator.
                ret = new ArrayList<>();
                break;

            // $indexStats previously shared $geoNear's body and silently ran geoNear logic (#243).
            // It is not implemented by the in-memory driver, so it is grouped with the other
            // unimplemented stages here and surfaces as a proper command error instead.
            case "$indexStats":
            default:
                // Previously this only logged and fell through, returning whatever partial `ret`
                // had been built up (typically empty) as if the stage had produced no documents.
                // A real mongod rejects an unrecognized/unimplemented pipeline stage outright
                // (Unrecognized pipeline stage name, code 40324) - surface the same shape of
                // error here instead of silently completing with an empty/wrong result.
                throw mongoCommandError(40324, "Unrecognized pipeline stage name: '" + stage + "'");
        }

        return ret;
    }

    @Override
    public List<Map<String, Object>> aggregateMap() {
        return doAggregation();
    }

    private List<Map<String, Object>> doAggregation() {
        if (getMorphium() == null) {
            throw new IllegalStateException("Morphium not set");
        }

        Query<?> q;
        // Special handling for Map-based aggregations: there is no @Entity, so creating a typed Query would fail.
        // Use a map query and bind it to the explicit collection name instead.
        if (Map.class.equals(getSearchType()) || getMorphium().getARHelper().getAnnotationFromHierarchy(getSearchType(), Entity.class) == null) {
            q = getMorphium().createMapQuery(getCollectionName());
        } else {
            q = getMorphium().createQueryFor(getSearchType());
            if (getCollectionName() != null) {
                q.setCollectionName(getCollectionName());
            }
        }

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = (List<Map<String, Object>>) (List<?>) q.asMapList();

        for (Map<String, Object> step : getPipeline()) {
            //evaluate each step
            result = execStep(step, result);
        }

        return result;
    }

    private Object normalizeGraphValue(Object value) {
        if (value instanceof Number num) {
            return num.doubleValue();
        }

        return value;
    }

    private boolean graphValuesEqual(Object a, Object b) {
        if (a == null || b == null) {
            return Objects.equals(a, b);
        }

        if (a instanceof Number && b instanceof Number) {
            return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue()) == 0;
        }

        return Objects.equals(a, b);
    }

    @Override
    public Map<String, Object> explain() throws MorphiumDriverException {
        return explain(null);
    }

    @Override
    public Map<String, Object> explain(ExplainVerbosity verbosity) throws MorphiumDriverException {
        return Doc.of("err", "not supported with inMemDriver", "ok", 0.0);
    }

    /**
     * Helper method to compare values for bucket assignment
     */
    private int compareValues(Object value1, Object value2) {
        if (value1 == null && value2 == null) return 0;
        if (value1 == null) return -1;
        if (value2 == null) return 1;

        if (value1 instanceof Number && value2 instanceof Number) {
            double d1 = ((Number) value1).doubleValue();
            double d2 = ((Number) value2).doubleValue();
            return Double.compare(d1, d2);
        }

        if (value1 instanceof String && value2 instanceof String) {
            return ((String) value1).compareTo((String) value2);
        }

        // Fallback to string comparison
        return value1.toString().compareTo(value2.toString());
    }

    private static final class BucketValue {
        final Object value;
        final Map<String, Object> doc;

        BucketValue(Object value, Map<String, Object> doc) {
            this.value = value;
            this.doc = doc;
        }
    }

    /**
     * Helper method to extract group key from group specification
     */
    @SuppressWarnings("unchecked")
    private Object extractGroupKey(Object groupIdSpec, Map<String, Object> doc) {
        if (groupIdSpec == null) {
            return null;
        }

        if (groupIdSpec instanceof String) {
            String fieldName = groupIdSpec.toString();
            if (fieldName.startsWith("$")) {
                fieldName = fieldName.substring(1);
            }
            return doc.get(fieldName);
        }

        if (groupIdSpec instanceof Expr) {
            return ((Expr) groupIdSpec).evaluate(doc);
        }

        if (groupIdSpec instanceof Map) {
            Map<String, Object> result = new HashMap<>();
            Map<String, Object> groupMap = (Map<String, Object>) groupIdSpec;

            for (Map.Entry<String, Object> entry : groupMap.entrySet()) {
                Object value = extractGroupKey(entry.getValue(), doc);
                result.put(entry.getKey(), value);
            }
            return result;
        }

        return groupIdSpec;
    }

    /**
     * True if a $project spec value is an inclusion flag (1 / true / a ValueExpr evaluating to 1) -
     * i.e. "keep this field", as opposed to an exclusion (0) or a computed expression. See #240.
     */
    private boolean isProjectInclusionFlag(Object v) {
        if (v instanceof Boolean) {
            return (Boolean) v;
        }
        if (v instanceof Number) {
            return ((Number) v).doubleValue() != 0.0;
        }
        if (v instanceof Expr.ValueExpr) {
            Object e = ((Expr) v).evaluate(new HashMap<>());
            if (e instanceof Boolean) {
                return (Boolean) e;
            }
            return e instanceof Number && ((Number) e).doubleValue() != 0.0;
        }
        return false;
    }

    /**
     * True if a $project spec value is an exclusion flag (0 / false / a ValueExpr evaluating to 0).
     */
    private boolean isProjectExclusionFlag(Object v) {
        if (v instanceof Boolean) {
            return !((Boolean) v);
        }
        if (v instanceof Number) {
            return ((Number) v).doubleValue() == 0.0;
        }
        if (v instanceof Expr.ValueExpr) {
            Object e = ((Expr) v).evaluate(new HashMap<>());
            if (e instanceof Boolean) {
                return !((Boolean) e);
            }
            return e instanceof Number && ((Number) e).doubleValue() == 0.0;
        }
        return false;
    }

    /**
     * Evaluates a non-flag $project value (field reference "$x", an {@link Expr}, or a raw
     * expression map) against a source document, for strict inclusion mode (#240).
     */
    private Object projectComputedValue(Object value, Map<String, Object> o) {
        if (value instanceof String && ((String) value).startsWith("$")) {
            return getByPath(o, ((String) value).substring(1));
        }
        if (value instanceof Expr) {
            return ((Expr) value).evaluate(o);
        }
        if (value instanceof Map) {
            return Expr.parse(value).evaluate(o);
        }
        return value;
    }

    // ---- $merge whenMatched pipeline (#241) ----------------------------------------------
    //
    // mongod only allows these stages inside a whenMatched update pipeline. They are executed
    // by a small dedicated executor below instead of reusing the execStep() implementations:
    // those evaluate expressions against the bare document, and the pipeline variables ($$new
    // resp. the `let` bindings) could only be smuggled in by writing them into the document
    // itself - which would leak them into the stored result.
    private static final Set<String> MERGE_PIPELINE_STAGES =
        Set.of("$addFields", "$set", "$project", "$unset", "$replaceRoot", "$replaceWith");

    // system variables that are always defined during expression evaluation
    private static final Set<String> MERGE_BUILTIN_VARIABLES = Set.of("ROOT", "CURRENT", "REMOVE", "NOW");

    // sentinel bound to $$REMOVE: a field whose expression evaluates to it is dropped
    private static final Object MERGE_REMOVE_SENTINEL = new Object();

    /** Validates a whenMatched pipeline: stage whitelist plus every $$variable being defined. */
    private List<Map<String, Object>> parseMergeWhenMatchedPipeline(List<?> rawPipeline, Object letSpec) {
        if (letSpec != null && !(letSpec instanceof Map)) {
            throw mongoCommandError(51198, "$merge: let must be a document");
        }

        List<Map<String, Object>> pipeline = new ArrayList<>();

        for (Object stageObj : rawPipeline) {
            Object stageVal = stageObj instanceof Expr ? ((Expr) stageObj).toQueryObject() : stageObj;

            if (!(stageVal instanceof Map) || ((Map<?, ?>) stageVal).size() != 1) {
                throw mongoCommandError(72,
                    "$merge whenMatched pipeline: each stage must be a document with exactly one stage name");
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> stageMap = (Map<String, Object>) stageVal;
            String stageName = stageMap.keySet().iterator().next();

            if (!MERGE_PIPELINE_STAGES.contains(stageName)) {
                throw mongoCommandError(72, "$merge whenMatched pipeline: unsupported stage '" + stageName + "'");
            }

            pipeline.add(stageMap);
        }

        // With a custom `let`, the default {new: "$$ROOT"} is REPLACED, not extended - $$new is
        // then only defined if `let` defines it itself (mongod behaviour). Fail up front instead
        // of silently evaluating undefined variables to null.
        Set<String> defined = new HashSet<>(MERGE_BUILTIN_VARIABLES);

        if (letSpec == null) {
            defined.add("new");
        } else {
            for (Object k : ((Map<?, ?>) letSpec).keySet()) {
                defined.add(String.valueOf(k));
            }
        }

        Set<String> referenced = new HashSet<>();
        collectVariableReferences(pipeline, referenced);
        for (String var : referenced) {
            if (!defined.contains(var)) {
                // 17276 is mongod's "Use of undefined variable" code
                throw mongoCommandError(17276, "$merge whenMatched pipeline: use of undefined variable: " + var);
            }
        }

        return pipeline;
    }

    /** Collects the names of all "$$var" references in an expression tree (best effort - a
     * string literal that merely looks like a variable reference is indistinguishable here). */
    private void collectVariableReferences(Object node, Set<String> out) {
        if (node instanceof Expr) {
            node = ((Expr) node).toQueryObject();
        }

        if (node instanceof String s) {
            if (s.startsWith("$$")) {
                String name = s.substring(2);
                int dot = name.indexOf('.');
                out.add(dot > 0 ? name.substring(0, dot) : name);
            }
        } else if (node instanceof Map) {
            for (Object v : ((Map<?, ?>) node).values()) {
                collectVariableReferences(v, out);
            }
        } else if (node instanceof List) {
            for (Object v : (List<?>) node) {
                collectVariableReferences(v, out);
            }
        }
    }

    /** Evaluates the `let` expressions against the INCOMING document (default: {new: "$$ROOT"})
     * and returns the variable bindings in both spellings ("x" and "$x", see Expr.map). */
    @SuppressWarnings("unchecked")
    private Map<String, Object> bindMergeLetVariables(Object letSpec, Map<String, Object> incoming) {
        Map<String, Object> vars = new HashMap<>();

        if (letSpec == null) {
            vars.put("new", incoming);
            vars.put("$new", incoming);
            return vars;
        }

        // "$$ROOT" inside a let expression refers to the incoming document
        Map<String, Object> letContext = new HashMap<>(incoming);
        letContext.put("ROOT", incoming);
        letContext.put("$ROOT", incoming);
        letContext.put("CURRENT", incoming);
        letContext.put("$CURRENT", incoming);

        for (Map.Entry<String, Object> e : ((Map<String, Object>) letSpec).entrySet()) {
            Object value = e.getValue();
            Object bound;

            if (value instanceof Expr) {
                bound = ((Expr) value).evaluate(letContext);
            } else if (value instanceof String s && s.startsWith("$")) {
                // handles both "$field.path" and "$$ROOT" style references
                bound = Expr.field(s).evaluate(letContext);
            } else if (value instanceof Map) {
                bound = Expr.parse(value).evaluate(letContext);
            } else {
                bound = value;
            }

            vars.put(e.getKey(), bound);
            vars.put("$" + e.getKey(), bound);
        }

        return vars;
    }

    /** Runs a validated whenMatched pipeline on the existing target document. */
    private Map<String, Object> applyMergeWhenMatchedPipeline(List<Map<String, Object>> pipeline,
        Map<String, Object> existing, Map<String, Object> vars) {
        Map<String, Object> current = new HashMap<>(existing);

        for (Map<String, Object> stageMap : pipeline) {
            String stageName = stageMap.keySet().iterator().next();
            Object body = stageMap.get(stageName);

            switch (stageName) {
                case "$set":
                case "$addFields": {
                    if (body instanceof Expr) {
                        body = ((Expr) body).toQueryObject();
                    }

                    if (!(body instanceof Map)) {
                        throw mongoCommandError(40272, "the " + stageName + " stage specification must be an object");
                    }

                    Map<String, Object> next = new HashMap<>(current);

                    //noinspection unchecked
                    for (Map.Entry<String, Object> e : ((Map<String, Object>) body).entrySet()) {
                        Object v = evaluateMergeExpression(e.getValue(), current, vars);

                        if (v == MERGE_REMOVE_SENTINEL) {
                            next.remove(e.getKey());
                        } else if (e.getKey().contains(".")) {
                            setNestedValue(next, e.getKey(), v);
                        } else {
                            next.put(e.getKey(), v);
                        }
                    }

                    current = next;
                    break;
                }

                case "$unset": {
                    if (body instanceof Expr) {
                        body = ((Expr) body).toQueryObject();
                    }

                    List<String> fields = new ArrayList<>();

                    if (body instanceof List) {
                        for (Object f : (List<?>) body) {
                            fields.add(String.valueOf(f));
                        }
                    } else {
                        fields.add(String.valueOf(body));
                    }

                    current = new HashMap<>(current);

                    for (String f : fields) {
                        removeByPath(current, f);
                    }

                    break;
                }

                case "$project": {
                    if (body instanceof Expr) {
                        body = ((Expr) body).toQueryObject();
                    }

                    if (!(body instanceof Map)) {
                        throw mongoCommandError(40272, "the $project stage specification must be an object");
                    }

                    //noinspection unchecked
                    current = applyMergeProjection((Map<String, Object>) body, current, vars);
                    break;
                }

                case "$replaceRoot":
                case "$replaceWith": {
                    Object rootExpr = body;

                    if ("$replaceRoot".equals(stageName)) {
                        if (body instanceof Expr) {
                            body = ((Expr) body).toQueryObject();
                        }

                        if (!(body instanceof Map) || !((Map<?, ?>) body).containsKey("newRoot")) {
                            throw mongoCommandError(40414, "$replaceRoot requires a 'newRoot' expression");
                        }

                        rootExpr = ((Map<?, ?>) body).get("newRoot");
                    }

                    Object result = evaluateMergeExpression(rootExpr, current, vars);

                    if (!(result instanceof Map)) {
                        throw mongoCommandError(40228, "'newRoot' expression must evaluate to an object");
                    }

                    //noinspection unchecked
                    current = new HashMap<>((Map<String, Object>) result);
                    break;
                }

                default:
                    // unreachable - the pipeline was validated up front
                    throw mongoCommandError(72, "$merge whenMatched pipeline: unsupported stage '" + stageName + "'");
            }
        }

        return current;
    }

    /** $project inside a whenMatched pipeline, mirroring the strict/lenient split of the
     * regular $project stage but evaluating computed values with the pipeline variables. */
    private Map<String, Object> applyMergeProjection(Map<String, Object> op, Map<String, Object> current,
        Map<String, Object> vars) {
        boolean strictInclusion = false;

        for (Map.Entry<String, Object> e : op.entrySet()) {
            if (!e.getKey().equals("_id") && isProjectInclusionFlag(e.getValue())) {
                strictInclusion = true;
                break;
            }
        }

        Map<String, Object> obj;

        if (strictInclusion) {
            obj = new HashMap<>();

            if (!(op.containsKey("_id") && isProjectExclusionFlag(op.get("_id"))) && current.containsKey("_id")) {
                obj.put("_id", current.get("_id"));
            }

            for (Map.Entry<String, Object> e : op.entrySet()) {
                String k = e.getKey();
                Object value = e.getValue();

                if (k.equals("_id")) {
                    if (!isProjectInclusionFlag(value) && !isProjectExclusionFlag(value)) {
                        obj.put(k, evaluateMergeExpression(value, current, vars));
                    }

                    continue;
                }

                if (isProjectInclusionFlag(value)) {
                    Object v = getByPath(current, k);

                    if (v != null || current.containsKey(k)) {
                        obj.put(k, v);
                    }
                } else if (isProjectExclusionFlag(value)) {
                    // exclusions mixed into an inclusion projection are ignored (except _id)
                } else {
                    Object v = evaluateMergeExpression(value, current, vars);

                    if (v != MERGE_REMOVE_SENTINEL) {
                        obj.put(k, v);
                    }
                }
            }
        } else {
            obj = new HashMap<>(current);

            for (Map.Entry<String, Object> e : op.entrySet()) {
                if (isProjectExclusionFlag(e.getValue())) {
                    obj.remove(e.getKey());
                } else {
                    Object v = evaluateMergeExpression(e.getValue(), current, vars);

                    if (v == MERGE_REMOVE_SENTINEL) {
                        obj.remove(e.getKey());
                    } else {
                        obj.put(e.getKey(), v);
                    }
                }
            }
        }

        return obj;
    }

    /** Evaluates one expression against the current intermediate document with the pipeline
     * variables bound in both spellings (see Expr.map: "$$x" resolves via the context key "$x"). */
    private Object evaluateMergeExpression(Object value, Map<String, Object> current, Map<String, Object> vars) {
        Map<String, Object> ctx = new HashMap<>(current);
        ctx.put("ROOT", current);
        ctx.put("$ROOT", current);
        ctx.put("CURRENT", current);
        ctx.put("$CURRENT", current);
        ctx.put("REMOVE", MERGE_REMOVE_SENTINEL);
        ctx.put("$REMOVE", MERGE_REMOVE_SENTINEL);
        ctx.put("NOW", new Date());
        ctx.put("$NOW", ctx.get("NOW"));
        ctx.putAll(vars);

        if (value instanceof Expr) {
            return ((Expr) value).evaluate(ctx);
        }

        if (value instanceof String s && s.startsWith("$")) {
            // handles field paths ("$a.b") and variable references ("$$new.a") alike
            return Expr.field(s).evaluate(ctx);
        }

        if (value instanceof Map) {
            return Expr.parse(value).evaluate(ctx);
        }

        return value;
    }

    /** Removes a (possibly dotted) path from a document, copying nested maps on the way. */
    private void removeByPath(Map<String, Object> doc, String path) {
        int dot = path.indexOf('.');

        if (dot < 0) {
            doc.remove(path);
            return;
        }

        String head = path.substring(0, dot);

        if (doc.get(head) instanceof Map) {
            //noinspection unchecked
            Map<String, Object> sub = new HashMap<>((Map<String, Object>) doc.get(head));
            removeByPath(sub, path.substring(dot + 1));
            doc.put(head, sub);
        }
    }

    /**
     * Helper method to compute group values for aggregation operations like $sum, $avg, etc.
     */
    @SuppressWarnings("unchecked")
    private Object computeGroupValue(Object valueSpec, List<Map<String, Object>> groupDocs) {
        if (valueSpec instanceof Expr) {
            // For Expr objects, convert to Map representation and handle as Map
            Expr expr = (Expr) valueSpec;
            Object queryObj = expr.toQueryObject();

            if (queryObj instanceof Map) {
                return computeGroupValue(queryObj, groupDocs);
            }

            // Fallback: Check if it's a simple integer (likely a count)
            Object exprValue = expr.evaluate(new HashMap<>());
            if (exprValue instanceof Number && ((Number) exprValue).intValue() == 1) {
                return groupDocs.size(); // Count
            }

            // Otherwise, assume it's a field reference and compute average
            double sum = 0;
            int count = 0;
            for (Map<String, Object> doc : groupDocs) {
                Object result = expr.evaluate(doc);
                if (result instanceof Number) {
                    sum += ((Number) result).doubleValue();
                    count++;
                }
            }
            return count > 0 ? sum / count : 0;
        } else if (valueSpec instanceof Map) {
            Map<String, Object> specMap = (Map<String, Object>) valueSpec;
            String operation = specMap.keySet().iterator().next();
            Object operand = specMap.get(operation);

            switch (operation) {
                case "$sum":
                    double sum = 0;
                    for (Map<String, Object> doc : groupDocs) {
                        Object value = extractValue(operand, doc);
                        if (value instanceof Number) {
                            sum += ((Number) value).doubleValue();
                        } else if (value != null && isNumeric(value.toString())) {
                            sum += Double.parseDouble(value.toString());
                        } else if (value == null || value.equals(1)) {
                            sum += 1; // For counting
                        }
                    }
                    return sum;

                case "$avg":
                    double total = 0;
                    int count = 0;
                    for (Map<String, Object> doc : groupDocs) {
                        Object value = extractValue(operand, doc);
                        if (value instanceof Number) {
                            total += ((Number) value).doubleValue();
                            count++;
                        }
                    }
                    return count > 0 ? total / count : 0;

                case "$min":
                    Double min = null;
                    for (Map<String, Object> doc : groupDocs) {
                        Object value = extractValue(operand, doc);
                        if (value instanceof Number) {
                            double d = ((Number) value).doubleValue();
                            if (min == null || d < min) {
                                min = d;
                            }
                        }
                    }
                    return min;

                case "$max":
                    Double max = null;
                    for (Map<String, Object> doc : groupDocs) {
                        Object value = extractValue(operand, doc);
                        if (value instanceof Number) {
                            double d = ((Number) value).doubleValue();
                            if (max == null || d > max) {
                                max = d;
                            }
                        }
                    }
                    return max;

                case "$first":
                    if (!groupDocs.isEmpty()) {
                        return extractValue(operand, groupDocs.get(0));
                    }
                    return null;

                case "$last":
                    if (!groupDocs.isEmpty()) {
                        return extractValue(operand, groupDocs.get(groupDocs.size() - 1));
                    }
                    return null;

                case "$push":
                    List<Object> pushList = new ArrayList<>();
                    for (Map<String, Object> doc : groupDocs) {
                        pushList.add(extractValue(operand, doc));
                    }
                    return pushList;

                case "$addToSet":
                    Set<Object> uniqueSet = new HashSet<>();
                    for (Map<String, Object> doc : groupDocs) {
                        uniqueSet.add(extractValue(operand, doc));
                    }
                    return new ArrayList<>(uniqueSet);

                default:
                    // An unrecognized accumulator operator in a $bucket/$bucketAuto output spec used to
                    // silently return null (a document with the output field missing/null). Real mongod
                    // reports this as "unknown group operator" (Location15952) - surface it the same way
                    // instead of swallowing it.
                    throw mongoCommandError(15952, "unknown group operator '" + operation + "'");
            }
        }

        return valueSpec;
    }

    /**
     * Helper method to extract value from document based on field reference or expression
     */
    @SuppressWarnings("unchecked")
    private Object extractValue(Object spec, Map<String, Object> doc) {
        if (spec instanceof String) {
            String fieldName = spec.toString();
            if (fieldName.startsWith("$")) {
                fieldName = fieldName.substring(1);
            }
            return doc.get(fieldName);
        }

        if (spec instanceof Expr) {
            return ((Expr) spec).evaluate(doc);
        }

        // Handle Map that contains expression structures (from mapExpr.toQueryObject())
        if (spec instanceof Map) {
            Map<String, Object> specMap = (Map<String, Object>) spec;

            // Create result map by evaluating each field
            Map<String, Object> result = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : specMap.entrySet()) {
                String fieldName = entry.getKey();
                Object fieldValue = entry.getValue();

                // If the field value is an expression structure, parse and evaluate it
                if (fieldValue instanceof Map) {
                    try {
                        Map<String, Object> exprMap = (Map<String, Object>) fieldValue;
                        Expr fieldExpr = Expr.parse(exprMap);
                        Object evaluatedValue = fieldExpr.evaluate(doc);
                        result.put(fieldName, evaluatedValue);
                    } catch (Exception e) {
                        // If evaluation fails, use the raw value
                        result.put(fieldName, fieldValue);
                    }
                } else if (fieldValue instanceof String && ((String) fieldValue).startsWith("$")) {
                    // Handle field references like "$year_born"
                    String fieldRef = ((String) fieldValue).substring(1);
                    Object fieldVal = doc.get(fieldRef);
                    result.put(fieldName, fieldVal);
                } else {
                    // Plain value, just copy
                    result.put(fieldName, fieldValue);
                }
            }

            return result;
        }

        return spec;
    }

    /**
     * Helper method to check if a string represents a number
     */
    private boolean isNumeric(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * Calculate distance between a document's location field and a target point
     */
    private Double calculateDistance(Map<String, Object> doc, Object nearPoint, String keyField) {
        Object locationValue = doc.get(keyField);
        if (locationValue == null || nearPoint == null) {
            return null;
        }

        try {
            double[] targetCoords = extractCoordinates(nearPoint);
            double[] docCoords = extractCoordinates(locationValue);

            if (targetCoords == null || docCoords == null) {
                return null;
            }

            // Use Haversine formula for distance calculation (same as in QueryHelper)
            double lon1 = Math.toRadians(targetCoords[0]);
            double lat1 = Math.toRadians(targetCoords[1]);
            double lon2 = Math.toRadians(docCoords[0]);
            double lat2 = Math.toRadians(docCoords[1]);

            double dlon = lon2 - lon1;
            double dlat = lat2 - lat1;
            double a = Math.pow(Math.sin(dlat / 2), 2) + Math.cos(lat1) * Math.cos(lat2) * Math.pow(Math.sin(dlon / 2), 2);
            double c = 2 * Math.asin(Math.sqrt(a));
            double r = 6371; // Earth's radius in kilometers
            return c * r;
        } catch (Exception e) {
            log.warn("Error calculating distance: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Extract coordinates from various point representations
     */
    @SuppressWarnings("unchecked")
    private double[] extractCoordinates(Object point) {
        if (point instanceof Map) {
            Map<String, Object> pointMap = (Map<String, Object>) point;

            // Handle GeoJSON Point format: {"type": "Point", "coordinates": [lon, lat]}
            if ("Point".equals(pointMap.get("type")) && pointMap.containsKey("coordinates")) {
                List<?> coords = (List<?>) pointMap.get("coordinates");
                if (coords != null && coords.size() >= 2) {
                    return new double[]{
                        ((Number) coords.get(0)).doubleValue(),
                        ((Number) coords.get(1)).doubleValue()
                    };
                }
            }
        } else if (point instanceof List) {
            // Handle array format: [lon, lat]
            List<?> coords = (List<?>) point;
            if (coords.size() >= 2) {
                return new double[]{
                    ((Number) coords.get(0)).doubleValue(),
                    ((Number) coords.get(1)).doubleValue()
                };
            }
        }
        return null;
    }

    /**
     * Set a nested value in a map using dot notation (e.g., "dist.calculated")
     */
    @SuppressWarnings("unchecked")
    private void setNestedValue(Map<String, Object> map, String path, Object value) {
        String[] parts = path.split("\\.");
        Map<String, Object> current = map;

        for (int i = 0; i < parts.length - 1; i++) {
            String part = parts[i];
            if (!current.containsKey(part)) {
                current.put(part, new LinkedHashMap<String, Object>());
            }
            current = (Map<String, Object>) current.get(part);
        }

        current.put(parts[parts.length - 1], value);
    }

    /**
     * Get a nested value from a map using dot notation (e.g., "dist.calculated")
     */
    @SuppressWarnings("unchecked")
    private Double getNestedValue(Map<String, Object> map, String path) {
        String[] parts = path.split("\\.");
        Object current = map;

        for (String part : parts) {
            if (current instanceof Map) {
                current = ((Map<String, Object>) current).get(part);
            } else {
                return null;
            }
        }

        return (current instanceof Number) ? ((Number) current).doubleValue() : null;
    }

    @SuppressWarnings("unchecked")
    private Object extractValueByPath(Map<String, Object> map, String path) {
        String[] parts = path.split("\\.");
        Object current = map;

        for (String part : parts) {
            if (current instanceof Map) {
                current = ((Map<String, Object>) current).get(part);
            } else {
                return null;
            }
        }

        return current;
    }

    /**
     * Substitute let variables in a pipeline stage
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> substituteLetVariables(Map<String, Object> stage, Map<String, Object> letContext) {
        Map<String, Object> result = new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry : stage.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (value instanceof String) {
                String strValue = (String) value;
                // Replace let variable references like $$order_item
                for (Map.Entry<String, Object> letEntry : letContext.entrySet()) {
                    if (strValue.equals(letEntry.getKey())) {
                        value = letEntry.getValue();
                        break;
                    }
                }
                result.put(key, value);
            } else if (value instanceof Map) {
                // Recursively substitute in nested maps
                result.put(key, substituteLetVariables((Map<String, Object>) value, letContext));
            } else if (value instanceof List) {
                // Substitute in lists
                List<Object> newList = new ArrayList<>();
                for (Object item : (List<?>) value) {
                    if (item instanceof Map) {
                        newList.add(substituteLetVariables((Map<String, Object>) item, letContext));
                    } else if (item instanceof String) {
                        String strItem = (String) item;
                        Object substituted = strItem;
                        for (Map.Entry<String, Object> letEntry : letContext.entrySet()) {
                            if (strItem.equals(letEntry.getKey())) {
                                substituted = letEntry.getValue();
                                break;
                            }
                        }
                        newList.add(substituted);
                    } else {
                        newList.add(item);
                    }
                }
                result.put(key, newList);
            } else {
                result.put(key, value);
            }
        }

        return result;
    }

    private static Object getByPath(Map<String, Object> doc, String path) {
        if (doc == null || path == null) {
            return null;
        }

        String[] parts = path.split("\\.");
        Object cur = doc;

        for (String p : parts) {
            if (!(cur instanceof Map)) {
                return null;
            }

            cur = ((Map) cur).get(p);

            if (cur == null) {
                return null;
            }
        }

        return cur;
    }

    // ---------------------------------------------------------------------------------------
    // #254: $documents / $densify / $fill / $setWindowFields / $out / $collStats
    // ---------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private Map<String, Object> asSpecMap(String stage, Object spec) {
        if (!(spec instanceof Map)) {
            throw mongoCommandError(40272, "the " + stage + " stage specification must be an object");
        }

        return (Map<String, Object>) spec;
    }

    /**
     * $documents: a literal document source. It replaces whatever the collection scan provided,
     * so pipelines can run without any backing collection (MongoDB restricts this stage to
     * db-level aggregations; the in-memory driver simply ignores the collection input instead).
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> stageDocuments(Object spec) {
        Object docs = spec instanceof Expr ? ((Expr) spec).evaluate(new HashMap<>()) : spec;

        if (!(docs instanceof List)) {
            throw mongoCommandError(5858203, "$documents must evaluate to an array of objects");
        }

        List<Map<String, Object>> ret = new ArrayList<>();

        for (Object entry : (List<?>) docs) {
            Object resolved = entry instanceof Expr ? ((Expr) entry).evaluate(new HashMap<>()) : entry;

            if (!(resolved instanceof Map)) {
                throw mongoCommandError(5858203, "$documents must evaluate to an array of objects");
            }

            Map<String, Object> doc = new LinkedHashMap<>();

            for (Map.Entry<String, Object> e : ((Map<String, Object>) resolved).entrySet()) {
                // literals built through the fluent API may carry Expr values - resolve them
                // without a document context (there is no input document to refer to)
                doc.put(e.getKey(), e.getValue() instanceof Expr ? ((Expr) e.getValue()).evaluate(new HashMap<>()) : e.getValue());
            }

            ret.add(doc);
        }

        return ret;
    }

    /**
     * $out: REPLACE the target collection with the pipeline result. Implemented as clear + write
     * through the driver's own primitives so index definitions, capped/TTL bookkeeping and
     * changestream watchers on the target stay intact (a drop would discard them). Terminal
     * stage - returns no documents.
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> stageOut(Object spec, List<Map<String, Object>> data) {
        String outDb = morphium.getConfig().connectionSettings().getDatabase();
        String outColl = null;

        if (spec instanceof Map) {
            Map<String, Object> m = (Map<String, Object>) spec;

            if (m.get("db") != null) {
                outDb = String.valueOf(m.get("db"));
            }

            outColl = m.get("coll") == null ? null : String.valueOf(m.get("coll"));
        } else if (spec != null) {
            outColl = String.valueOf(spec);
        }

        if (outColl == null || outColl.isEmpty()) {
            throw mongoCommandError(16994, "$out requires a target collection name");
        }

        InMemoryDriver drv = (InMemoryDriver) morphium.getDriver();
        drv.delete(outDb, outColl, new HashMap<>(), null, true, null, null);

        if (!data.isEmpty()) {
            List<Map<String, Object>> copies = new ArrayList<>();

            for (Map<String, Object> doc : data) {
                copies.add(new HashMap<>(doc));
            }

            drv.store(outDb, outColl, copies, null);
        }

        return new ArrayList<>();
    }

    /**
     * $collStats as a pipeline stage. Counts are real; byte-size fields are reported as 0 by the
     * same precedent as the dbStats command - the in-memory driver does not track document byte
     * sizes. Latency/queryExec counters are structurally present but zeroed (nothing is metered).
     */
    private List<Map<String, Object>> stageCollStats(Map<String, Object> spec) {
        Set<String> known = Set.of("latencyStats", "storageStats", "count", "queryExecStats");

        for (String k : spec.keySet()) {
            if (!known.contains(k)) {
                throw mongoCommandError(40415, "BSON field '$collStats." + k + "' is an unknown field.");
            }
        }

        String db = morphium.getConfig().connectionSettings().getDatabase();
        String coll = getCollectionName();
        InMemoryDriver drv = (InMemoryDriver) morphium.getDriver();
        long count = drv.find(db, coll, new HashMap<>(), null, null, 0, 0).size();

        Map<String, Object> statsDoc = new LinkedHashMap<>();
        statsDoc.put("ns", db + "." + coll);
        statsDoc.put("host", "inMem");
        statsDoc.put("localTime", new Date());

        if (spec.containsKey("latencyStats")) {
            Map<String, Object> latency = new LinkedHashMap<>();

            for (String op : List.of("reads", "writes", "commands", "transactions")) {
                latency.put(op, Doc.of("latency", 0L, "ops", 0L));
            }

            statsDoc.put("latencyStats", latency);
        }

        if (spec.containsKey("storageStats")) {
            // real BSON data size + estimated index size, same computation as dbStats/collStats
            long size = drv.collectionDataSize(db, coll);
            long indexSize = drv.estimatedIndexSize(db, coll, count);
            Map<String, Object> storage = new LinkedHashMap<>();
            storage.put("size", size);
            storage.put("count", count);
            storage.put("avgObjSize", count > 0 ? (double) size / count : 0.0);
            storage.put("storageSize", size);
            storage.put("freeStorageSize", 0);
            storage.put("capped", false);
            storage.put("totalIndexSize", indexSize);
            storage.put("totalSize", size + indexSize);
            statsDoc.put("storageStats", storage);
        }

        if (spec.containsKey("count")) {
            statsDoc.put("count", count);
        }

        if (spec.containsKey("queryExecStats")) {
            statsDoc.put("queryExecStats", Doc.of("collectionScans", Doc.of("total", 0L, "nonTailable", 0L)));
        }

        List<Map<String, Object>> ret = new ArrayList<>();
        ret.add(statsDoc);
        return ret;
    }

    // ---- $densify -------------------------------------------------------------------------

    private static final Map<String, Long> DENSIFY_UNIT_MILLIS = Map.of(
        "millisecond", 1L, "second", 1000L, "minute", 60000L, "hour", 3600000L,
        "day", 86400000L, "week", 604800000L);
    private static final Map<String, Integer> DENSIFY_UNIT_MONTHS = Map.of(
        "month", 1, "quarter", 3, "year", 12);
    // hard cap so a huge range/tiny step cannot OOM the JVM - real mongod streams, we materialize
    private static final int DENSIFY_MAX_GENERATED = 500_000;

    /**
     * $densify: fills gaps in a numeric or date sequence. Covers the documented core: explicit
     * [lower, upper) bounds, "partition" and "full" bounds, partitionByFields, and date steps
     * with a unit (fixed-length units plus month/quarter/year via calendar arithmetic).
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> stageDensify(Map<String, Object> spec, List<Map<String, Object>> data) {
        Object fieldSpec = spec.get("field");

        if (!(fieldSpec instanceof String) || ((String) fieldSpec).isEmpty()) {
            throw mongoCommandError(5733201, "$densify requires 'field' to be a non-empty string");
        }

        String field = (String) fieldSpec;

        if (!(spec.get("range") instanceof Map)) {
            throw mongoCommandError(5733201, "$densify requires a 'range' object");
        }

        Map<String, Object> range = (Map<String, Object>) spec.get("range");
        Object stepSpec = range.get("step");

        if (!(stepSpec instanceof Number) || ((Number) stepSpec).doubleValue() <= 0) {
            throw mongoCommandError(5733303, "$densify range.step must be a strictly positive number");
        }

        double step = ((Number) stepSpec).doubleValue();
        String unit = range.get("unit") == null ? null : String.valueOf(range.get("unit"));

        if (unit != null && !DENSIFY_UNIT_MILLIS.containsKey(unit) && !DENSIFY_UNIT_MONTHS.containsKey(unit)) {
            throw mongoCommandError(5733201, "$densify range.unit '" + unit + "' is not a valid time unit");
        }

        if (unit != null && DENSIFY_UNIT_MONTHS.containsKey(unit) && step != Math.rint(step)) {
            throw mongoCommandError(5733303, "$densify range.step must be a whole number for unit '" + unit + "'");
        }

        Object bounds = range.get("bounds");
        boolean fullBounds = "full".equals(bounds);
        boolean partitionBounds = "partition".equals(bounds);
        List<?> explicitBounds = bounds instanceof List ? (List<?>) bounds : null;

        if (!fullBounds && !partitionBounds && (explicitBounds == null || explicitBounds.size() != 2)) {
            throw mongoCommandError(5733201, "$densify range.bounds must be 'full', 'partition' or a [lower, upper] array");
        }

        List<String> partitionFields = new ArrayList<>();

        if (spec.get("partitionByFields") instanceof List) {
            for (Object p : (List<?>) spec.get("partitionByFields")) {
                partitionFields.add(String.valueOf(p));
            }
        }

        // determine the value kind (numeric vs date) from the data - it decides how to step
        boolean sawNumber = false;
        boolean sawDate = false;
        Object globalMin = null;
        Object globalMax = null;

        for (Map<String, Object> doc : data) {
            Object v = getByPath(doc, field);

            if (v == null) {
                continue;
            }

            if (v instanceof Number) {
                sawNumber = true;
            } else if (v instanceof Date) {
                sawDate = true;
            } else {
                throw mongoCommandError(5733201, "$densify field '" + field + "' must contain only numeric or date values");
            }

            if (globalMin == null || densifyCompare(v, globalMin) < 0) {
                globalMin = v;
            }

            if (globalMax == null || densifyCompare(v, globalMax) > 0) {
                globalMax = v;
            }
        }

        if (sawNumber && sawDate) {
            throw mongoCommandError(5733201, "$densify field '" + field + "' mixes numeric and date values");
        }

        if (explicitBounds != null) {
            for (Object b : explicitBounds) {
                if (b instanceof Number) {
                    sawNumber = true;
                } else if (b instanceof Date) {
                    sawDate = true;
                } else {
                    throw mongoCommandError(5733201, "$densify range.bounds values must be numeric or dates");
                }
            }

            if (sawNumber && sawDate) {
                throw mongoCommandError(5733201, "$densify range.bounds type must match the field type");
            }
        }

        if (sawNumber && unit != null) {
            throw mongoCommandError(5733201, "$densify range.unit is only valid for date values");
        }

        if (sawDate && unit == null) {
            throw mongoCommandError(5733201, "$densify on a date field requires range.unit");
        }

        // group into partitions, keeping encounter order
        Map<List<Object>, List<Map<String, Object>>> partitions = new LinkedHashMap<>();

        for (Map<String, Object> doc : data) {
            List<Object> key = new ArrayList<>();

            for (String pf : partitionFields) {
                key.add(canonPartitionValue(getByPath(doc, pf)));
            }

            partitions.computeIfAbsent(key, k -> new ArrayList<>()).add(doc);
        }

        if (partitions.isEmpty() && explicitBounds != null && partitionFields.isEmpty()) {
            // no input at all, but explicit bounds still define what to generate
            partitions.put(new ArrayList<>(), new ArrayList<>());
        }

        List<Map<String, Object>> ret = new ArrayList<>();
        int generatedTotal = 0;

        for (Map.Entry<List<Object>, List<Map<String, Object>>> part : partitions.entrySet()) {
            List<Map<String, Object>> docs = part.getValue();
            List<Map<String, Object>> withField = new ArrayList<>();
            Object min = null;
            Object max = null;

            for (Map<String, Object> doc : docs) {
                Object v = getByPath(doc, field);

                if (v == null) {
                    // docs without the densify field pass through untouched
                    ret.add(doc);
                    continue;
                }

                withField.add(doc);

                if (min == null || densifyCompare(v, min) < 0) {
                    min = v;
                }

                if (max == null || densifyCompare(v, max) > 0) {
                    max = v;
                }
            }

            Object lower;
            Object upper;
            boolean upperExclusive;

            if (explicitBounds != null) {
                lower = explicitBounds.get(0);
                upper = explicitBounds.get(1);
                upperExclusive = true;
            } else if (partitionBounds) {
                lower = min;
                upper = max;
                upperExclusive = false;
            } else { // full
                lower = globalMin;
                upper = globalMax;
                upperExclusive = false;
            }

            List<Map<String, Object>> merged = new ArrayList<>(withField);

            if (lower != null && upper != null) {
                Set<Object> existing = new HashSet<>();

                for (Map<String, Object> doc : withField) {
                    existing.add(canonPartitionValue(getByPath(doc, field)));
                }

                for (long k = 0; ; k++) {
                    Object value = densifyAdvance(lower, step, unit, k);
                    int cmp = densifyCompare(value, upper);

                    if (upperExclusive ? cmp >= 0 : cmp > 0) {
                        break;
                    }

                    if (!existing.contains(canonPartitionValue(value))) {
                        if (++generatedTotal > DENSIFY_MAX_GENERATED) {
                            throw mongoCommandError(5733201,
                                "$densify would generate more than " + DENSIFY_MAX_GENERATED
                                + " documents - refusing (range too large for the given step)");
                        }

                        Map<String, Object> gen = new LinkedHashMap<>();

                        for (int i = 0; i < partitionFields.size(); i++) {
                            setNestedValue(gen, partitionFields.get(i), part.getKey().get(i));
                        }

                        setNestedValue(gen, field, value);
                        merged.add(gen);
                    }
                }
            }

            merged.sort((a, b) -> densifyCompare(getByPath(a, field), getByPath(b, field)));
            ret.addAll(merged);
        }

        return ret;
    }

    private int densifyCompare(Object a, Object b) {
        if (a instanceof Number && b instanceof Number) {
            return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
        }

        if (a instanceof Date && b instanceof Date) {
            return Long.compare(((Date) a).getTime(), ((Date) b).getTime());
        }

        throw mongoCommandError(5733201, "$densify cannot compare values of different types");
    }

    /** lower + k*step, in the value domain: plain numbers, fixed-length time units or months. */
    private Object densifyAdvance(Object lower, double step, String unit, long k) {
        if (lower instanceof Number) {
            double v = ((Number) lower).doubleValue() + k * step;

            // keep whole-number progressions integral so generated values look like the input
            if (v == Math.rint(v) && !Double.isInfinite(v)) {
                return (long) v;
            }

            return v;
        }

        Date d = (Date) lower;

        if (DENSIFY_UNIT_MILLIS.containsKey(unit)) {
            return new Date(d.getTime() + (long)(k * step * DENSIFY_UNIT_MILLIS.get(unit)));
        }

        // month-based units have no fixed length - use calendar arithmetic in UTC
        long months = (long) step * DENSIFY_UNIT_MONTHS.get(unit) * k;
        return Date.from(java.time.ZonedDateTime.ofInstant(d.toInstant(), java.time.ZoneOffset.UTC)
                         .plusMonths(months).toInstant());
    }

    /** Normalizes values used as partition/set keys so 1, 1L and 1.0 land in the same bucket. */
    private Object canonPartitionValue(Object v) {
        if (v instanceof Number) {
            return ((Number) v).doubleValue();
        }

        if (v instanceof Date) {
            return ((Date) v).getTime();
        }

        return v;
    }

    // ---- $fill ----------------------------------------------------------------------------

    /**
     * $fill: fills null/missing values. Covers the documented core: "value" (an expression
     * evaluated against the document), method "locf" (last observed carried forward) and method
     * "linear" (interpolation along a single numeric sortBy field), with partitionBy/
     * partitionByFields and sortBy.
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> stageFill(Map<String, Object> spec, List<Map<String, Object>> data) {
        if (!(spec.get("output") instanceof Map) || ((Map<String, Object>) spec.get("output")).isEmpty()) {
            throw mongoCommandError(6050200, "$fill requires a non-empty 'output' object");
        }

        Map<String, Object> output = (Map<String, Object>) spec.get("output");
        Map<String, Object> sortBy = spec.get("sortBy") instanceof Map ? (Map<String, Object>) spec.get("sortBy") : null;

        for (Map.Entry<String, Object> e : output.entrySet()) {
            if (!(e.getValue() instanceof Map)) {
                throw mongoCommandError(6050200, "$fill output." + e.getKey() + " must be an object with 'value' or 'method'");
            }

            Map<String, Object> fieldSpec = (Map<String, Object>) e.getValue();
            boolean hasValue = fieldSpec.containsKey("value");
            Object method = fieldSpec.get("method");

            if (hasValue == (method != null)) {
                throw mongoCommandError(6050200, "$fill output." + e.getKey() + " must specify exactly one of 'value' or 'method'");
            }

            if (method != null) {
                if (!"locf".equals(method) && !"linear".equals(method)) {
                    throw mongoCommandError(6050200, "unknown $fill method '" + method + "' - only 'locf' and 'linear' are supported");
                }

                if (sortBy == null || sortBy.isEmpty()) {
                    throw mongoCommandError(6050201, "$fill method '" + method + "' requires sortBy");
                }

                if ("linear".equals(method) && sortBy.size() != 1) {
                    throw mongoCommandError(6050202, "$fill method 'linear' requires sortBy on exactly one field");
                }
            }
        }

        List<List<Map<String, Object>>> partitions =
            partitionForWindowing(data, spec.get("partitionBy"), (List<Object>) spec.get("partitionByFields"));
        List<Map<String, Object>> ret = new ArrayList<>();

        for (List<Map<String, Object>> partition : partitions) {
            List<Map<String, Object>> docs = new ArrayList<>();

            for (Map<String, Object> d : partition) {
                docs.add(new LinkedHashMap<>(d));
            }

            applySortBy(docs, sortBy);

            for (Map.Entry<String, Object> e : output.entrySet()) {
                String field = e.getKey();
                Map<String, Object> fieldSpec = (Map<String, Object>) e.getValue();

                if (fieldSpec.containsKey("value")) {
                    for (Map<String, Object> doc : docs) {
                        if (getByPath(doc, field) == null) {
                            setNestedValue(doc, field, projectComputedValue(fieldSpec.get("value"), doc));
                        }
                    }
                } else if ("locf".equals(fieldSpec.get("method"))) {
                    Object last = null;

                    for (Map<String, Object> doc : docs) {
                        Object v = getByPath(doc, field);

                        if (v != null) {
                            last = v;
                        } else if (last != null) {
                            setNestedValue(doc, field, last);
                        }
                        // nothing observed yet -> leading nulls stay null
                    }
                } else { // linear
                    fillLinear(docs, field, sortBy.keySet().iterator().next());
                }
            }

            ret.addAll(docs);
        }

        return ret;
    }

    /** Linear interpolation between the surrounding non-null values along the sort field. */
    private void fillLinear(List<Map<String, Object>> docs, String field, String sortField) {
        int prevKnown = -1;

        for (int i = 0; i < docs.size(); i++) {
            Object v = getByPath(docs.get(i), field);

            if (v == null) {
                continue;
            }

            if (!(v instanceof Number)) {
                throw mongoCommandError(6050202, "$fill method 'linear' requires numeric values in field '" + field + "'");
            }

            if (prevKnown >= 0 && i - prevKnown > 1) {
                double v0 = ((Number) getByPath(docs.get(prevKnown), field)).doubleValue();
                double v1 = ((Number) v).doubleValue();
                Object s0raw = getByPath(docs.get(prevKnown), sortField);
                Object s1raw = getByPath(docs.get(i), sortField);

                if (!(s0raw instanceof Number) || !(s1raw instanceof Number)) {
                    throw mongoCommandError(6050202, "$fill method 'linear' requires a numeric sortBy field");
                }

                double s0 = ((Number) s0raw).doubleValue();
                double s1 = ((Number) s1raw).doubleValue();

                for (int j = prevKnown + 1; j < i; j++) {
                    Object sjRaw = getByPath(docs.get(j), sortField);

                    if (!(sjRaw instanceof Number)) {
                        throw mongoCommandError(6050202, "$fill method 'linear' requires a numeric sortBy field");
                    }

                    double sj = ((Number) sjRaw).doubleValue();
                    // degenerate sort distance would divide by zero - fall back to the left value
                    double filled = s1 == s0 ? v0 : v0 + (v1 - v0) * (sj - s0) / (s1 - s0);
                    setNestedValue(docs.get(j), field, filled);
                }
            }

            prevKnown = i;
        }
        // nulls before the first / after the last known value stay null by definition
    }

    // ---- $setWindowFields -----------------------------------------------------------------

    private static final Set<String> SWF_ACCUMULATORS =
        Set.of("$sum", "$avg", "$min", "$max", "$count", "$push", "$first", "$last",
               "$stdDevPop", "$stdDevSamp", "$covariancePop", "$covarianceSamp",
               "$firstN", "$lastN", "$minN", "$maxN", "$top", "$bottom", "$topN", "$bottomN");
    private static final Set<String> SWF_RANK_FAMILY = Set.of("$rank", "$denseRank", "$documentNumber");

    /**
     * $setWindowFields: partitionBy, sortBy, documents- and range-windows (range with optional
     * time unit up to 'week'), and the window functions
     * $sum/$avg/$min/$max/$count/$push/$first/$last/$stdDevPop/$stdDevSamp/$covariancePop/
     * $covarianceSamp/$firstN/$lastN/$minN/$maxN/$top/$bottom/$topN/$bottomN (accumulator
     * family, documents and range windows), $rank/$denseRank/$documentNumber/$shift (need the
     * stage sortBy, no window), $derivative/$integral (single-field sortBy, optional unit),
     * and $expMovingAvg/$linearFill/$locf (partition-sequential, no window). Anything else
     * (e.g. $percentile/$median window state) is rejected with a command error - never
     * silently wrong results.
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> stageSetWindowFields(Map<String, Object> spec, List<Map<String, Object>> data) {
        if (!(spec.get("output") instanceof Map) || ((Map<String, Object>) spec.get("output")).isEmpty()) {
            throw mongoCommandError(5397900, "$setWindowFields requires a non-empty 'output' object");
        }

        Map<String, Object> output = (Map<String, Object>) spec.get("output");
        Map<String, Object> sortBy = spec.get("sortBy") instanceof Map ? (Map<String, Object>) spec.get("sortBy") : null;
        List<List<Map<String, Object>>> partitions = partitionForWindowing(data, spec.get("partitionBy"), null);
        List<Map<String, Object>> ret = new ArrayList<>();

        for (List<Map<String, Object>> partition : partitions) {
            List<Map<String, Object>> docs = new ArrayList<>();

            for (Map<String, Object> d : partition) {
                docs.add(new LinkedHashMap<>(d));
            }

            applySortBy(docs, sortBy);
            int n = docs.size();

            for (Map.Entry<String, Object> e : output.entrySet()) {
                String outField = e.getKey();

                if (!(e.getValue() instanceof Map)) {
                    throw mongoCommandError(5397900, "$setWindowFields output." + outField + " must be an object");
                }

                Map<String, Object> fieldSpec = (Map<String, Object>) e.getValue();
                String fn = null;

                for (String k : fieldSpec.keySet()) {
                    if ("window".equals(k)) {
                        continue;
                    }

                    if (fn != null) {
                        throw mongoCommandError(5397900,
                            "$setWindowFields output." + outField + " must contain exactly one window function");
                    }

                    fn = k;
                }

                if (fn == null) {
                    throw mongoCommandError(5397900,
                        "$setWindowFields output." + outField + " must contain a window function");
                }

                Object arg = fieldSpec.get(fn);
                Object windowSpec = fieldSpec.get("window");

                if (SWF_RANK_FAMILY.contains(fn) || "$shift".equals(fn)) {
                    if (windowSpec != null) {
                        throw mongoCommandError(5371601, fn + " does not accept a 'window' specification");
                    }

                    if (sortBy == null || sortBy.isEmpty()) {
                        throw mongoCommandError(5371602, fn + " requires a sortBy");
                    }

                    for (int i = 0; i < n; i++) {
                        setNestedValue(docs.get(i), outField, rankOrShiftValue(fn, arg, docs, i, sortBy));
                    }
                } else if ("$expMovingAvg".equals(fn)) {
                    if (windowSpec != null) {
                        throw mongoCommandError(5371601, fn + " does not accept a 'window' specification");
                    }

                    if (sortBy == null || sortBy.isEmpty()) {
                        throw mongoCommandError(5371602, fn + " requires a sortBy");
                    }

                    applyExpMovingAvg(swfMapArg(fn, arg, "input"), docs, outField);
                } else if ("$linearFill".equals(fn) || "$locf".equals(fn)) {
                    if (windowSpec != null) {
                        throw mongoCommandError(5371601, fn + " does not accept a 'window' specification");
                    }

                    if ("$locf".equals(fn)) {
                        if (sortBy == null || sortBy.isEmpty()) {
                            throw mongoCommandError(5371602, fn + " requires a sortBy");
                        }

                        Object carry = null;

                        for (Map<String, Object> doc : docs) {
                            Object v = projectComputedValue(arg, doc);

                            if (v != null) {
                                carry = v;
                            }

                            setNestedValue(doc, outField, carry);
                        }
                    } else {
                        applyLinearFill(arg, docs, outField, sortBy);
                    }
                } else if ("$derivative".equals(fn) || "$integral".equals(fn)) {
                    Map<String, Object> a = swfMapArg(fn, arg, "input");
                    Long unitMillis = a.get("unit") == null ? null : swfUnitMillis(String.valueOf(a.get("unit")), fn);
                    String sortField = swfSingleSortField(fn, sortBy);

                    for (int i = 0; i < n; i++) {
                        int[] w = resolveWindow(windowSpec, docs, i, sortBy);
                        setNestedValue(docs.get(i), outField,
                            derivativeOrIntegral(fn, a.get("input"), docs, w[0], w[1], sortField, unitMillis));
                    }
                } else if (SWF_ACCUMULATORS.contains(fn)) {
                    for (int i = 0; i < n; i++) {
                        int[] w = resolveWindow(windowSpec, docs, i, sortBy);
                        setNestedValue(docs.get(i), outField, windowAccumulate(fn, arg, docs, w[0], w[1]));
                    }
                } else {
                    throw mongoCommandError(5397901,
                        "window function '" + fn + "' is not supported by the in-memory driver");
                }
            }

            ret.addAll(docs);
        }

        return ret;
    }

    /**
     * Resolves a window spec (documents or range) to inclusive index bounds [lo, hi] around
     * position i; lo &gt; hi denotes an empty window. Default (no window) is the whole partition.
     */
    @SuppressWarnings("unchecked")
    private int[] resolveWindow(Object windowSpec, List<Map<String, Object>> docs, int i, Map<String, Object> sortBy) {
        int n = docs.size();

        if (windowSpec == null) {
            return new int[] {0, n - 1};
        }

        if (!(windowSpec instanceof Map)) {
            throw mongoCommandError(5397903, "'window' must be an object");
        }

        Map<String, Object> w = (Map<String, Object>) windowSpec;

        if (w.containsKey("documents") && w.containsKey("range")) {
            throw mongoCommandError(5397903, "'window' must contain either 'documents' or 'range', not both");
        }

        if (w.containsKey("range")) {
            return rangeWindow(w, docs, i, sortBy);
        }

        if (w.containsKey("unit")) {
            throw mongoCommandError(5397903, "window.unit is only valid together with a range window");
        }

        return documentsWindow(w, i, n);
    }

    /**
     * Resolves a range-window spec: [lo, hi] are value distances to the current document's
     * sortBy value (with a unit: a date, distances in unit-milliseconds). Requires an
     * ascending single-field stage sortBy (mongod code 5339902).
     */
    private int[] rangeWindow(Map<String, Object> w, List<Map<String, Object>> docs, int i, Map<String, Object> sortBy) {
        if (sortBy == null || sortBy.size() != 1) {
            throw mongoCommandError(5339902, "range-based windows require a sortBy with exactly one field");
        }

        Map.Entry<String, Object> s = sortBy.entrySet().iterator().next();

        if (!(s.getValue() instanceof Number) || ((Number) s.getValue()).doubleValue() <= 0) {
            throw mongoCommandError(5339902, "range-based windows require an ascending sortBy");
        }

        Object bounds = w.get("range");

        if (!(bounds instanceof List) || ((List<?>) bounds).size() != 2) {
            throw mongoCommandError(5397903, "window.range must be a [lower, upper] array");
        }

        Long unitMillis = w.get("unit") == null ? null : swfUnitMillis(String.valueOf(w.get("unit")), "window.range");
        String sortField = s.getKey();
        double cur = rangeSortValue(docs.get(i), sortField, unitMillis != null);
        double scale = unitMillis == null ? 1 : unitMillis;
        double loVal = rangeBound(((List<?>) bounds).get(0), cur, scale, true);
        double hiVal = rangeBound(((List<?>) bounds).get(1), cur, scale, false);
        // docs are sorted ascending by the sort field: the window is the contiguous run
        // of documents whose sort value lies in [cur + lo, cur + hi]
        int lo = docs.size();
        int hi = -1;

        for (int j = 0; j < docs.size(); j++) {
            double v = rangeSortValue(docs.get(j), sortField, unitMillis != null);

            if (v >= loVal && j < lo) {
                lo = j;
            }

            if (v <= hiVal) {
                hi = j;
            }
        }

        return new int[] {lo, hi};
    }

    private double rangeBound(Object bound, double cur, double scale, boolean isLower) {
        if ("unbounded".equals(bound)) {
            return isLower ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
        }

        if ("current".equals(bound)) {
            return cur;
        }

        if (bound instanceof Number) {
            return cur + ((Number) bound).doubleValue() * scale;
        }

        throw mongoCommandError(5397903,
            "window.range bounds must be 'unbounded', 'current' or a number, got: " + bound);
    }

    /** The sortBy value of a document inside a range window: a number, or a date with a unit. */
    private double rangeSortValue(Map<String, Object> doc, String sortField, boolean needsDate) {
        Object v = getByPath(doc, sortField);

        if (needsDate) {
            if (v instanceof Date) {
                return ((Date) v).getTime();
            }

            throw mongoCommandError(5397905,
                "range windows with a unit require the sortBy field to hold date values, got: " + v);
        }

        if (v instanceof Number) {
            return ((Number) v).doubleValue();
        }

        throw mongoCommandError(5397905,
            "range windows require the sortBy field to hold numeric values, got: " + v);
    }

    /**
     * Resolves a documents-window spec to inclusive index bounds [lo, hi] around position i;
     * lo &gt; hi denotes an empty window.
     */
    private int[] documentsWindow(Map<String, Object> w, int i, int n) {
        Object docsBounds = w.get("documents");

        if (!(docsBounds instanceof List) || ((List<?>) docsBounds).size() != 2) {
            throw mongoCommandError(5397903, "window.documents must be a [lower, upper] array");
        }

        int lo = windowBound(((List<?>) docsBounds).get(0), i, n, true);
        int hi = windowBound(((List<?>) docsBounds).get(1), i, n, false);
        return new int[] {Math.max(lo, 0), Math.min(hi, n - 1)};
    }

    private int windowBound(Object bound, int i, int n, boolean isLower) {
        if ("unbounded".equals(bound)) {
            return isLower ? 0 : n - 1;
        }

        if ("current".equals(bound)) {
            return i;
        }

        if (bound instanceof Number && ((Number) bound).doubleValue() == Math.rint(((Number) bound).doubleValue())) {
            long off = ((Number) bound).longValue();
            long idx = i + off;
            return (int) Math.max(Math.min(idx, Integer.MAX_VALUE), Integer.MIN_VALUE + 1);
        }

        throw mongoCommandError(5397903,
            "window.documents bounds must be 'unbounded', 'current' or an integer, got: " + bound);
    }

    /** $rank/$denseRank/$documentNumber/$shift over the sorted partition. */
    @SuppressWarnings("unchecked")
    private Object rankOrShiftValue(String fn, Object arg, List<Map<String, Object>> docs, int i, Map<String, Object> sortBy) {
        switch (fn) {
            case "$documentNumber":
                return i + 1;

            case "$rank": {
                // ties (equal sortBy tuple) share the rank of their first occurrence
                int r = i;

                while (r > 0 && sortTupleEquals(docs.get(r - 1), docs.get(i), sortBy)) {
                    r--;
                }

                return r + 1;
            }

            case "$denseRank": {
                int rank = 1;

                for (int j = 1; j <= i; j++) {
                    if (!sortTupleEquals(docs.get(j - 1), docs.get(j), sortBy)) {
                        rank++;
                    }
                }

                return rank;
            }

            case "$shift": {
                if (!(arg instanceof Map)) {
                    throw mongoCommandError(5397900, "$shift requires an object with 'output', 'by' and optional 'default'");
                }

                Map<String, Object> shift = (Map<String, Object>) arg;
                Object bySpec = shift.get("by");

                if (!(bySpec instanceof Number)) {
                    throw mongoCommandError(5397900, "$shift requires an integer 'by'");
                }

                int idx = i + ((Number) bySpec).intValue();

                if (idx >= 0 && idx < docs.size()) {
                    return projectComputedValue(shift.get("output"), docs.get(idx));
                }

                // out of the partition: the default expression may not reference document fields
                Object def = shift.get("default");
                return def == null ? null : projectComputedValue(def, new HashMap<>());
            }

            default:
                throw mongoCommandError(5397901, "window function '" + fn + "' is not supported by the in-memory driver");
        }
    }

    private boolean sortTupleEquals(Map<String, Object> a, Map<String, Object> b, Map<String, Object> sortBy) {
        for (String k : sortBy.keySet()) {
            if (compareSortValues(getByPath(a, k), getByPath(b, k)) != 0) {
                return false;
            }
        }

        return true;
    }

    /** Accumulator-style window functions over the inclusive index window [lo, hi]. */
    private Object windowAccumulate(String fn, Object arg, List<Map<String, Object>> docs, int lo, int hi) {
        switch (fn) {
            case "$count": {
                return Math.max(hi - lo + 1, 0);
            }

            case "$first":
                return lo > hi ? null : projectComputedValue(arg, docs.get(lo));

            case "$last":
                return lo > hi ? null : projectComputedValue(arg, docs.get(hi));

            case "$push": {
                List<Object> values = new ArrayList<>();

                for (int j = lo; j <= hi; j++) {
                    values.add(projectComputedValue(arg, docs.get(j)));
                }

                return values;
            }

            case "$sum":
            case "$avg": {
                double sum = 0;
                int count = 0;

                for (int j = lo; j <= hi; j++) {
                    Object v = projectComputedValue(arg, docs.get(j));

                    if (v instanceof Number) {
                        sum += ((Number) v).doubleValue();
                        count++;
                    }
                }

                if ("$sum".equals(fn)) {
                    return sum;
                }

                return count == 0 ? null : sum / count;
            }

            case "$min":
            case "$max": {
                Object best = null;

                for (int j = lo; j <= hi; j++) {
                    Object v = projectComputedValue(arg, docs.get(j));

                    if (v == null) {
                        // $min/$max ignore null/missing, like their accumulator counterparts
                        continue;
                    }

                    if (best == null) {
                        best = v;
                        continue;
                    }

                    int cmp = compareSortValues(v, best);

                    if ("$min".equals(fn) ? cmp < 0 : cmp > 0) {
                        best = v;
                    }
                }

                return best;
            }

            case "$stdDevPop":
            case "$stdDevSamp": {
                List<Object> values = new ArrayList<>();

                for (int j = lo; j <= hi; j++) {
                    values.add(projectComputedValue(arg, docs.get(j)));
                }

                // non-numeric values are filtered inside the Expr helpers, per mongod semantics
                return "$stdDevPop".equals(fn) ? Expr.computeStdDevPop(values) : Expr.computeStdDevSamp(values);
            }

            case "$covariancePop":
            case "$covarianceSamp": {
                if (!(arg instanceof List) || ((List<?>) arg).size() != 2) {
                    throw mongoCommandError(5397900, fn + " requires an array of exactly two expressions");
                }

                Object xExpr = ((List<?>) arg).get(0);
                Object yExpr = ((List<?>) arg).get(1);
                List<double[]> pairs = new ArrayList<>();

                for (int j = lo; j <= hi; j++) {
                    Object x = projectComputedValue(xExpr, docs.get(j));
                    Object y = projectComputedValue(yExpr, docs.get(j));

                    if (x instanceof Number && y instanceof Number) {
                        pairs.add(new double[] {((Number) x).doubleValue(), ((Number) y).doubleValue()});
                    }
                }

                int cnt = pairs.size();

                if (cnt == 0 || ("$covarianceSamp".equals(fn) && cnt < 2)) {
                    return null;
                }

                double meanX = 0;
                double meanY = 0;

                for (double[] p : pairs) {
                    meanX += p[0];
                    meanY += p[1];
                }

                meanX /= cnt;
                meanY /= cnt;
                double sum = 0;

                for (double[] p : pairs) {
                    sum += (p[0] - meanX) * (p[1] - meanY);
                }

                return "$covariancePop".equals(fn) ? sum / cnt : sum / (cnt - 1);
            }

            case "$firstN":
            case "$lastN": {
                Map<String, Object> a = swfMapArg(fn, arg, "n", "input");
                int count = swfPositiveN(fn, a.get("n"));
                Object input = a.get("input");
                List<Object> values = new ArrayList<>();

                if ("$firstN".equals(fn)) {
                    for (int j = lo; j <= hi && values.size() < count; j++) {
                        values.add(projectComputedValue(input, docs.get(j)));
                    }
                } else {
                    for (int j = Math.max(lo, hi - count + 1); j <= hi; j++) {
                        values.add(projectComputedValue(input, docs.get(j)));
                    }
                }

                return values;
            }

            case "$minN":
            case "$maxN": {
                Map<String, Object> a = swfMapArg(fn, arg, "n", "input");
                int count = swfPositiveN(fn, a.get("n"));
                Object input = a.get("input");
                List<Object> values = new ArrayList<>();

                for (int j = lo; j <= hi; j++) {
                    Object v = projectComputedValue(input, docs.get(j));

                    if (v != null) {
                        // like $min/$max, the N-forms ignore null and missing values
                        values.add(v);
                    }
                }

                values.sort(this::compareSortValues);

                if ("$maxN".equals(fn)) {
                    Collections.reverse(values);
                }

                return values.size() > count ? new ArrayList<>(values.subList(0, count)) : values;
            }

            case "$top":
            case "$bottom":
            case "$topN":
            case "$bottomN": {
                boolean nForm = fn.endsWith("N");
                Map<String, Object> a = nForm ? swfMapArg(fn, arg, "n", "sortBy", "output")
                                              : swfMapArg(fn, arg, "sortBy", "output");

                if (!(a.get("sortBy") instanceof Map) || ((Map<?, ?>) a.get("sortBy")).isEmpty()) {
                    throw mongoCommandError(5397900, fn + " requires a non-empty 'sortBy' object");
                }

                @SuppressWarnings("unchecked")
                Map<String, Object> opSortBy = (Map<String, Object>) a.get("sortBy");
                Object output = a.get("output");
                List<Map<String, Object>> windowDocs = new ArrayList<>();

                for (int j = lo; j <= hi; j++) {
                    windowDocs.add(docs.get(j));
                }

                // the operator sorts by its OWN sortBy spec, independent of the stage sortBy
                applySortBy(windowDocs, opSortBy);

                if (!nForm) {
                    if (windowDocs.isEmpty()) {
                        return null;
                    }

                    int idx = "$top".equals(fn) ? 0 : windowDocs.size() - 1;
                    return projectComputedValue(output, windowDocs.get(idx));
                }

                int count = swfPositiveN(fn, a.get("n"));
                List<Object> values = new ArrayList<>();

                if ("$topN".equals(fn)) {
                    for (int j = 0; j < Math.min(count, windowDocs.size()); j++) {
                        values.add(projectComputedValue(output, windowDocs.get(j)));
                    }
                } else {
                    // bottomN: the last n documents, keeping the operator's sort order
                    for (int j = Math.max(0, windowDocs.size() - count); j < windowDocs.size(); j++) {
                        values.add(projectComputedValue(output, windowDocs.get(j)));
                    }
                }

                return values;
            }

            default:
                throw mongoCommandError(5397901, "window function '" + fn + "' is not supported by the in-memory driver");
        }
    }

    /** Validates a window-function argument object: must be a Map containing all required keys. */
    @SuppressWarnings("unchecked")
    private Map<String, Object> swfMapArg(String fn, Object arg, String... requiredKeys) {
        if (!(arg instanceof Map)) {
            throw mongoCommandError(5397900, fn + " requires an object argument");
        }

        Map<String, Object> a = (Map<String, Object>) arg;

        for (String k : requiredKeys) {
            if (!a.containsKey(k)) {
                throw mongoCommandError(5397900, fn + " requires a '" + k + "' field");
            }
        }

        return a;
    }

    /**
     * Resolves a time unit for $derivative/$integral/range windows to its millisecond factor.
     * Calendar units (month/quarter/year) are rejected like mongod does for these operators.
     */
    private long swfUnitMillis(String unit, String context) {
        Long factor = DENSIFY_UNIT_MILLIS.get(unit);

        if (factor == null) {
            throw mongoCommandError(5397904, context + ": unit '" + unit
                + "' is not supported - only 'week', 'day', 'hour', 'minute', 'second' and 'millisecond'");
        }

        return factor;
    }

    /** $derivative/$integral (and $linearFill) need a stage sortBy over exactly one field. */
    private String swfSingleSortField(String fn, Map<String, Object> sortBy) {
        if (sortBy == null || sortBy.size() != 1) {
            throw mongoCommandError(5371801, fn + " requires a sortBy specification with exactly one field");
        }

        return sortBy.keySet().iterator().next();
    }

    /**
     * The sort-axis value of a document for $derivative/$integral: with a unit the sortBy field
     * must hold dates (value in millis), without a unit it must hold numbers.
     */
    private double swfSortAxisValue(String fn, Map<String, Object> doc, String sortField, Long unitMillis) {
        Object v = getByPath(doc, sortField);

        if (unitMillis != null) {
            if (v instanceof Date) {
                return ((Date) v).getTime();
            }

            throw mongoCommandError(5371802,
                fn + " with 'unit' requires the sortBy field to hold date values, got: " + v);
        }

        if (v instanceof Number) {
            return ((Number) v).doubleValue();
        }

        throw mongoCommandError(5371802,
            fn + " requires the sortBy field to hold numeric values (or dates with 'unit'), got: " + v);
    }

    /** Input expression value for $derivative/$integral: numeric, null/missing, or a loud error. */
    private Double swfNumericInput(String fn, Object input, Map<String, Object> doc) {
        Object v = projectComputedValue(input, doc);

        if (v == null) {
            return null;
        }

        if (v instanceof Number) {
            return ((Number) v).doubleValue();
        }

        throw mongoCommandError(5397906, fn + " requires numeric input values, got: " + v);
    }

    /**
     * $derivative: (input[hi] - input[lo]) / (sort[hi] - sort[lo]); $integral: trapezoid rule
     * over the window. With a unit the sort axis is a date measured in unit-milliseconds.
     * Windows with fewer than two documents (and null/missing inputs) yield null - except a
     * single-document $integral, which is 0 (a degenerate trapezoid has no area).
     */
    private Object derivativeOrIntegral(String fn, Object input, List<Map<String, Object>> docs,
            int lo, int hi, String sortField, Long unitMillis) {
        if (lo > hi) {
            return null;
        }

        if ("$derivative".equals(fn)) {
            if (lo == hi) {
                return null;
            }

            Double y0 = swfNumericInput(fn, input, docs.get(lo));
            Double y1 = swfNumericInput(fn, input, docs.get(hi));

            if (y0 == null || y1 == null) {
                return null;
            }

            double dx = swfSortAxisValue(fn, docs.get(hi), sortField, unitMillis)
                - swfSortAxisValue(fn, docs.get(lo), sortField, unitMillis);

            if (unitMillis != null) {
                dx /= unitMillis;
            }

            // a zero sort-distance has no defined slope
            return dx == 0 ? null : (y1 - y0) / dx;
        }

        double sum = 0;

        for (int j = lo; j < hi; j++) {
            Double ya = swfNumericInput(fn, input, docs.get(j));
            Double yb = swfNumericInput(fn, input, docs.get(j + 1));

            if (ya == null || yb == null) {
                return null;
            }

            double dx = swfSortAxisValue(fn, docs.get(j + 1), sortField, unitMillis)
                - swfSortAxisValue(fn, docs.get(j), sortField, unitMillis);

            if (unitMillis != null) {
                dx /= unitMillis;
            }

            sum += (ya + yb) / 2.0 * dx;
        }

        return sum;
    }

    /**
     * $expMovingAvg over the sorted partition: result_0 = input_0,
     * result_i = input_i * alpha + result_{i-1} * (1 - alpha); N is sugar for alpha = 2/(N+1).
     * Non-numeric inputs yield null for that document and leave the state untouched.
     */
    private void applyExpMovingAvg(Map<String, Object> a, List<Map<String, Object>> docs, String outField) {
        boolean hasN = a.containsKey("N");
        boolean hasAlpha = a.containsKey("alpha");

        if (hasN == hasAlpha) {
            throw mongoCommandError(5397900, "$expMovingAvg requires exactly one of 'N' or 'alpha'");
        }

        double alpha;

        if (hasN) {
            Object nVal = a.get("N");

            if (!(nVal instanceof Number) || ((Number) nVal).doubleValue() != Math.rint(((Number) nVal).doubleValue())
                    || ((Number) nVal).longValue() < 1) {
                throw mongoCommandError(5397900, "$expMovingAvg 'N' must be a positive integer, got: " + nVal);
            }

            alpha = 2.0 / (((Number) nVal).doubleValue() + 1.0);
        } else {
            Object alphaVal = a.get("alpha");

            if (!(alphaVal instanceof Number) || ((Number) alphaVal).doubleValue() <= 0
                    || ((Number) alphaVal).doubleValue() >= 1) {
                throw mongoCommandError(5397900, "$expMovingAvg 'alpha' must be a number between 0 and 1 exclusive, got: " + alphaVal);
            }

            alpha = ((Number) alphaVal).doubleValue();
        }

        Object input = a.get("input");
        Double state = null;

        for (Map<String, Object> doc : docs) {
            Object v = projectComputedValue(input, doc);

            if (v instanceof Number) {
                double x = ((Number) v).doubleValue();
                state = state == null ? x : x * alpha + state * (1 - alpha);
                setNestedValue(doc, outField, state);
            } else {
                setNestedValue(doc, outField, null);
            }
        }
    }

    /**
     * $linearFill: null/missing values are interpolated linearly between the surrounding
     * non-null values, proportionally to the sortBy distance (numbers, or dates via their
     * millisecond value). The sortBy values must be strictly increasing (mongod code 605001);
     * edge documents without both anchors stay null.
     */
    private void applyLinearFill(Object arg, List<Map<String, Object>> docs, String outField, Map<String, Object> sortBy) {
        if (sortBy == null || sortBy.size() != 1) {
            throw mongoCommandError(605001, "$linearFill requires a sortBy with exactly one field");
        }

        String sortField = sortBy.keySet().iterator().next();
        int n = docs.size();
        double[] xs = new double[n];
        Double[] ys = new Double[n];

        for (int i = 0; i < n; i++) {
            Object s = getByPath(docs.get(i), sortField);

            if (s instanceof Number) {
                xs[i] = ((Number) s).doubleValue();
            } else if (s instanceof Date) {
                xs[i] = ((Date) s).getTime();
            } else {
                throw mongoCommandError(605001, "$linearFill requires numeric or date sortBy values, got: " + s);
            }

            if (i > 0 && xs[i] <= xs[i - 1]) {
                throw mongoCommandError(605001, "$linearFill requires strictly increasing sortBy values");
            }

            Object v = projectComputedValue(arg, docs.get(i));

            if (v == null) {
                ys[i] = null;
            } else if (v instanceof Number) {
                ys[i] = ((Number) v).doubleValue();
            } else {
                throw mongoCommandError(5397906, "$linearFill requires numeric input values, got: " + v);
            }
        }

        int prevKnown = -1;

        for (int i = 0; i < n; i++) {
            if (ys[i] == null) {
                continue;
            }

            setNestedValue(docs.get(i), outField, ys[i]);

            if (prevKnown >= 0 && i - prevKnown > 1) {
                for (int j = prevKnown + 1; j < i; j++) {
                    double filled = ys[prevKnown]
                        + (ys[i] - ys[prevKnown]) * (xs[j] - xs[prevKnown]) / (xs[i] - xs[prevKnown]);
                    setNestedValue(docs.get(j), outField, filled);
                }
            } else if (prevKnown < 0) {
                // leading nulls without a left anchor stay null
                for (int j = 0; j < i; j++) {
                    setNestedValue(docs.get(j), outField, null);
                }
            }

            prevKnown = i;
        }

        // trailing nulls without a right anchor stay null (also covers an all-null partition)
        for (int j = Math.max(prevKnown + 1, 0); j < n; j++) {
            setNestedValue(docs.get(j), outField, null);
        }
    }

    /** Validates the 'n' argument of $firstN/$lastN/$minN/$maxN/$topN/$bottomN (mongod code 5787908). */
    private int swfPositiveN(String fn, Object nSpec) {
        Object n = nSpec instanceof Number ? nSpec : projectComputedValue(nSpec, new HashMap<>());

        if (!(n instanceof Number) || ((Number) n).doubleValue() != Math.rint(((Number) n).doubleValue())
                || ((Number) n).longValue() < 1) {
            throw mongoCommandError(5787908, "'n' must be a positive integer for " + fn + ", got: " + nSpec);
        }

        return (int) Math.min(((Number) n).longValue(), Integer.MAX_VALUE);
    }

    // ---- shared windowing helpers ---------------------------------------------------------

    /**
     * Splits documents into partitions (encounter order preserved): by a partitionBy expression,
     * by a partitionByFields list, or a single partition when neither is given.
     */
    private List<List<Map<String, Object>>> partitionForWindowing(List<Map<String, Object>> data,
            Object partitionBy, List<Object> partitionByFields) {
        Map<Object, List<Map<String, Object>>> partitions = new LinkedHashMap<>();

        for (Map<String, Object> doc : data) {
            Object key;

            if (partitionBy != null) {
                key = canonPartitionValue(projectComputedValue(partitionBy, doc));
            } else if (partitionByFields != null && !partitionByFields.isEmpty()) {
                List<Object> tuple = new ArrayList<>();

                for (Object f : partitionByFields) {
                    tuple.add(canonPartitionValue(getByPath(doc, String.valueOf(f))));
                }

                key = tuple;
            } else {
                key = Boolean.TRUE;
            }

            partitions.computeIfAbsent(key, k -> new ArrayList<>()).add(doc);
        }

        return new ArrayList<>(partitions.values());
    }

    /** Stable in-place sort by a {field: 1|-1} spec; a null/empty spec keeps the input order. */
    private void applySortBy(List<Map<String, Object>> docs, Map<String, Object> sortBy) {
        if (sortBy == null || sortBy.isEmpty()) {
            return;
        }

        docs.sort((a, b) -> {
            for (Map.Entry<String, Object> e : sortBy.entrySet()) {
                int c = compareSortValues(getByPath(a, e.getKey()), getByPath(b, e.getKey()));

                if (e.getValue() instanceof Number && ((Number) e.getValue()).intValue() < 0) {
                    c = -c;
                }

                if (c != 0) {
                    return c;
                }
            }

            return 0;
        });
    }

    /** Null-safe, numeric-aware comparison (nulls first, mirroring MongoDB's sort of missing). */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private int compareSortValues(Object a, Object b) {
        if (a == null && b == null) {
            return 0;
        }

        if (a == null) {
            return -1;
        }

        if (b == null) {
            return 1;
        }

        if (a instanceof Number && b instanceof Number) {
            return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
        }

        if (a instanceof Comparable && a.getClass().isAssignableFrom(b.getClass())) {
            return ((Comparable) a).compareTo(b);
        }

        return String.valueOf(a).compareTo(String.valueOf(b));
    }
}
