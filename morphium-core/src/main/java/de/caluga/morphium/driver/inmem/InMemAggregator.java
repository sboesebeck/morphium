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
                //noinspection unchecked
                ret = o.subList(0, size);
                break;

            case "$merge":
                //{ $merge: {
                //     into: <collection> -or- { db: <db>, coll: <collection> },
                //     on: <identifier field> -or- [ <identifier field1>, ...],  // Optional
                //     let: <variables>,                                         // Optional
                //     whenMatched: <replace|keepExisting|merge|fail|pipeline>,  // Optional
                //     whenNotMatched: <insert|discard|fail>                     // Optional
                //} }
                @SuppressWarnings("unchecked")
                Map setting = ((Map<String, Object>) step.get(stage));
                String db = morphium.getConfig().connectionSettings().getDatabase();
                String coll = "";

                if (setting.get("into") instanceof Map) {
                    //noinspection unchecked
                    db = (String)((Map<String, Object>) setting.get("into")).get("db");
                    //noinspection unchecked
                    coll = (String)((Map<String, Object>) setting.get("into")).get("coll");
                } else {
                    coll = (String)(setting.get("into"));
                }

                if (setting.containsKey("on")) {
                    //merge
                    //need to lookup each entry, match it to on field
                    @SuppressWarnings("unchecked")
                    List<String> on = (List<String>)(setting.get("on"));
                    MergeActionWhenNotMatched notMatched = MergeActionWhenNotMatched.insert;
                    MergeActionWhenMatched matched = MergeActionWhenMatched.merge;
                    List<Map<String, Object>> mergePipeline = null;

                    if (setting.containsKey("whenMatched")) {
                        if (setting.get("whenMatched") instanceof Map) {
                            //noinspection unchecked
                            mergePipeline = (List<Map<String, Object>>) setting.get("whenMatched");
                        } else {
                            matched = MergeActionWhenMatched.valueOf((String) setting.get("whenMatched"));
                        }
                    }

                    if (setting.containsKey("whenNotMatched")) {
                        notMatched = MergeActionWhenNotMatched.valueOf((String) setting.get("whenNotMatched"));
                    }

                    for (Map<String, Object> doc : data) {
                            Map<String, Object> q = new HashMap<>();

                            for (String onfld : on) {
                                q.put(onfld, doc.get(onfld));
                            }

                            List<Map<String, Object>> toMergeTo = null;         //morphium.getDriver().find(db, coll, q, null, null, 0, -1, 100, null, null, null);

                            if (toMergeTo == null || toMergeTo.size() == 0) {
                                //not matched
                                switch (notMatched) {
                                    case fail:
                                        throw new MorphiumDriverException("Aggregation merge step failed - no doc matched!");

                                    case discard:
                                        continue;

                                    case insert:
                                        //morphium.getDriver().store(db, coll, Collections.singletonList(doc), null);
                                        break;

                                    default:
                                        throw new IllegalArgumentException("unknown whenNotMatched action " + notMatched);
                                }
                            } else if (toMergeTo.size() > 1) {
                                throw new MorphiumDriverException("Aggregation merge step failed - on fields query returned more than one value. On-Fields are not unique");
                            } else {
                                Map<String, Object> mergeObject = toMergeTo.get(0);

                                switch (matched) {
                                    case merge:
                                        if (mergePipeline != null) {
                                            //need to run through pipeline....
                                            for (Map<String, Object> mergePipelineStep : mergePipeline) {
                                                String s = mergePipelineStep.keySet().stream().findFirst().get();

                                                switch (s) {
                                                    case "$set":
                                                    case "$addFields":
                                                        break;

                                                    case "$unset":
                                                        Map<String, Object> newO = new HashMap<>(doc);

                                                        if (mergePipelineStep.get(s) instanceof List) {
                                                            @SuppressWarnings("unchecked")
                                                            List<String> flds = (List<String>) mergePipelineStep.get(s);

                                                            for (String f : flds) {
                                                                newO.remove(f);
                                                            }
                                                        } else {
                                                            newO.remove(mergePipelineStep.get(s));
                                                        }

                                                        ret.add(newO);
                                                        break;

                                                    case "$project":
                                                        break;

                                                    case "$replaceRoot":
                                                    case "$replaceWith":
                                                        Object newRoot;

                                                        if (mergePipelineStep.get(s) instanceof Map) {
                                                            newRoot = ((Map) mergePipelineStep.get(s)).get("newRoot");
                                                        } else {
                                                            newRoot = mergePipelineStep.get(s);
                                                        }

                                                        if (newRoot instanceof String) {
                                                            if (newRoot.toString().startsWith("$")) {
                                                                //fieldRef
                                                                //noinspection unchecked
                                                                ret.add((Map<String, Object>) Expr.field(newRoot.toString()).evaluate(doc));
                                                            } else {
                                                                throw new IllegalArgumentException("cannot replace root with single value");
                                                            }
                                                        } else {
                                                            //parse expr!!!!!
                                                            Expr expr = Expr.parse(newRoot);
                                                            //noinspection unchecked
                                                            ret.add((Map<String, Object>) expr.evaluate(doc));
                                                        }

                                                        break;

                                                    default:
                                                        throw new MorphiumDriverException("Aggregation error: unknown aggregation step in merge pipeline " + s);
                                                }
                                            }
                                        } else {
                                            if (mergeObject.containsKey("_id")) {
                                                throw new MorphiumDriverException("Aggregation merge failure: referenced object keeps _id!");
                                            }

                                            //just merge
                                            Map<String, Object> newDoc = new HashMap<>(doc);
                                            newDoc.putAll(mergeObject);
                                            //                                            morphium.getDriver().store(db, coll, Collections.singletonList(mergeObject), null);
                                        }

                                        break;

                                    case fail:
                                    case replace:
                                    case keepExisting:
                                    default:
                                        throw new IllegalArgumentException("unknown whenMatched action " + matched);
                                }
                            }
                    }
                } else {
                    //                    try {
                    //                        morphium.getDriver().store(db, coll, data, null);
                    //                    } catch (MorphiumDriverException e) {
                    //                        log.error("Something went wrong with $merge", e);
                    //                    }
                }

                break;

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
            // $indexStats previously shared $geoNear's body and silently ran geoNear logic (#243).
            // It is not implemented by the in-memory driver, so it is grouped with the other
            // unimplemented stages here and surfaces as a proper command error instead.
            case "$indexStats":
            case "$collStats":
            case "$listSessions":
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
}
