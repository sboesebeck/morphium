package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.*;
import de.caluga.morphium.aggregation.*;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.SingleBatchCursor;
import de.caluga.morphium.driver.commands.AggregateMongoCommand;
import de.caluga.morphium.driver.commands.ExplainCommand.ExplainVerbosity;
import de.caluga.morphium.ObjectMapperImpl;
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
    private Class<? extends T> type;
    private Morphium morphium;
    private Class<? extends R> rType;
    private String collectionName;
    private boolean useDisk = false;
    private boolean explain = false;
    private Collation collation;

    public InMemAggregator(Morphium morphium, Class<? extends T> type, Class<? extends R> resultType) {
        this.morphium = morphium;
        setSearchType(type);
        setResultType(resultType);
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
    public Class<? extends T> getSearchType() {
        return type;
    }

    @Override
    public void setSearchType(Class<? extends T> type) {
        this.type = type;
    }

    @Override
    public Class<? extends R> getResultType() {
        return rType;
    }

    @Override
    public void setResultType(Class<? extends R> type) {
        rType = type;
    }

    @Override
    public Aggregator<T, R> project(Map<String, Object> m) {
        Map<String, Object> p = new LinkedHashMap<>();

        for (Map.Entry<String, Object> e : m.entrySet()) {
            p.put(e.getKey(), e.getValue());
        }

        Map<String, Object> map = UtilsMap.of("$project", p);
        params.add(map);
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
    //        params.add(o);
    //        return this;
    //    }

    @Override
    public Aggregator<T, R> addFields(Map<String, Object> m) {
        Map<String, Object> ret = new LinkedHashMap<>();

        for (Map.Entry<String, Object> e : m.entrySet()) {
            if (!(e.getValue() instanceof Expr)) {
                throw new IllegalArgumentException("InMemAggregator only works with Expr");
            }

            ret.put(e.getKey(), e.getValue());
        }

        Map<String, Object> o = UtilsMap.of("$addFields", ret);
        params.add(o);
        return this;
    }

    @Override
    public Aggregator<T, R> match(Query<T> q) {
        Map<String, Object> o = UtilsMap.of("$match", q.toQueryObject());

        if (collectionName == null) {
            collectionName = q.getCollectionName();
        }

        params.add(o);
        return this;
    }

    @Override
    public Aggregator<T, R> matchSubQuery(Query<?> q) {
        Map<String, Object> o = UtilsMap.of("$match", q.toQueryObject());
        params.add(o);
        return this;
    }

    @Override
    public Aggregator<T, R> match(Expr q) {
        params.add(UtilsMap.of("$match", UtilsMap.of("$expr", q)));
        return this;
    }

    @Override
    public Aggregator<T, R> limit(int num) {
        Map<String, Object> o = UtilsMap.of("$limit", num);
        params.add(o);
        return this;
    }

    @Override
    public Aggregator<T, R> skip(int num) {
        Map<String, Object> o = UtilsMap.of("$skip", num);
        params.add(o);
        return this;
    }

    @Override
    public Aggregator<T, R> unwind(String listField) {
        Map<String, Object> o = UtilsMap.of("$unwind", listField);
        params.add(o);
        return this;
    }

    public Aggregator<T, R> unwind(Expr field) {
        Map<String, Object> o = UtilsMap.of("$unwind", field);
        params.add(o);
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
        Map<String, Object> o = UtilsMap.of("$sort", sort);
        params.add(o);
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
        Group<T, R> gr = new Group<>(this, id);
        groups.add(gr);
        return gr;
    }

    @Override
    public void addOperator(Map<String, Object> o) {
        params.add(o);
    }

    @Override
    public List<R> aggregate() {
        try {
            return deserializeList();
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getCount() {
        var originalPipeline = new ArrayList<>(getPipeline());
        addOperator(UtilsMap.of("$count", "num"));
        List<Map<String, Object>> res = doAggregation();

        if (res.get(0).get("num") instanceof Integer) {
            params.clear();
            params.addAll(originalPipeline);
            return ((Integer) res.get(0).get("num")).longValue();
        }

        params.clear();
        params.addAll(originalPipeline);
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
            new Thread(this::doAggregation);
        } else {
            morphium.queueTask(() -> {
                try {
                    long start = System.currentTimeMillis();
                    List<R> result = deserializeList();
                    callback.onOperationSucceeded(AsyncOperationType.READ, null, System.currentTimeMillis() - start, result, null, InMemAggregator.this);
                } catch (MorphiumDriverException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    @SuppressWarnings("RedundantThrows")
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
        //TODO implement
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
        params.add(UtilsMap.of("$count", fld));
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
        params.add(m);
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

        params.add(map);
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

        params.add(UtilsMap.of("$collStats", m));
        return this;
    }

    //{ $currentOp: { allUsers: <boolean>, idleConnections: <boolean>, idleCursors: <boolean>, idleSessions: <boolean>, localOps: <boolean> } }
    @Override
    public Aggregator<T, R> currentOp(boolean allUsers, boolean idleConnections, boolean idleCursors, boolean idleSessions, boolean localOps) {
        params.add(UtilsMap.of("$currentOp", UtilsMap.of("allUsers", allUsers, "idleConnections", idleConnections, "idleCursors", idleCursors, "idleSessions", idleSessions, "localOps", localOps)));
        return this;
    }

    @Override
    public Aggregator<T, R> facetExpr(Map<String, Expr> facets) {
        Map<String, Object> map = Utils.getQueryObjectMap(facets);
        params.add(UtilsMap.of("$facet", map));
        return this;
    }

    @Override
    public Aggregator<T, R> facet(Map<String, Aggregator> facets) {
        Map<String, Object> map = new HashMap<>();

        for (Map.Entry<String, Aggregator> e : facets.entrySet()) {
            map.put(e.getKey(), e.getValue().getPipeline());
        }

        params.add(UtilsMap.of("$facet", map));
        return this;
    }

    @Override
    public Aggregator<T, R> geoNear(Map<GeoNearFields, Object> param) {
        Map<String, Object> map = new LinkedHashMap<>();

        for (Map.Entry<GeoNearFields, Object> e : param.entrySet()) {
            map.put(e.getKey().name(), ((ObjectMapperImpl) morphium.getMapper()).marshallIfNecessary(e.getValue()));
        }

        params.add(UtilsMap.of("$geoNear", map));
        return this;
    }

    @Override
    public Aggregator<T, R> graphLookup(Class<?> type, Expr startWith, Enum connectFromField, Enum connectToField, String as, Integer maxDepth, String depthField, Query restrictSearchWithMatch) {
        return graphLookup(morphium.getMapper().getCollectionName(type), startWith, connectFromField.name(), connectToField.name(), as, maxDepth, depthField, restrictSearchWithMatch);
    }

    @Override
    public Aggregator<T, R> graphLookup(Class<?> type, Expr startWith, String connectFromField, String connectToField, String as, Integer maxDepth, String depthField, Query restrictSearchWithMatch) {
        return graphLookup(morphium.getMapper().getCollectionName(type), startWith, connectFromField, connectToField, as, maxDepth, depthField, restrictSearchWithMatch);
    }

    @Override
    public Aggregator<T, R> graphLookup(String fromCollection, Expr startWith, String connectFromField, String connectToField, String as, Integer maxDepth, String depthField,
        Query restrictSearchWithMatch) {
        Map<String, Object> add = UtilsMap.of("from", (Object) fromCollection, "startWith", startWith, "connectFromField", connectFromField, "connectToField", connectToField, "as", as);
        params.add(UtilsMap.of("$graphLookup", add));

        if (maxDepth != null) {
            add.put("maxDepth", maxDepth);
        }

        if (depthField != null) {
            add.put("depthField", depthField);
        }

        if (restrictSearchWithMatch != null) {
            add.put("restrictSearchWithMatch", restrictSearchWithMatch);
        }

        return this;
    }

    @Override
    public Aggregator<T, R> indexStats() {
        params.add(UtilsMap.of("$indexStats", new HashMap<>()));
        return this;
    }

    @Override
    public Aggregator<T, R> listLocalSessionsAllUsers() {
        params.add(UtilsMap.of("$listLocalSessions", UtilsMap.of("allUsers", true)));
        return this;
    }

    @Override
    public Aggregator<T, R> listLocalSessions() {
        params.add(UtilsMap.of("$listLocalSessions", new HashMap<>()));
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

        params.add(UtilsMap.of("$listLocalSessions", UtilsMap.of("users", usersList)));
        return this;
    }

    @Override
    public Aggregator<T, R> listSessionsAllUsers() {
        params.add(UtilsMap.of("$listSessions", UtilsMap.of("allUsers", true)));
        return this;
    }

    @Override
    public Aggregator<T, R> listSessions() {
        params.add(UtilsMap.of("$listSessions", new HashMap<>()));
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

        params.add(UtilsMap.of("$listSessions", UtilsMap.of("users", usersList)));
        return this;
    }

    /**
     * $lookup:
     * {
     * from: <collection to join>,
     * localField: <field from the input documents>,
     * foreignField: <field from the documents of the "from" collection>,
     * as: <output array field>
     * }
     *
     * @return
     */

    public Aggregator<T, R> lookup(Class fromType, Enum localField, Enum foreignField, String outputArray, List<Expr> pipeline, Map<String, Expr> let) {
        return lookup(getMorphium().getMapper().getCollectionName(fromType), getMorphium().getARHelper().getMongoFieldName(getSearchType(), localField.name()),
                getMorphium().getARHelper().getMongoFieldName(fromType, foreignField.name()), outputArray, pipeline, let);
    }

    @Override
    public Aggregator<T, R> lookup(String fromCollection, String localField, String foreignField, String outputArray, List<Expr> pipeline, Map<String, Expr> let) {
        Map<String, Object> m = new HashMap<>(UtilsMap.of("from", fromCollection));

        if (localField != null) {
            m.put("localField", localField);
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

        params.add(UtilsMap.of("$lookup", m));
        return this;
    }

    @Override
    public Aggregator<T, R> merge(String intoDb, String intoCollection, MergeActionWhenMatched matchAction, MergeActionWhenNotMatched notMatchedAction, String... onFields) {
        return merge(intoDb, intoCollection, null, null, matchAction, notMatchedAction, onFields);
    }

    @Override
    public Aggregator<T, R> merge(String intoCollection, Map<String, Expr> let, List<Map<String, Expr>> machedPipeline, MergeActionWhenNotMatched notMatchedAction, String... onFields) {
        return merge(morphium.getConfig().getDatabase(), intoCollection, let, machedPipeline, MergeActionWhenMatched.merge, notMatchedAction, onFields);
    }

    @Override
    public Aggregator<T, R> merge(Class<?> intoCollection, Map<String, Expr> let, List<Map<String, Expr>> machedPipeline, MergeActionWhenMatched matchAction,
        MergeActionWhenNotMatched notMatchedAction, String... onFields) {
        return merge(morphium.getConfig().getDatabase(), morphium.getMapper().getCollectionName(intoCollection), let, machedPipeline, MergeActionWhenMatched.merge, notMatchedAction, onFields);
    }

    @Override
    public Aggregator<T, R> merge(String intoDb, String intoCollection) {
        return merge(intoDb, intoCollection, null, null, MergeActionWhenMatched.merge, MergeActionWhenNotMatched.insert);
    }

    @Override
    public Aggregator<T, R> merge(Class<?> intoCollection) {
        return merge(morphium.getConfig().getDatabase(), morphium.getMapper().getCollectionName(intoCollection), null, null, MergeActionWhenMatched.merge, MergeActionWhenNotMatched.insert);
    }

    @Override
    public Aggregator<T, R> merge(String intoCollection) {
        return merge(morphium.getConfig().getDatabase(), intoCollection, null, null, MergeActionWhenMatched.merge, MergeActionWhenNotMatched.insert);
    }

    @Override
    public Aggregator<T, R> merge(String intoCollection, MergeActionWhenMatched matchAction, MergeActionWhenNotMatched notMatchedAction, String... onFields) {
        return merge(morphium.getConfig().getDatabase(), intoCollection, null, null, matchAction, notMatchedAction, onFields);
    }

    @SuppressWarnings("ConstantConditions")
    private Aggregator<T, R> merge(String intoDb, String intoCollection, Map<String, Expr> let, List<Map<String, Expr>> pipeline, MergeActionWhenMatched matchAction,
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

        params.add(UtilsMap.of("$merge", doc));
        return this;
    }

    @Override
    public Aggregator<T, R> out(String collectionName) {
        params.add(UtilsMap.of("$out", UtilsMap.of("coll", collectionName)));
        return this;
    }

    @Override
    public Aggregator<T, R> out(Class<?> type) {
        return out(morphium.getMapper().getCollectionName(type));
    }

    @Override
    public Aggregator<T, R> out(String db, String collectionName) {
        params.add(UtilsMap.of("$out", UtilsMap.of("coll", collectionName, "db", db)));
        return this;
    }

    @Override
    public Aggregator<T, R> planCacheStats(Map<String, Object> param) {
        params.add(UtilsMap.of("$planCacheStats", new HashMap<>()));
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
        params.add(UtilsMap.of("$redact", redact));
        return this;
    }

    @Override
    public Aggregator<T, R> replaceRoot(Expr newRoot) {
        params.add(UtilsMap.of("$replaceRoot", UtilsMap.of("newRoot", newRoot)));
        return this;
    }

    @Override
    public Aggregator<T, R> replaceWith(Expr newDoc) {
        params.add(UtilsMap.of("$replaceWith", newDoc));
        return this;
    }

    @Override
    public Aggregator<T, R> sample(int sampleSize) {
        params.add(UtilsMap.of("$sample", UtilsMap.of("size", sampleSize)));
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
        Map<String, Object> o = UtilsMap.of("$set", Utils.getQueryObjectMap(param));
        params.add(o);
        return this;
    }

    /**
     * The $sortByCount stage is equivalent to the following $group + $sort
     * sequence:
     * <p>
     * <p>
     * { $group: { _id: <expression>, count: { $sum: 1 } } },
     * { $sort: { count: -1 } }
     *
     * @param sortby
     * @return
     */
    @Override
    public Aggregator<T, R> sortByCount(Expr sortby) {
        params.add(UtilsMap.of("$sortByCount", sortby));
        return this;
    }

    @Override
    public Aggregator<T, R> unionWith(String collection) {
        params.add(UtilsMap.of("$unionWith", collection));
        return this;
    }

    @Override
    public Aggregator<T, R> unionWith(Aggregator agg) {
        params.add(UtilsMap.of("$unionWith", UtilsMap.of("coll", (Object) collectionName, "pipeline", agg.getPipeline())));
        return this;
    }

    @Override
    public Aggregator<T, R> unset(List<String> field) {
        params.add(UtilsMap.of("$unset", field));
        return this;
    }

    @Override
    public Aggregator<T, R> unset(String... param) {
        params.add(UtilsMap.of("$unset", Arrays.asList(param)));
        return this;
    }

    @Override
    public Aggregator<T, R> unset(Enum... field) {
        List<String> lst = Arrays.stream(field).map(Enum::name).collect(Collectors.toList());
        params.add(UtilsMap.of("$unset", lst));
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

        params.add(UtilsMap.of(stageName, param));
        return this;
    }

    @Override
    public Aggregator<T, R> collation(Collation collation) {
        this.collation = collation;
        return this;
    }

    public List<Map<String, Object>> execStep(Map<String, Object> step, List<Map<String, Object>> data) {
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

            case "$project":
                for (Map<String, Object> o : data) {
                    Map<String, Object> obj = new HashMap<>(o);
                    ret.add(obj);
                    @SuppressWarnings("unchecked")
                    Map<String, Object> op = (((Map<String, Object>) step.get(stage)));

                    for (String k : op.keySet()) {
                        Object value = op.get(k);

                        if (value instanceof String && ((String) value).startsWith("$")) {
                            obj.put(k, obj.get(((String) value).substring(1)));
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

                break;

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
                            //noinspection unchecked
                            for (String fld : ((Map<String, Object>) value).keySet()) {
                                if (obj.get(fld) instanceof Expr) {
                                    obj.put(fld, ((Expr) obj.get(fld)).evaluate(obj));
                                } else {
                                    log.error("InMemoryAggregation oly works with Expr");
                                }
                            }
                        }
                    }
                }

                break;

            case "$count":
                ret.add(UtilsMap.of((String) step.get(stage), data.size()));
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
                        if (((Map<?, ?>) id).keySet().toArray()[0].toString().startsWith("$")) {
                            //expr?
                            var expr = Expr.parse(id);
                            var result = expr.evaluate(obj);
                            res.putIfAbsent(result, new HashMap<>());
                            res.get(result).putIfAbsent("_id", result);
                            id = result;
                        } else {
                            log.info("ID is combined...");
                            Map newIdMap = new HashMap<>();

                            for (var e : ((Map<?, ?>) id).entrySet()) {
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
                                    Object toPush = ((Map<?, ?>) opValue).get(op);
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
                                    if (((Map<?, ?>) opValue).get(op).toString().startsWith("$")) {
                                        //field reference
                                        Number count = (Number)((Map) res.get(id).get("$_calc_" + fld)).get("count");
                                        count = count.intValue() + 1;
                                        //noinspection unchecked
                                        ((Map) res.get(id).get("$_calc_" + fld)).put("count", count);
                                        Number current = (Number)((Map) res.get(id).get("$_calc_" + fld)).get("sum");
                                        Number v = (Number) o.get(((Map<?, ?>) opValue).get(op).toString().substring(1));
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
                                    res.get(id).putIfAbsent(fld, o.get(((Map<?, ?>) opValue).get(op).toString().substring(1)));
                                    break;

                                case "$last":
                                    res.get(id).put(fld, o.get(((Map<?, ?>) opValue).get(op).toString().substring(1)));
                                    break;

                                case "$max":
                                    if (((Map<?, ?>) opValue).get(op).toString().startsWith("$")) {
                                        Object oVal = o.get(((Map<?, ?>) opValue).get(op).toString().substring(1));
                                        res.get(id).putIfAbsent(fld, oVal);

                                        //noinspection unchecked
                                        if (((Comparable) res.get(id).get(fld)).compareTo(oVal) > 0) {
                                            res.get(id).put(fld, oVal);
                                        }
                                    }

                                    break;

                                case "$min":
                                    if (((Map<?, ?>) opValue).get(op).toString().startsWith("$")) {
                                        Object oVal = o.get(((Map<?, ?>) opValue).get(op).toString().substring(1));
                                        res.get(id).putIfAbsent(fld, oVal);

                                        //noinspection unchecked
                                        if (((Comparable) res.get(id).get(fld)).compareTo(oVal) < 0) {
                                            res.get(id).put(fld, oVal);
                                        }
                                    }

                                    break;

                                case "$sum":
                                    res.get(id).putIfAbsent(fld, 0);
                                    Number current = (Number) res.get(id).get(fld);

                                    if (((Map<?, ?>) opValue).get(op).toString().startsWith("$")) {
                                        //field reference
                                        Number v = (Number) o.get(((Map<?, ?>) opValue).get(op).toString().substring(1));
                                        res.get(id).put(fld, current.doubleValue() + v.doubleValue());
                                    } else if (((Map<?, ?>) opValue).get(op) instanceof Number) {
                                        Number v = (Number)((Map<?, ?>) opValue).get(op);
                                        res.get(id).put(fld, current.doubleValue() + v.doubleValue());
                                    } else if (((Map<?, ?>) opValue).get(op) instanceof Expr) {
                                        Number v = (Number)(o.get(((Expr)((Map<?, ?>) opValue).get(op)).evaluate(o)));
                                        res.get(id).put(fld, current.doubleValue() + v.doubleValue());
                                    }

                                    break;

                                case "$accumulator":
                                case "$mergeObjects":
                                case "$stdDevPop":
                                case "$stdDevSamp":
                                    throw new RuntimeException(op + " not implemented yet,sorry");

                                default:
                                    if (opValue instanceof Map) {
                                        Map<String, Object> opMap = (Map<String, Object>) opValue;

                                        try {
                                            Expr expr = Expr.parse(opMap);
                                            res.get(id).put(fld, expr.evaluate(o));
                                        } catch (Exception e) {
                                            //swallow
                                        }
                                    }

                                    log.error("unknown accumulator " + op);
                                    break;
                            }
                        } else if (opValue instanceof String && opValue.toString().startsWith("$")) {
                            opValue = o.get(opValue.toString().substring(1));
                            res.get(id).put(fld, opValue);
                        }
                    }
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
                ret = new ArrayList<>(data);
                ret.sort((o1, o2) -> {
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
                List<Map<String, Object>> pipeline = (List<Map<String, Object>>) lookup.get("pipeline");
                @SuppressWarnings("unchecked")
                Map<String, Object> let = (Map<String, Object>) lookup.get("let");
                String as = (String) lookup.get("as");

                if (pipeline != null || let != null) {
                    throw new IllegalArgumentException("pipeline/let is not supported yet.");
                }

                for (Map<String, Object> doc : data) {
                    Object localValue = doc.get(localField);
                    //                    try {
                    //                        List<Map<String, Object>> other = morphium.getDriver().find(morphium.getConfig().getDatabase(), collection, UtilsMap.of(foreignField, localValue), null, null, 0, 0, 100, null, null, null);
                    //                        Map<String, Object> o = new HashMap<>(doc);
                    //                        o.put(as, other);
                    //                        ret.add(o);
                    //                    } catch (MorphiumDriverException e) {
                    //                        throw new RuntimeException(e);
                    //                    }
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
                String db = morphium.getConfig().getDatabase();
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
                        try {
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
                        } catch (MorphiumDriverException e) {
                            throw new RuntimeException(e);
                            // e.printStackTrace();
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

            case "$planCacheStats":
            case "$redact":
            case "$sortByCount":
            case "$unionWith":
            case "$currentOp":
            case "$listLocalSessions":
            case "$findAndModyfy":
            case "$update":
            case "$bucket":
            case "$bucketAuto":
            case "$collStats":
            case "$listSessions":
            case "$facet":
            case "$indexStats":
            case "$geoNear":
            case "$graphLookup":
            default:
                log.error("unhandled Aggregation stage " + stage);
        }

        return ret;
    }

    @Override
    public List<Map<String, Object>> aggregateMap() {
        return doAggregation();
    }

    private List<Map<String, Object>> doAggregation() {
        var q = getMorphium().createQueryFor(getSearchType());

        if (getCollectionName() != null) {
            q.setCollectionName(getCollectionName());
        }

        List<Map<String, Object>> result = q.asMapList();

        for (Map<String, Object> step : getPipeline()) {
            //evaluate each step
            result = execStep(step, result);
        }

        return result;
    }

    @Override
    public Map<String, Object> explain() throws MorphiumDriverException {
        return explain(null);
    }

    @Override
    public Map<String, Object> explain(ExplainVerbosity verbosity) throws MorphiumDriverException {
        return Doc.of("err", "not supported with inMemDriver", "ok", 0.0);
    }
}
