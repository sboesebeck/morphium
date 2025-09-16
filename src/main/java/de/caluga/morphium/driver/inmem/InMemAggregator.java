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
                List<Map<String, Object>> pipeline = (List<Map<String, Object>>) lookup.get("pipeline");
                @SuppressWarnings("unchecked")
                Map<String, Object> let = (Map<String, Object>) lookup.get("let");
                String as = (String) lookup.get("as");

                if (pipeline != null || let != null) {
                    throw new IllegalArgumentException("pipeline/let is not supported yet.");
                }

                for (Map<String, Object> doc : data) {
                    Object localValue = doc.get(localField);

                    // Use the InMemoryDriver's private find method via reflection or direct access
                    // Since we're in the InMemAggregator which is part of the inmem package,
                    // we can access the InMemoryDriver's data directly
                    InMemoryDriver inMemDriver = (InMemoryDriver) morphium.getDriver();

                    try {
                        // Get the foreign collection data directly from InMemoryDriver
                        Map<String, List<Map<String, Object>>> database = inMemDriver.getDatabase(morphium.getConfig().getDatabase());
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

            case "$facet":
                op = step.get(stage);

                if (op instanceof Map) {
                    Map<String, Object> facetMap = (Map<String, Object>) op;
                    Map<String, Object> facetResult = new HashMap<>();

                    // Execute each facet pipeline on current data
                    for (Map.Entry<String, Object> facetEntry : facetMap.entrySet()) {
                        String facetName = facetEntry.getKey();
                        Object facetPipeline = facetEntry.getValue();

                        // For now, treat each facet as a simple field extraction
                        List<Object> facetResults = new ArrayList<>();
                        for (Map<String, Object> doc : data) {
                            if (facetPipeline instanceof Expr) {
                                Object value = ((Expr) facetPipeline).evaluate(doc);
                                if (value != null) {
                                    facetResults.add(value);
                                }
                            }
                        }
                        facetResult.put(facetName, facetResults);
                    }

                    ret = List.of(facetResult);
                }
                break;

            case "$planCacheStats":
            case "$redact":
            case "$unionWith":
            case "$currentOp":
            case "$listLocalSessions":
            case "$findAndModyfy":
            case "$update":
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

                    Map<Object, List<Map<String, Object>>> buckets = new HashMap<>();

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
                    List<Object> values = new ArrayList<>();
                    Map<Object, Map<String, Object>> valueToDoc = new HashMap<>();

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
                            values.add(value);
                            valueToDoc.put(value, doc);
                        }
                    }

                    // Sort values
                    values.sort((a, b) -> compareValues(a, b));

                    // Create auto buckets
                    int bucketSize = Math.max(1, values.size() / numBuckets);
                    ret = new ArrayList<>();

                    for (int i = 0; i < numBuckets && i * bucketSize < values.size(); i++) {
                        int startIdx = i * bucketSize;
                        int endIdx = Math.min((i + 1) * bucketSize, values.size());

                        if (i == numBuckets - 1) {
                            endIdx = values.size(); // Last bucket gets all remaining
                        }

                        List<Map<String, Object>> bucketDocs = new ArrayList<>();
                        Object minValue = values.get(startIdx);
                        Object maxValue = values.get(endIdx - 1);

                        for (int j = startIdx; j < endIdx; j++) {
                            bucketDocs.add(valueToDoc.get(values.get(j)));
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
                            }

                            ret.add(bucketResult);
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
                    Map<String, List<Map<String, Object>>> database = inMemDriver.getDatabase(morphium.getConfig().getDatabase());
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
                            toVisit.add(startValue);

                            int currentDepth = 0;
                            while (!toVisit.isEmpty() && (maxDepth == null || currentDepth < maxDepth)) {
                                Queue<Object> nextLevel = new LinkedList<>();

                                while (!toVisit.isEmpty()) {
                                    Object searchValue = toVisit.poll();
                                    if (visited.contains(searchValue)) continue;
                                    visited.add(searchValue);

                                    // Find matching documents
                                    for (Map<String, Object> foreignDoc : foreignCollection) {
                                        Object connectToValue = foreignDoc.get(connectToField);
                                        if (Objects.equals(searchValue, connectToValue)) {
                                            graphResults.add(new HashMap<>(foreignDoc));

                                            // Add connectFromField value for next level
                                            Object connectFromValue = foreignDoc.get(connectFromField);
                                            if (connectFromValue != null && !visited.contains(connectFromValue)) {
                                                nextLevel.add(connectFromValue);
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

            case "$indexStats":
            case "$geoNear":
            case "$collStats":
            case "$listSessions":
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

    /**
     * Helper method to execute a single aggregation step
     */
    private List<Map<String, Object>> executeAggregationStep(Map<String, Object> step, List<Map<String, Object>> inputData, String stageName, Object stageOp) {
        List<Map<String, Object>> originalData = this.data;
        this.data = inputData;

        List<Map<String, Object>> result = new ArrayList<>();

        try {
            // Execute the step using the existing logic
            for (Map.Entry<String, Object> entry : step.entrySet()) {
                String stage = entry.getKey();
                Object op = entry.getValue();

                switch (stage) {
                    case "$match":
                        // Reuse existing $match logic
                        Query q = null;
                        if (op instanceof Query) {
                            q = (Query) op;
                        } else if (op instanceof Expr) {
                            // Handle Expr-based match
                            for (Map<String, Object> doc : data) {
                                try {
                                    Object matchResult = ((Expr) op).evaluate(doc);
                                    if (matchResult instanceof Boolean && (Boolean) matchResult) {
                                        result.add(doc);
                                    }
                                } catch (Exception e) {
                                    // Skip documents that cause evaluation errors
                                }
                            }
                            return result;
                        } else if (op instanceof Map) {
                            q = getMorphium().createQueryFor(getSearchType());
                            q.setCollectionName(getCollectionName());
                            QueryHelper.parseQueryObject((Map<String, Object>) op, q);
                        }

                        if (q != null) {
                            for (Map<String, Object> o : data) {
                                if (QueryHelper.matchesQuery(getSearchType(), q, o)) {
                                    result.add(o);
                                }
                            }
                        }
                        break;

                    case "$group":
                        // Reuse existing $group logic - simplified version
                        if (op instanceof Map) {
                            Map<String, Object> groupSpec = (Map<String, Object>) op;
                            Map<Object, List<Map<String, Object>>> groups = new HashMap<>();

                            // Group documents
                            for (Map<String, Object> doc : data) {
                                Object groupKey = extractGroupKey(groupSpec.get("_id"), doc);
                                groups.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(doc);
                            }

                            // Compute group results
                            for (Map.Entry<Object, List<Map<String, Object>>> group : groups.entrySet()) {
                                Map<String, Object> groupResult = new HashMap<>();
                                groupResult.put("_id", group.getKey());

                                for (Map.Entry<String, Object> spec : groupSpec.entrySet()) {
                                    if (!"_id".equals(spec.getKey())) {
                                        Object value = computeGroupValue(spec.getValue(), group.getValue());
                                        groupResult.put(spec.getKey(), value);
                                    }
                                }
                                result.add(groupResult);
                            }
                        }
                        break;

                    default:
                        // For other stages, just pass through the data
                        result = new ArrayList<>(data);
                }
            }
        } finally {
            this.data = originalData;
        }

        return result;
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

    /**
     * Helper method to extract group key from group specification
     */
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
     * Helper method to compute group values for aggregation operations like $sum, $avg, etc.
     */
    private Object computeGroupValue(Object valueSpec, List<Map<String, Object>> groupDocs) {
        if (valueSpec instanceof Expr) {
            // For Expr objects, assume we want to compute sum or count
            Expr expr = (Expr) valueSpec;

            // Check if it's a simple integer (likely a count)
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
                    return null;
            }
        }

        return valueSpec;
    }

    /**
     * Helper method to extract value from document based on field reference or expression
     */
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
}
