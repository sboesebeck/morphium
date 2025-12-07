package de.caluga.morphium.aggregation;

import de.caluga.morphium.Collation;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.commands.AggregateMongoCommand;
import de.caluga.morphium.driver.commands.ExplainCommand.ExplainVerbosity;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.morphium.Utils;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * User: Stephan Bösebeck
 * Date: 30.08.12
 * Time: 16:24
 * <p>
 */
@SuppressWarnings({"rawtypes", "CommentedOutCode"})
public class AggregatorImpl<T, R> implements Aggregator<T, R> {
    private final List<Map<String, Object>> params = new ArrayList<>();
    private final List<Group<T, R>> groups = new ArrayList<>();
    private Class<? extends T> type;
    private Morphium morphium;
    private Class<? extends R> rType;
    private String collectionName;
    private boolean useDisk = false;
    private boolean explain = false;
    private Collation collation;
    private final Logger log = LoggerFactory.getLogger(AggregatorImpl.class);

    public AggregatorImpl(Morphium morphium, Class<? extends T> type, Class<? extends R> resultType) {
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
            if (e.getValue() instanceof Expr) {
                p.put(e.getKey(), ((Expr) e.getValue()).toQueryObject());
            } else {
                p.put(e.getKey(), e.getValue());
            }
        }

        Map<String, Object> map = UtilsMap.of("$project", p);
        params.add(map);
        return this;
    }

    @Override
    public Aggregator<T, R> project(String fld, Expr e) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(fld, e.toQueryObject());
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
            if (e.getValue() instanceof Expr) {
                ret.put(e.getKey(), ((Expr) e.getValue()).toQueryObject());
            } else {
                ret.put(e.getKey(), e.getValue());
            }
        }

        Map<String, Object> o = UtilsMap.of("$addFields", ret);
        params.add(o);
        return this;
    }

    public Map<String, Object> explain() throws MorphiumDriverException {
        return explain(null);
    }

    @Override
    public Map<String, Object> explain(ExplainVerbosity verbosity) throws MorphiumDriverException {
        var cmd = getAggregateCmd();

        try {
            return cmd.explain(verbosity);
        } finally {
            cmd.releaseConnection();
        }
    }

    @Override
    public Aggregator<T, R> match(Query<T> q) {
        Map<String, Object> o = UtilsMap.of("$match", q.toQueryObject());

        if (collectionName == null)
            collectionName = q.getCollectionName();

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
        params.add(UtilsMap.of("$match", UtilsMap.of("$expr", q.toQueryObject())));
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
    public Aggregator<T, R> unwind(Expr listField) {
        Map<String, Object> o = UtilsMap.of("$unwind", listField);
        params.add(o);
        return this;
    }

    @Override
    public Aggregator<T, R> unwind(String listField) {
        Map<String, Object> o = UtilsMap.of("$unwind", listField);
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
        List<Map<String, Object>> pipeline = getPipeline();
        pipeline.add(Doc.of("$count", "num"));
        List<Map<String, Object>> res = null;
        AggregateMongoCommand cmd = getAggregateCmd();

        try {
            cmd.setPipeline(pipeline);
            res = cmd.execute();
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        } finally {
            cmd.releaseConnection();
        }

        if (res.size() == 0) return 0;

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
            AggregateMongoCommand cmd = getAggregateCmd();

            try {
                log.warn("Async operation but callback is null!");
                cmd.executeAsync();
            } catch (MorphiumDriverException e) {
                throw new RuntimeException(e);
            } finally {
                cmd.releaseConnection();
            }
        } else {
            morphium.queueTask(() -> {
                try {
                    long start = System.currentTimeMillis();
                    List<R> result = deserializeList();
                    callback.onOperationSucceeded(AsyncOperationType.READ, null, System.currentTimeMillis() - start, result, null, AggregatorImpl.this);
                } catch (MorphiumDriverException e) {
                    log.error("error in queued task", e);
                }
            });
        }
    }

    private List<R> deserializeList() throws MorphiumDriverException {
        AggregateMongoCommand cmd = getAggregateCmd();

        try {
            List<Map<String, Object>> r = cmd.execute();
            cmd.releaseConnection();
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
        } finally {
            cmd.releaseConnection();
        }
    }

    @Override
    public List<Map<String, Object>> aggregateMap() {
        AggregateMongoCommand cmd = getAggregateCmd();

        try {
            if (collation != null) cmd.setCollation(Doc.of(getCollation().toQueryObject()));
            return cmd.execute();
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        } finally {
            cmd.releaseConnection();
        }
    }

    @Override
    public void aggregateMap(AsyncOperationCallback<Map<String, Object>> callback) {
        if (callback == null) {
            AggregateMongoCommand cmd = getAggregateCmd();

            try {
                cmd.execute();
            } catch (MorphiumDriverException e) {
                throw new RuntimeException(e);
            } finally {
                cmd.releaseConnection();
            }
        } else {
            morphium.queueTask(() -> {
                AggregateMongoCommand cmd = getAggregateCmd();

                try {
                    long start = System.currentTimeMillis();
                    List<Map<String, Object>> ret = cmd.execute();
                    callback.onOperationSucceeded(AsyncOperationType.READ, null, System.currentTimeMillis() - start, ret, null, AggregatorImpl.this);
                } catch (MorphiumDriverException e) {
                    LoggerFactory.getLogger(AggregatorImpl.class).error("error", e);
                } finally {
                    cmd.releaseConnection();
                }
            });
        }
    }

    public AggregateMongoCommand getAggregateCmd() {
        MongoConnection readConnection = morphium.getDriver().getReadConnection(morphium.getReadPreferenceForClass(getSearchType()));
        AggregateMongoCommand cmd = new AggregateMongoCommand(readConnection);
        cmd.setDb(morphium.getDatabase())
        .setColl(getCollectionName())
        .setPipeline(getPipeline())
        .setExplain(isExplain())
        .setAllowDiskUse(isUseDisk());

        if (collation != null) cmd.setCollation(Doc.of(getCollation().toQueryObject()));
        if (morphium.getReadConcernForClass(getSearchType())!=null){
            cmd.setReadConcern(Map.of("level",morphium.getReadConcernForClass(getSearchType()).name()));
        }

        return cmd;
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

    @Override
    public Aggregator<T, R> out(Class<?> type) {
        return out(morphium.getMapper().getCollectionName(type));
    }

    /**
     * Categorizes incoming documents into groups, called buckets, based on a specified expression and
     * bucket boundaries and outputs a document per each bucket. Each output document contains an _id field
     * whose value specifies the inclusive lower bound of the bucket. The output option specifies
     * the fields included in each output document.
     * <p>
     * $bucket only produces output documents for buckets that contain at least one input document.
     *
     * @param groupBy:    Expression to group by, usually a field name
     * @param boundaries: Boundaries for the  buckets
     * @param preset:     the default, needs to be a literal
     * @param output:     definition of output documents and accumulator
     * @return
     */
    @Override
    public Aggregator<T, R> bucket(Expr groupBy, List<Expr> boundaries, Expr preset, Map<String, Expr> output) {
        Map<String, Object> out = new LinkedHashMap<>();

        for (Map.Entry<String, Expr> e : output.entrySet()) {
            out.put(e.getKey(), e.getValue().toQueryObject());
        }

        List<Object> bn = new ArrayList<>();
        boundaries.forEach(x -> bn.add(x.toQueryObject()));
        Map<String, Object> m = UtilsMap.of("$bucket", (Object) UtilsMap.of("groupBy", groupBy.toQueryObject(),
            "boundaries", bn,
            "default", preset.toQueryObject(),
            "output", Utils.getQueryObjectMap(output))
            );
        params.add(m);
        return this;
    }

    @Override
    public Aggregator<T, R> bucketAuto(Expr groupBy, int numBuckets, Map<String, Expr> output, BucketGranularity granularity) {
        Map<String, Object> out = null;

        if (output != null) {
            out = new LinkedHashMap<>();

            for (Map.Entry<String, Expr> e : output.entrySet()) {
                out.put(e.getKey(), e.getValue().toQueryObject());
            }
        }

        var bucketAuto = UtilsMap.of("groupBy", groupBy.toQueryObject(), "buckets", numBuckets);
        var map = UtilsMap.of("$bucketAuto", (Object) bucketAuto);

        if (out != null)
            bucketAuto.put("output", out);

        if (granularity != null)
            bucketAuto.put("granularity", granularity.getValue());

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
        @SuppressWarnings({"unchecked", "rawtypes"}) Map<String, Object> m = new LinkedHashMap();

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
        params.add(UtilsMap.of("$currentOp", UtilsMap.of("allUsers", allUsers, "idleConnections", idleConnections, "idleCursors", idleCursors,
            "idleSessions", idleSessions,
            "localOps", localOps)
            ));
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
        return graphLookup(morphium.getMapper().getCollectionName(type),
            startWith,
            connectFromField.name(),
            connectToField.name(),
            as,
            maxDepth, depthField, restrictSearchWithMatch);
    }

    @Override
    public Aggregator<T, R> graphLookup(Class<?> type, Expr startWith, String connectFromField, String connectToField, String as, Integer maxDepth, String depthField, Query restrictSearchWithMatch) {
        return graphLookup(morphium.getMapper().getCollectionName(type),
            startWith,
            connectFromField,
            connectToField,
            as,
            maxDepth, depthField, restrictSearchWithMatch);
    }

    @Override
    public Aggregator<T, R> graphLookup(String fromCollection, Expr startWith, String connectFromField, String connectToField, String as, Integer maxDepth, String depthField,
        Query restrictSearchWithMatch) {
        Map<String, Object> add = UtilsMap.of("from", (Object) fromCollection,
            "startWith", startWith.toQueryObject(),
            "connectFromField", connectFromField,
            "connectToField", connectToField,
            "as", as);
        params.add(UtilsMap.of("$graphLookup", add));

        if (maxDepth != null)
            add.put("maxDepth", maxDepth);

        if (depthField != null)
            add.put("depthField", depthField);

        if (restrictSearchWithMatch != null)
            add.put("restrictSearchWithMatch", restrictSearchWithMatch.toQueryObject());

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
        return lookup(getMorphium().getMapper().getCollectionName(fromType),
            localField.name(), foreignField.name(), outputArray, pipeline, let
            );
    }

    @Override
    public Aggregator<T, R> lookup(String fromCollection, String localField, String foreignField, String outputArray, List<Expr> pipeline, Map<String, Expr> let) {
        Map<String, Object> m = UtilsMap.of("from", fromCollection);

        if (localField != null)
            m.put("localField", localField);

        if (foreignField != null)
            m.put("foreignField", foreignField);

        if (outputArray != null)
            m.put("as", outputArray);

        if (pipeline != null && pipeline.size() > 0) {
            List lst = new ArrayList();

            for (Expr e : pipeline) {
                //noinspection unchecked
                lst.add(e.toQueryObject());
            }

            m.put("pipeline", lst);
        }

        if (let != null) {
            Map<String, Object> map = new HashMap<>();

            for (Map.Entry<String, Expr> e : let.entrySet()) {
                map.put(e.getKey(), e.getValue().toQueryObject());
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
    public Aggregator<T, R> merge(Class<?> intoCollection, Map<String, Expr> let, List<Map<String, Expr>> machedPipeline, MergeActionWhenMatched matchAction, MergeActionWhenNotMatched notMatchedAction,
        String... onFields) {
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

        Map doc = UtilsMap.of("into", UtilsMap.of("db", intoDb, "coll", intoCollection));

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
    public Aggregator<T, R> out(String db, String collectionName) {
        params.add(UtilsMap.of("$out", UtilsMap.of("coll", collectionName, "db", db)
            ));
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
     * System Variable  Description
     * $$DESCEND    $redact returns the fields at the current document level, excluding embedded documents. To include embedded documents and embedded documents within arrays, apply the $cond expression to the embedded documents to determine access for these embedded documents.
     * $$PRUNE  $redact excludes all fields at this current document/embedded document level, without further inspection of any of the excluded fields. This applies even if the excluded field contains embedded documents that may have different access levels.
     * $$KEEP   $redact returns or keeps all fields at this current document/embedded document level, without further inspection of the fields at this level. This applies even if the included field contains embedded documents that may have different access levels.
     */
    @Override
    public Aggregator<T, R> redact(Expr redact) {
        params.add(UtilsMap.of("$redact", redact.toQueryObject()));
        return this;
    }

    @Override
    public Aggregator<T, R> replaceRoot(Expr newRoot) {
        params.add(UtilsMap.of("$replaceRoot", UtilsMap.of("newRoot", newRoot.toQueryObject())));
        return this;
    }

    @Override
    public Aggregator<T, R> replaceWith(Expr newDoc) {
        params.add(UtilsMap.of("$replaceWith", newDoc.toQueryObject()));
        return this;
    }

    @Override
    public Aggregator<T, R> sample(int sampleSize) {
        params.add(UtilsMap.of("$sample", UtilsMap.of("size", sampleSize)));
        return this;
    }


    /**
     * Adds new fields to documents. $set outputs documents that contain all existing fields from the input documents and newly added fields.
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
     * The $sortByCount stage is equivalent to the following $group + $sort sequence:
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
        params.add(UtilsMap.of("$sortByCount", sortby.toQueryObject()));
        return this;
    }

    @Override
    public Aggregator<T, R> unionWith(String collection) {
        params.add(UtilsMap.of("$unionWith", collection));
        return this;
    }

    @Override
    public Aggregator<T, R> unionWith(Aggregator agg) {
        params.add(UtilsMap.of("$unionWith", UtilsMap.of("coll", (Object) collectionName,
            "pipeline", agg.getPipeline())
            ));
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
    public Aggregator<T, R> unset(@SuppressWarnings("rawtypes") Enum... field) {
        List<String> lst = Arrays.stream(field).map(Enum::name).collect(Collectors.toList());
        params.add(UtilsMap.of("$unset", lst));
        return this;
    }

    @Override
    public Aggregator<T, R> genericStage(String stageName, Object param) {
        if (param instanceof Expr) {
            param = ((Expr) param).toQueryObject();
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
}
