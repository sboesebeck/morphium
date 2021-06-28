package de.caluga.morphium.aggregation;

import de.caluga.morphium.Collation;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.morphium.Utils;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
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
 * <p/>
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
        Map<String, Object> map = Utils.getMap("$project", p);

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
//        Map<String, Object> o = Utils.getMap("$project", m);
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

        Map<String, Object> o = Utils.getMap("$addFields", ret);
        params.add(o);
        return this;
    }

    @Override
    public Aggregator<T, R> match(Query<T> q) {
        Map<String, Object> o = Utils.getMap("$match", q.toQueryObject());
        if (collectionName == null)
            collectionName = q.getCollectionName();
        params.add(o);
        return this;
    }

    @Override
    public Aggregator<T, R> matchSubQuery(Query<?> q) {
        Map<String, Object> o = Utils.getMap("$match", q.toQueryObject());
        params.add(o);
        return this;
    }

    @Override
    public Aggregator<T, R> match(Expr q) {
        params.add(Utils.getMap("$match", Utils.getMap("$expr", q.toQueryObject())));
        return this;
    }

    @Override
    public Aggregator<T, R> limit(int num) {
        Map<String, Object> o = Utils.getMap("$limit", num);
        params.add(o);
        return this;
    }

    @Override
    public Aggregator<T, R> skip(int num) {
        Map<String, Object> o = Utils.getMap("$skip", num);
        params.add(o);
        return this;
    }


    @Override
    public Aggregator<T, R> unwind(Expr listField) {
        Map<String, Object> o = Utils.getMap("$unwind", listField);
        params.add(o);
        return this;
    }

    @Override
    public Aggregator<T, R> unwind(String listField) {
        Map<String, Object> o = Utils.getMap("$unwind", listField);
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
        Map<String, Object> o = Utils.getMap("$sort", sort);
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
    public void setCollectionName(String cn) {
        collectionName = cn;
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
        List<Map<String, Object>> pipeline = new ArrayList<>(getPipeline());
        pipeline.add(Utils.getMap("$count", "num"));
        List<Map<String, Object>> res = null;
        try {
            res = getMorphium().getDriver().aggregate(getMorphium().getConfig().getDatabase(), getCollectionName(), pipeline, isExplain(), isUseDisk(), getCollation(), getMorphium().getReadPreferenceForClass(getSearchType()));
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
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
            try {
                morphium.getDriver().aggregate(morphium.getConfig().getDatabase(), getCollectionName(), getPipeline(), isExplain(), isUseDisk(), getCollation(), morphium.getReadPreferenceForClass(getSearchType()));
            } catch (MorphiumDriverException e) {
                throw new RuntimeException(e);
            }
        } else {

            morphium.queueTask(() -> {
                try {
                    long start = System.currentTimeMillis();
                    List<R> result = deserializeList();

                    callback.onOperationSucceeded(AsyncOperationType.READ, null, System.currentTimeMillis() - start, result, null, AggregatorImpl.this);
                } catch (MorphiumDriverException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private List<R> deserializeList() throws MorphiumDriverException {
        List<Map<String, Object>> r = morphium.getDriver().aggregate(morphium.getConfig().getDatabase(), getCollectionName(), getPipeline(), isExplain(), isUseDisk(), getCollation(), morphium.getReadPreferenceForClass(getSearchType()));
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
    public List<Map<String, Object>> aggregateMap() {
        try {
            return morphium.getDriver().aggregate(morphium.getConfig().getDatabase(), getCollectionName(), getPipeline(), isExplain(), isUseDisk(), getCollation(), morphium.getReadPreferenceForClass(getSearchType()));
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void aggregateMap(AsyncOperationCallback<Map<String, Object>> callback) {
        if (callback == null) {
            try {
                morphium.getDriver().aggregate(morphium.getConfig().getDatabase(), getCollectionName(), getPipeline(), isExplain(), isUseDisk(), getCollation(), morphium.getReadPreferenceForClass(getSearchType()));
            } catch (MorphiumDriverException e) {
                throw new RuntimeException(e);
            }

        } else {

            morphium.queueTask(() -> {
                try {
                    long start = System.currentTimeMillis();
                    List<Map<String, Object>> ret = morphium.getDriver().aggregate(morphium.getConfig().getDatabase(), getCollectionName(), getPipeline(), isExplain(), isUseDisk(), getCollation(), morphium.getReadPreferenceForClass(getSearchType()));
                    callback.onOperationSucceeded(AsyncOperationType.READ, null, System.currentTimeMillis() - start, ret, null, AggregatorImpl.this);
                } catch (MorphiumDriverException e) {
                    LoggerFactory.getLogger(AggregatorImpl.class).error("error", e);
                }
            });
        }
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
        params.add(Utils.getMap("$count", fld));
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
        Map<String, Object> m = Utils.getMap("$bucket", Utils.getMap("groupBy", groupBy.toQueryObject())
                .add("boundaries", bn)
                .add("default", preset.toQueryObject())
                .add("output", Utils.getQueryObjectMap(output))
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
        Utils.UtilsMap<String, Object> bucketAuto = Utils.getMap("groupBy", groupBy.toQueryObject());
        bucketAuto.add("buckets", numBuckets);
        Utils.UtilsMap<String, Object> map = Utils.getMap("$bucketAuto", bucketAuto);

        if (out != null)
            bucketAuto.add("output", out);
        if (granularity != null)
            bucketAuto.add("granularity", granularity.getValue());
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
            m.put("latencyStats", Utils.getMap("histograms", latencyHistograms));
        }
        if (scale != null) {
            m.put("storageStats", Utils.getMap("scale", scale));
        }
        if (count) {
            m.put("count", new HashMap<>());
        }
        if (queryExecStats) {
            m.put("queryExecStats", new HashMap<>());
        }

        params.add(Utils.getMap("$collStats", m));

        return this;
    }


    //{ $currentOp: { allUsers: <boolean>, idleConnections: <boolean>, idleCursors: <boolean>, idleSessions: <boolean>, localOps: <boolean> } }
    @Override
    public Aggregator<T, R> currentOp(boolean allUsers, boolean idleConnections, boolean idleCursors, boolean idleSessions, boolean localOps) {
        params.add(Utils.getMap("$currentOp", Utils.getMap("allUsers", allUsers)
                .add("idleConnections", idleConnections)
                .add("idleCursors", idleCursors)
                .add("idleSessions", idleSessions)
                .add("localOps", localOps)
        ));
        return this;
    }

    @Override
    public Aggregator<T, R> facetExpr(Map<String, Expr> facets) {
        Map<String, Object> map = Utils.getQueryObjectMap(facets);
        params.add(Utils.getMap("$facet", map));
        return this;
    }

    @Override
    public Aggregator<T, R> facet(Map<String, Aggregator> facets) {

        Map<String, Object> map = new HashMap<>();

        for (Map.Entry<String, Aggregator> e : facets.entrySet()) {
            map.put(e.getKey(), e.getValue().getPipeline());
        }
        params.add(Utils.getMap("$facet", map));
        return this;
    }

    @Override
    public Aggregator<T, R> geoNear(Map<GeoNearFields, Object> param) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (Map.Entry<GeoNearFields, Object> e : param.entrySet()) {
            map.put(e.getKey().name(), ((ObjectMapperImpl) morphium.getMapper()).marshallIfNecessary(e.getValue()));
        }
        params.add(Utils.getMap("$geoNear", map));
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
    public Aggregator<T, R> graphLookup(String fromCollection, Expr startWith, String connectFromField, String connectToField, String as, Integer maxDepth, String depthField, Query restrictSearchWithMatch) {
        Utils.UtilsMap<String, Object> add = Utils.getMap("from", (Object) fromCollection)
                .add("startWith", startWith.toQueryObject())
                .add("connectFromField", connectFromField)
                .add("connectToField", connectToField)
                .add("as", as);
        params.add(Utils.getMap("$graphLookup", add));
        if (maxDepth != null)
            add.add("maxDepth", maxDepth);
        if (depthField != null)
            add.add("depthField", depthField);
        if (restrictSearchWithMatch != null)
            add.add("restrictSearchWithMatch", restrictSearchWithMatch.toQueryObject());

        return this;
    }

    @Override
    public Aggregator<T, R> indexStats() {
        params.add(Utils.getMap("$indexStats", new HashMap<>()));
        return this;
    }

    @Override
    public Aggregator<T, R> listLocalSessionsAllUsers() {
        params.add(Utils.getMap("$listLocalSessions", Utils.getMap("allUsers", true)));
        return this;
    }

    @Override
    public Aggregator<T, R> listLocalSessions() {
        params.add(Utils.getMap("$listLocalSessions", new HashMap<>()));
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
            usersList.add(Utils.getMap(users.get(i), dbs.get(j)));
        }
        params.add(Utils.getMap("$listLocalSessions", Utils.getMap("users", usersList)));
        return this;
    }

    @Override
    public Aggregator<T, R> listSessionsAllUsers() {
        params.add(Utils.getMap("$listSessions", Utils.getMap("allUsers", true)));
        return this;
    }

    @Override
    public Aggregator<T, R> listSessions() {
        params.add(Utils.getMap("$listSessions", new HashMap<>()));
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
            usersList.add(Utils.getMap(users.get(i), dbs.get(j)));
        }
        params.add(Utils.getMap("$listSessions", Utils.getMap("users", usersList)));
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
        Utils.UtilsMap<String, Object> m = Utils.getMap("from", fromCollection);
        if (localField != null)
            m.add("localField", localField);
        if (foreignField != null)
            m.add("foreignField", foreignField);
        if (outputArray != null)
            m.add("as", outputArray);
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
        params.add(Utils.getMap("$lookup", m));
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
    public Aggregator<T, R> merge(Class<?> intoCollection, Map<String, Expr> let, List<Map<String, Expr>> machedPipeline, MergeActionWhenMatched matchAction, MergeActionWhenNotMatched notMatchedAction, String... onFields) {
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
    private Aggregator<T, R> merge(String intoDb, String intoCollection, Map<String, Expr> let, List<Map<String, Expr>> pipeline, MergeActionWhenMatched matchAction, MergeActionWhenNotMatched notMatchedAction, String... onFields) {
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
        Map doc = Utils.getMap("into", Utils.getMap("db", intoDb).add("coll", intoCollection));
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

        params.add(Utils.getMap("$merge", doc));
        return this;
    }

    @Override
    public Aggregator<T, R> out(String collectionName) {
        params.add(Utils.getMap("$out", Utils.getMap("coll", collectionName)));
        return this;
    }

    @Override
    public Aggregator<T, R> out(String db, String collectionName) {
        params.add(Utils.getMap("$out", Utils.getMap("coll", collectionName)
                .add("db", db)
        ));
        return this;
    }

    @Override
    public Aggregator<T, R> planCacheStats(Map<String, Object> param) {
        params.add(Utils.getMap("$planCacheStats", new HashMap<>()));
        return this;
    }


    /**
     * redact needs to resolve to $$DESCEND, $$PRUNE, or $$KEEP
     * <p>
     * System Variable	Description
     * $$DESCEND	$redact returns the fields at the current document level, excluding embedded documents. To include embedded documents and embedded documents within arrays, apply the $cond expression to the embedded documents to determine access for these embedded documents.
     * $$PRUNE	$redact excludes all fields at this current document/embedded document level, without further inspection of any of the excluded fields. This applies even if the excluded field contains embedded documents that may have different access levels.
     * $$KEEP	$redact returns or keeps all fields at this current document/embedded document level, without further inspection of the fields at this level. This applies even if the included field contains embedded documents that may have different access levels.
     */
    @Override
    public Aggregator<T, R> redact(Expr redact) {
        params.add(Utils.getMap("$redact", redact.toQueryObject()));
        return this;
    }

    @Override
    public Aggregator<T, R> replaceRoot(Expr newRoot) {
        params.add(Utils.getMap("$replaceRoot", Utils.getMap("newRoot", newRoot.toQueryObject())));
        return this;
    }

    @Override
    public Aggregator<T, R> replaceWith(Expr newDoc) {
        params.add(Utils.getMap("$replaceWith", newDoc.toQueryObject()));
        return this;
    }

    @Override
    public Aggregator<T, R> sample(int sampleSize) {
        params.add(Utils.getMap("$sample", Utils.getMap("size", sampleSize)));
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
        Map<String, Object> o = Utils.getMap("$set", Utils.getQueryObjectMap(param));
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
        params.add(Utils.getMap("$sortByCount", sortby.toQueryObject()));
        return this;
    }

    @Override
    public Aggregator<T, R> unionWith(String collection) {
        params.add(Utils.getMap("$unionWith", collection));
        return this;
    }

    @Override
    public Aggregator<T, R> unionWith(Aggregator agg) {
        params.add(Utils.getMap("$unionWith", Utils.getMap("coll", (Object) collectionName)
                .add("pipeline", agg.getPipeline())
        ));
        return this;
    }

    @Override
    public Aggregator<T, R> unset(List<String> field) {
        params.add(Utils.getMap("$unset", field));
        return this;
    }

    @Override
    public Aggregator<T, R> unset(String... param) {
        params.add(Utils.getMap("$unset", Arrays.asList(param)));
        return this;
    }

    @Override
    public Aggregator<T, R> unset(@SuppressWarnings("rawtypes") Enum... field) {
        List<String> lst = Arrays.stream(field).map(Enum::name).collect(Collectors.toList());
        params.add(Utils.getMap("$unset", lst));
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
        params.add(Utils.getMap(stageName, param));
        return this;
    }

    @Override
    public Aggregator<T, R> collation(Collation collation) {
        this.collation = collation;
        return this;
    }
}
