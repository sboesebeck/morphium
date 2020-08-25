package de.caluga.morphium.aggregation;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.query.Query;

import java.util.*;
import java.util.stream.Collectors;

/**
 * User: Stephan Bösebeck
 * Date: 30.08.12
 * Time: 16:24
 * <p/>
 */
public class AggregatorImpl<T, R> implements Aggregator<T, R> {
    private final List<Map<String, Object>> params = new ArrayList<>();
    private final List<Group<T, R>> groups = new ArrayList<>();
    private Class<? extends T> type;
    private Morphium morphium;
    private Class<? extends R> rType;
    private String collectionName;
    private boolean useDisk = false;
    private boolean explain = false;

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
                    fld = morphium.getARHelper().getFieldName(type, fld);
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
        return morphium.aggregate(this);
    }

    @Override
    public void aggregate(final AsyncOperationCallback<R> callback) {
        if (callback == null) {
            morphium.aggregate(this);
        } else {

            morphium.queueTask(() -> {
                long start = System.currentTimeMillis();
                List<R> ret = morphium.aggregate(AggregatorImpl.this);
                callback.onOperationSucceeded(AsyncOperationType.READ, null, System.currentTimeMillis() - start, ret, null, AggregatorImpl.this);
            });
        }
    }

    @Override
    public List<Map<String, Object>> toAggregationList() {
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
        boundaries.stream().forEach(x -> bn.add(x.toQueryObject()));
        Map<String, Object> m = Utils.getMap("bucket", Utils.getMap("groupBy", groupBy.toQueryObject())
                .add("boudaries", bn)
                .add("default", preset)
                .add("output", out)
        );
        params.add(m);
        return this;
    }

    @Override
    public Aggregator<T, R> bucketAuto(Expr groupBy, int numBuckets, Map<String, Expr> output, BucketGranularity granularity) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, Expr> e : output.entrySet()) {
            out.put(e.getKey(), e.getValue().toQueryObject());
        }
        params.add(Utils.getMap("$bucketAuto", Utils.getMap("groupBy", groupBy.toQueryObject())
                .add("buckets", numBuckets)
                .add("output", out)
                .add("granularity", granularity.getValue())
        ));

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
        Map<String, Object> m = new LinkedHashMap();
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
    public Aggregator<T, R> facet(Map<String, Expr> facets) {
        Map<String, Object> map = Utils.getQueryObjectMap(facets);
        params.add(Utils.getMap("$facet", map));
        return this;
    }

    @Override
    public Aggregator<T, R> geoNear(Map<Aggregator.GeoNearFiels, Object> param) {
        params.add(Utils.getMap("$geoNear", Utils.getNoExprMap((Map) param)));
        return this;
    }

    @Override
    public Aggregator<T, R> graphLookup(Class<?> type, Expr startWith, String connectFromField, String connectToField, String as, int maxDepth, String depthField, Query restrictSearchWithMatch) {
        return graphLookup(morphium.getMapper().getCollectionName(type),
                startWith,
                connectFromField,
                connectToField,
                as,
                maxDepth, depthField, restrictSearchWithMatch);
    }

    @Override
    public Aggregator<T, R> graphLookup(String fromCollection, Expr startWith, String connectFromField, String connectToField, String as, int maxDepth, String depthField, Query restrictSearchWithMatch) {
        params.add(Utils.getMap("$graphLookup", Utils.getMap("from", (Object) fromCollection)
                .add("startWith", startWith.toQueryObject())
                .add("connectFromField", connectFromField)
                .add("connectToField", connectToField)
                .add("maxDepth", maxDepth)
                .add("depthField", depthField)
                .add("restrictSearchWithMatch", restrictSearchWithMatch.toQueryObject())
        ));

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


    @Override
    public Aggregator<T, R> lookup(Map<String, Object> param) {
        return null;
    }


    @Override
    public Aggregator<T, R> merge(Map<String, Object> param) {
        return null;
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
                .add("pipeline", agg.toAggregationList())
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
    public Aggregator<T, R> unset(Enum... field) {
        List<String> lst = Arrays.asList(field).stream().map((x) -> x.name()).collect(Collectors.toList());
        params.add(Utils.getMap("$unset",lst));
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
        params.add(Utils.getMap(stageName,param));
        return this;
    }
}
