package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.driver.MorphiumId;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class QueryHelper {
    private static final Logger log = LoggerFactory.getLogger(QueryHelper.class);


    public static boolean matchesQuery(Map<String, Object> query, Map<String, Object> toCheck) {
        if (query.isEmpty()) {
            return true;
        }
        if (query.containsKey("$where")) {
            ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");
            //engine.eval("print('Hello World!');");
            engine.getContext().setAttribute("obj", toCheck, ScriptContext.ENGINE_SCOPE);
            engine.getContext().setAttribute("this", toCheck, ScriptContext.ENGINE_SCOPE);
            try {
                Object result = engine.eval((String) query.get("$where"));
                if (result == null || result.equals(Boolean.FALSE)) return false;
            } catch (ScriptException e) {
                throw new RuntimeException("Scripting error", e);
            }
        }
        //noinspection LoopStatementThatDoesntLoop
        for (String key : query.keySet()) {
            switch (key) {
                case "$and": {
                    //list of field queries
                    @SuppressWarnings("unchecked") List<Map<String, Object>> lst = ((List<Map<String, Object>>) query.get(key));
                    for (Map<String, Object> q : lst) {
                        if (!matchesQuery(q, toCheck)) {
                            return false;
                        }
                    }
                    return true;
                }
                case "$or": {
                    //list of or queries
                    @SuppressWarnings("unchecked") List<Map<String, Object>> lst = ((List<Map<String, Object>>) query.get(key));
                    for (Map<String, Object> q : lst) {
                        if (matchesQuery(q, toCheck)) {
                            return true;
                        }
                    }
                    return false;

                }
                case "$not": {
                    return (!matchesQuery((Map<String, Object>) query.get(key), toCheck));
                }
                case "$nor": {
                    //list of or queries
                    @SuppressWarnings("unchecked") List<Map<String, Object>> lst = ((List<Map<String, Object>>) query.get(key));

                    for (Map<String, Object> q : lst) {
                        if (matchesQuery(q, toCheck)) {
                            return false;
                        }
                    }
                    return true;

                }
                default:
                    //field check
                    if (query.get(key) instanceof Map) {
                        //probably a query operand
                        @SuppressWarnings("unchecked") Map<String, Object> q = (Map<String, Object>) query.get(key);
                        assert (q.size() == 1);
                        String k = q.keySet().iterator().next();
                        if (key.equals("$expr")) {
                            Expr e = Expr.parse(q);
                            return Boolean.TRUE.equals(e.evaluate(toCheck));
                        }

                        switch (k) {
                            case "$eq":
                                if (toCheck.get(key) == null && q.get(k) == null) return true;
                                if (toCheck.get(key) != null && q.get(k) == null) return false;
                                if (toCheck.get(key) == null && q.get(k) != null) return false;
                                return ((Comparable) toCheck.get(key)).compareTo(q.get(k)) == 0;
                            case "$lt":
                                //noinspection unchecked
                                return ((Comparable) toCheck.get(key)).compareTo(q.get(k)) < 0;
                            case "$lte":
                                //noinspection unchecked
                                return ((Comparable) toCheck.get(key)).compareTo(q.get(k)) <= 0;
                            case "$gt":
                                //noinspection unchecked
                                return ((Comparable) toCheck.get(key)).compareTo(q.get(k)) > 0;
                            case "$gte":
                                //noinspection unchecked
                                return ((Comparable) toCheck.get(key)).compareTo(q.get(k)) >= 0;
                            case "$mod":
                                Number n = (Number) toCheck.get(key);
                                List arr = (List) q.get(k);
                                int div = ((Integer) arr.get(0));
                                int rem = ((Integer) arr.get(1));
                                return n.intValue() % div == rem;
                            case "$ne":
                                //noinspection unchecked
                                boolean contains = false;
                                if (toCheck.get(key) instanceof List) {
                                    List chk = Collections.synchronizedList(new CopyOnWriteArrayList((List) toCheck.get(key)));
                                    for (Object o : chk) {
                                        if (o != null && q.get(k) != null && o.equals(q.get(k))) {
                                            contains = true;
                                            break;
                                        }
                                    }
                                    return !contains;
                                }
                                if (toCheck.get(key) == null && q.get(k) != null) return true;
                                if (toCheck.get(key) == null && q.get(k) == null) return false;
                                //noinspection unchecked
                                return ((Comparable) toCheck.get(key)).compareTo(q.get(k)) != 0;
                            case "$exists":
                                boolean exists = (toCheck.containsKey(key));
                                if (q.get(k).equals(Boolean.TRUE) || q.get(k).equals("true") || q.get(k).equals(1)) {
                                    return exists;
                                } else {
                                    return !exists;
                                }
                            case "$nin":
                                boolean found = false;
                                for (Object v : (List) q.get(k)) {
                                    if (v instanceof MorphiumId) {
                                        v = new ObjectId(v.toString());
                                    }
                                    if (toCheck.get(key) == null) {
                                        if (v == null) found = true;
                                    } else {
                                        if (toCheck.get(key).equals(v)) {
                                            found = true;
                                        }
                                        if (toCheck.get(key) instanceof List) {
                                            for (Object v2 : (List) toCheck.get(key)) {
                                                if (v2.equals(v)) {
                                                    found = true;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                                return !found;
                            case "$in":
                                for (Object v : (List) q.get(k)) {
                                    if (v instanceof MorphiumId) {
                                        v = new ObjectId(v.toString());
                                    }
                                    if (toCheck.get(key) == null && v == null) {
                                        return true;
                                    }
                                    if (toCheck.get(key) != null && toCheck.get(key).equals(v)) {
                                        return true;
                                    }
                                    if (toCheck.get(key) != null && toCheck.get(key) instanceof List) {
                                        for (Object v2 : (List) toCheck.get(key)) {
                                            if (v2.equals(v)) {
                                                return true;
                                            }
                                        }
                                    }

                                }
                                return false;

                            case "$comment":
                                continue;
                            case "$expr":
                                Expr e = (Expr) q.get(k);
                                Object ev = e.evaluate(toCheck);
                                return ev != null && (ev.equals(Boolean.TRUE) || ev.equals(1) || ev.equals("true"));
                            case "$jsonSchema":
                            case "$type":
                            case "$regex":
                            case "$text":
                            case "$geoIntersects":
                            case "$geoWithin":
                            case "$near":
                            case "$nearSphere":
                            case "$all":
                            case "$elemMatch":
                            case "$size":
                            case "$bitsAllClear":
                            case "$bitsAllSet":
                            case "$bitsAnyClear":
                            case "$bitsAnySet":
                                log.warn("Unsupported op " + k + " for in memory driver");
                                break;
                            default:
                                throw new RuntimeException("Unknown Operator " + k);
                        }


                    } else {
                        //value comparison - should only be one here
                        assert (query.size() == 1);
//                        if (toCheck.get(key)!=null) {
                        if (toCheck.get(key) instanceof MorphiumId || toCheck.get(key) instanceof ObjectId) {
                            return toCheck.get(key).toString().equals(query.get(key).toString());
                        }
                        if (toCheck.get(key) == null && query.get(key) != null) return false;
                        if (toCheck.get(key) == null && query.get(key) == null) return true;
                        return toCheck.get(key).equals(query.get(key));
//                        }
                    }
            }
        }
        return false;
    }

}
