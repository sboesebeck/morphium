package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.Collation;
import de.caluga.morphium.MongoType;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.math.BigDecimal;
import java.text.Collator;
import java.text.ParseException;
import java.text.RuleBasedCollator;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;

public class QueryHelper {
    private static final Logger log = LoggerFactory.getLogger(QueryHelper.class);


    public static boolean matchesQuery(Map<String, Object> query, Map<String, Object> toCheck, Map<String, Object> collation) {
        if (query.isEmpty()) {
            return true;
        }
        if (query.containsKey("$where")) {
            System.setProperty("polyglot.engine.WarnInterpreterOnly", "false");
            ScriptEngine engine = new ScriptEngineManager().getEngineByName("javascript");
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
                        if (!matchesQuery(q, toCheck, collation)) {
                            return false;
                        }
                    }
                    return true;
                }
                case "$or": {
                    //list of or queries
                    @SuppressWarnings("unchecked") List<Map<String, Object>> lst = ((List<Map<String, Object>>) query.get(key));
                    for (Map<String, Object> q : lst) {
                        if (matchesQuery(q, toCheck, collation)) {
                            return true;
                        }
                    }
                    return false;

                }
                case "$not": {
                    //noinspection unchecked
                    return (!matchesQuery((Map<String, Object>) query.get(key), toCheck, collation));
                }
                case "$nor": {
                    //list of or queries
                    @SuppressWarnings("unchecked") List<Map<String, Object>> lst = ((List<Map<String, Object>>) query.get(key));

                    for (Map<String, Object> q : lst) {
                        if (matchesQuery(q, toCheck, collation)) {
                            return false;
                        }
                    }
                    return true;

                }
                case "$expr": {
                    Expr expr = Expr.parse(query.get(key));
                    var result = expr.evaluate(toCheck);
                    if (result instanceof Expr) {
                        result = ((Expr) result).evaluate(toCheck);
                    }
                    return Boolean.TRUE.equals(result);
                }
                default:
                    //field check

                    if (query.get(key) instanceof Map) {
                        //probably a query operand
                        @SuppressWarnings("unchecked") Map<String, Object> q = (Map<String, Object>) query.get(key);
                        String k = q.keySet().iterator().next();
                        if (key.equals("$expr")) {
                            Expr e = Expr.parse(q);
                            return Boolean.TRUE.equals(e.evaluate(toCheck));
                        }
                        Collator coll = null;
                        if (collation != null && !collation.isEmpty()) {
                            coll = getCollator(collation);
                            //add support for caseLevel: <boolean>,
                            //   caseFirst: <string>,
                            //   strength: <int>,
                            //   numericOrdering: <boolean>,
                            //   alternate: <string>,
                            //   maxVariable: <string>,
                            //   backwards: <boolean>


                        }

                        switch (k) {

                            case "$eq":
                                if (toCheck.get(key) == null && q.get(k) == null) return true;
                                if (toCheck.get(key) != null && q.get(k) == null) return false;
                                if (toCheck.get(key) == null && q.get(k) != null) return false;
                                if (coll != null && (toCheck.get(key) instanceof String))
                                    return coll.equals((String) toCheck.get(key), (String) q.get(k));

                                //noinspection unchecked
                                return ((Comparable) toCheck.get(key)).compareTo(q.get(k)) == 0;
                            case "$lt":
                                //noinspection unchecked
                                if (coll != null && (toCheck.get(key) instanceof String))
                                    return coll.compare(toCheck.get(key), q.get(k)) < 0;
                                return ((Comparable) toCheck.get(key)).compareTo(q.get(k)) < 0;
                            case "$lte":
                                //noinspection unchecked
                                if (coll != null && (toCheck.get(key) instanceof String))
                                    return coll.compare(toCheck.get(key), q.get(k)) <= 0;
                                return ((Comparable) toCheck.get(key)).compareTo(q.get(k)) <= 0;
                            case "$gt":
                                //noinspection unchecked
                                if (coll != null && (toCheck.get(key) instanceof String))
                                    return coll.compare(toCheck.get(key), q.get(k)) > 0;
                                return ((Comparable) toCheck.get(key)).compareTo(q.get(k)) > 0;
                            case "$gte":
                                //noinspection unchecked
                                if (coll != null && (toCheck.get(key) instanceof String))
                                    return coll.compare(toCheck.get(key), q.get(k)) >= 0;
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
                                    @SuppressWarnings("unchecked") List chk = Collections.synchronizedList(new CopyOnWriteArrayList((List) toCheck.get(key)));
                                    for (Object o : chk) {
                                        if (coll != null && (o instanceof String)) {
                                            if (coll.equals((String) o, (String) q.get(k))) {
                                                contains = true;
                                                break;
                                            }
                                            ;
                                        }
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
                                if (coll != null && (toCheck.get(key) instanceof String))
                                    return coll.compare(toCheck.get(key), q.get(k)) != 0;
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
                                        if (coll != null && toCheck.get(key) instanceof String && coll.equals((String) toCheck.get(key), (String) v)) {
                                            found = true;
                                            break;
                                        }
                                        if (toCheck.get(key).equals(v)) {
                                            found = true;
                                            break;
                                        }
                                        if (toCheck.get(key) instanceof List) {
                                            for (Object v2 : (List) toCheck.get(key)) {
                                                if (coll != null && coll.equals((String) v2, (String) v)) {
                                                    found = true;
                                                    break;
                                                }
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
                                    if (coll != null && toCheck.get(key) instanceof String && coll.equals((String) toCheck.get(key), (String) v)) {
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
                            case "$regex":
                            case "$text":
                                Object valtoCheck = null;
                                if (key.contains(".")) {
                                    //path
                                    String[] b = key.split("\\.");
                                    Map<String, Object> val = toCheck;
                                    for (int i = 0; i < b.length; i++) {
                                        String el = b[i];
                                        Object candidate = val.get(el);
                                        if (candidate == null) {
                                            //did not find path in object
                                            return false;
                                        }
                                        if (!(candidate instanceof Map) && i < b.length - 1) {
                                            //found path element, but it is not a map
                                            return false;
                                        } else if (i < b.length - 1) {
                                            val = (Map) candidate;
                                        }
                                        if (i == b.length - 1) {
                                            valtoCheck = candidate;
                                        }
                                    }
                                } else {
                                    valtoCheck = toCheck.get(key);
                                }
                                int opts = 0;
                                if (q.containsKey("$options")) {
                                    String opt = q.get("$options").toString().toLowerCase();
                                    if (opt.contains("i")) {
                                        opts = opts | Pattern.CASE_INSENSITIVE;
                                    }
                                    if (opt.contains("m")) {
                                        opts = opts | Pattern.MULTILINE;
                                    }
                                    if (opt.contains("s")) {
                                        opts = opts | Pattern.DOTALL;
                                    }
                                    if (opt.contains("x")) {
                                        log.warn("There is no proper equivalent for the 'x' option in mongodb!");
//                                        opts=opts|Pattern.COMMENTS;
                                    }

                                }
                                if (valtoCheck == null) return false;
                                if (k.equals("$text")) {
                                    String srch = q.get("$text").toString().toLowerCase();
                                    String[] tokens = null;
                                    if (srch.contains("\"")) {
                                        //phrase search
                                        List<String> tks = new ArrayList<>();
                                        Pattern p = Pattern.compile("\"([^\"]*)\"");
                                        String t = p.matcher(srch).group(1);
                                        srch = srch.replaceAll("\"" + t + "\"", "");
                                        tks.add(t);
                                        tks.addAll(Arrays.asList(srch.split(" ")));
                                    } else {
                                        srch = srch.replaceAll("[^a-zA-Z0-9 ]", " ");
                                        tokens = srch.split(" ");
                                    }

                                    var v = valtoCheck.toString().toLowerCase();
                                    found = true;
                                    for (String s : tokens) {
                                        if (s.isEmpty() || s.isBlank()) continue;
                                        if (!v.contains(s)) {
                                            found = false;
                                            break;
                                        }
                                    }
                                    return found;
                                } else {
                                    String regex = q.get("$regex").toString();
                                    if (!regex.startsWith("^")) {
                                        regex = ".*" + regex;
                                    }
                                    Pattern p = Pattern.compile(regex, opts);
                                    return p.matcher(valtoCheck.toString()).matches();
                                }
                            case "$type":
                                MongoType type = null;
                                if (q.get(k) instanceof Integer) {
                                    type = MongoType.findByValue((Integer) q.get(k));
                                } else if (q.get(k) instanceof String) {
                                    type = MongoType.findByTxt((String) q.get(k));
                                } else {
                                    log.error("Type specification needs to be either int or string - not " + q.get(k).getClass().getName());
                                    return false;
                                }
                                List elements = new ArrayList();
                                if (toCheck.get(key) instanceof List) {
                                    elements = (List) toCheck.get(key);
                                } else {
                                    elements.add(toCheck.get(key));
                                }

                                boolean fnd = false;
                                for (Object o : elements) {
                                    if (o == null) {
                                        fnd = type.equals(MongoType.NULL);
                                    } else if (o instanceof byte[]) {
                                        fnd = type.equals(MongoType.BINARY_DATA);
                                    } else if ((o instanceof List) || (o.getClass().isArray())) {
                                        fnd = type.equals(MongoType.ARRAY);
                                    } else if (o instanceof Pattern) {
                                        fnd = type.equals(MongoType.REGEX);
                                    } else if (o instanceof Map) {
                                        fnd = type.equals(MongoType.OBJECT);
                                    } else if ((o instanceof Double)) {
                                        fnd = type.equals(MongoType.DOUBLE);
                                    } else if ((o instanceof Date)) {
                                        fnd = type.equals(MongoType.DATE);
                                    } else if ((o instanceof MorphiumId) || (o instanceof ObjectId)) {
                                        fnd = type.equals(MongoType.OBJECT_ID);
                                    } else if ((o instanceof BigDecimal)) {
                                        fnd = type.equals(MongoType.DECIMAL);
                                    } else if ((o instanceof String)) {
                                        fnd = type.equals(MongoType.STRING);
                                    } else if ((o instanceof Boolean)) {
                                        fnd = type.equals(MongoType.BOOLEAN);
                                    } else if ((o instanceof Float)) {
                                        fnd = type.equals(MongoType.DOUBLE);
                                    } else if ((o instanceof Long)) {
                                        fnd = type.equals(MongoType.LONG);
                                    } else if ((o instanceof Integer)) {
                                        fnd = type.equals(MongoType.INTEGER);
                                    }
                                    if (fnd) break;
                                }
                                return fnd;
                            case "$jsonSchema":
                            case "$geoIntersects":
                            case "$near":
                            case "$nearSphere":
                                break;
                            case "$geoWithin":
                                if (!(toCheck.get(key) instanceof List)) {
                                    log.warn("No proper coordinates found");
                                    return false;
                                }
                                if (!(q.get(k) instanceof Map)) {
                                    log.warn("No proper geoWithin query for " + key);
                                    return false;
                                }
                                Map<String, Object> geoQuery = (Map<String, Object>) q.get(k);
                                List coordToCheckWithin = (List) toCheck.get(key);
                                if (geoQuery.containsKey("$box")) {
                                    //coordinates=list of 2 points
                                    List coords = (List) geoQuery.get("$box");
                                    if (coords.size() > 2) {
                                        log.error("Box coordinates should be 2 Points!");
                                    } else if (coords.size() < 2) {
                                        log.error("No proper box coordinates");
                                        return false;
                                    }
                                    //checking on a flat surface!
                                    var upperLeftX = (Double) ((List) (coords.get(0))).get(0);
                                    var upperLeftY = (Double) ((List) (coords.get(0))).get(1);
                                    var lowerRightX = (Double) ((List) (coords.get(1))).get(0);
                                    var lowerRightY = (Double) ((List) (coords.get(1))).get(1);

                                    if (lowerRightX < upperLeftX) {
                                        var tmp = upperLeftX;
                                        upperLeftX = lowerRightX;
                                        lowerRightX = tmp;
                                    }
                                    if (lowerRightY < upperLeftY) {
                                        var tmp = upperLeftY;
                                        upperLeftY = lowerRightY;
                                        lowerRightY = tmp;
                                    }

                                    if (coordToCheckWithin.size() == 2 && coordToCheckWithin.get(0) instanceof Double) {
                                        //single coordinate
                                        var x = (Double) ((List) coordToCheckWithin).get(0);
                                        var y = (Double) ((List) coordToCheckWithin).get(1);
                                        if (x > lowerRightX || x < upperLeftX) return false;
                                        if (y > lowerRightY || y < upperLeftY) return false;
                                        return true;
                                    }
                                    for (Object o : coordToCheckWithin) {
                                        for (List<Double> coord : ((List<List<Double>>) o)) {
                                            var x = coord.get(0);
                                            var y = coord.get(1);
                                            if (x > lowerRightX || x < upperLeftX) return false;
                                            if (y > lowerRightY || y < upperLeftY) return false;
                                        }
                                    }
                                    return true;
                                }
                                break;
                            case "$all":
                                if (toCheck.get(key) == null) return false;
                                if (!(toCheck.get(key) instanceof List)) {
                                    log.warn("Trying $all on non-list value");
                                    return false;
                                }
                                if (!(q.get(k) instanceof List)) {
                                    log.warn("Syntax: $all: [ v1,v2,v3... ]");
                                    return false;
                                }
                                List toCheckValList = (List) toCheck.get(key);
                                List queryValues = (List) q.get(k);

                                for (Object o : queryValues) {
                                    if (!toCheckValList.contains(o)) return false;
                                }
                                return true;
                            case "$size":
                                if (toCheck.get(key) == null) return q.get(k).equals(0);
                                if (!(toCheck.get(key) instanceof List)) {
                                    log.warn("Trying $size on non-list value");
                                    return false;
                                }
                                return q.get(k).equals(((List) toCheck.get(key)).size());
                            case "$elemMatch":
                                if (toCheck.get(key) == null) return false;
                                if (!(toCheck.get(key) instanceof List)) {
                                    log.warn("Trying $elemMatch on non-list value");
                                    return false;
                                }
                                if (!(q.get(k) instanceof Map)) {
                                    log.warn("Syntax: $elemMatch { field: QUERYMAP}");
                                    return false;
                                }
                                List valList = (List) toCheck.get(key);
                                Map<String, Object> queryMap = (Map<String, Object>) q.get(k);

                                for (Object o : valList) {
                                    if (!(o instanceof Map)) {
                                        o = Doc.of("value", o);
                                    }
                                    if (matchesQuery(queryMap, (Map<String, Object>) o, null) || matchesQuery(Doc.of("value", queryMap), (Map<String, Object>) o, null)) {
                                        return true;
                                    }
                                }

                                return false;

                            case "$bitsAnySet":
                            case "$bitsAllClear":
                            case "$bitsAllSet":
                            case "$bitsAnyClear":
                                if (toCheck.get(key) == null) return false;
                                long value = 0;
                                if (q.get(k) instanceof Integer) {
                                    value = ((Integer) q.get(k)).longValue();
                                } else if (q.get(k) instanceof Long) {
                                    value = ((Long) q.get(k));
                                } else if (q.get(k) instanceof List) {
                                    for (Object o : ((List) q.get(k))) {
                                        if (o instanceof Integer) {
                                            value = value | 1L << ((Integer) o);
                                        }
                                    }
                                } else if (q.get(k) instanceof byte[]) {
                                    var b = (byte[]) q.get(k);

                                    //long
                                    var bits = 0;
                                    for (int idx = b.length - 1; idx > 0; idx++) {
                                        value = value | (b[idx] << bits);
                                        bits += 8;
                                    }
                                }
                                long chkVal = 0;
                                if (toCheck.get(key) instanceof Integer) {
                                    chkVal = ((Integer) toCheck.get(key)).longValue();
                                } else if (toCheck.get(key) instanceof Long) {
                                    chkVal = ((Long) toCheck.get(key));
                                } else if (toCheck.get(key) instanceof Byte) {
                                    chkVal = ((Byte) toCheck.get(key)).byteValue();
                                }
                                if (k.equals("$bitsAnySet")) {
                                    return (value & chkVal) != 0;
                                } else if (k.equals("$bitsAllSet")) {
                                    return (value & chkVal) == value;
                                } else if (k.equals("$bitsAnyClear")) {
                                    return (value & chkVal) < value;
                                } else { //if (k.equals("$bitsAllClear")
                                    return (value & chkVal) == 0;
                                }
                            case "$options":
                                break;
                            default:
                                throw new RuntimeException("Unknown Operator " + k);
                        }


                    } else {
                        if (key.contains(".")) {
                            //getting the proper value
                            var path = key.split("\\.");
                            var current = toCheck;
                            Object value = null;
                            for (var k : path) {
                                if (current.get(k) == null) {
                                    if (query.get(key) == null) return true;
                                    return false;
                                }
                                if (Map.class.isAssignableFrom(current.get(k).getClass())) {
                                    current = (Map<String, Object>) current.get(k);
                                } else {
                                    value = current.get(k);
                                }
                            }
                            return value.equals(query.get(key));
                        }
                        if (toCheck.get(key) == null && query.get(key) == null) return true;
                        if (toCheck.get(key) == null && query.get(key) != null) return false;
                        //value comparison - should only be one here
                        assert (query.size() == 1);

                        if (toCheck.get(key) instanceof MorphiumId || toCheck.get(key) instanceof ObjectId) {
                            return toCheck.get(key).toString().equals(query.get(key).toString());
                        }

                        return toCheck.get(key).equals(query.get(key));

                    }
            }
        }
        return false;
    }

    public static Collator getCollator(Map<String, Object> collation) {
        Locale locale = Locale.ROOT;
        if (collation.containsKey("locale") && !"simple".equals(collation.get("locale"))) {
            locale = Locale.forLanguageTag((String) collation.get("locale"));
        }
        var coll = Collator.getInstance(locale);
        if (collation.containsKey("caseFirst")) {
            if (Collation.CaseFirst.UPPER.getMongoText().equals(collation.get("caseFirst"))) {
                try {
                    coll = new RuleBasedCollator(((RuleBasedCollator) coll).getRules() + ",A<a,B<b,C<c,D<d,E<e,F<f,G<g,H<h,I<i,J<j,K<k,L<l,M<m,N<n,O<o,P<p,Q<q,R<r,S<s,T<t,U<u,V<v,W<w,X<x,Y<y,Z<z,Ö<ö,Ü<ü,Ä<ä");
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        if (collation.containsKey("strength"))
            coll.setStrength((Integer) collation.get("strength"));
        return coll;
    }

}
