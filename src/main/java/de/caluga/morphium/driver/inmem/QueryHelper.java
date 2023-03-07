package de.caluga.morphium.driver.inmem;

import java.math.BigDecimal;
import java.text.Collator;
import java.text.ParseException;
import java.text.RuleBasedCollator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Collation;
import de.caluga.morphium.MongoType;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;

public class QueryHelper {
    private static final Logger log = LoggerFactory.getLogger(QueryHelper.class);

    public static boolean matchesQuery(Map<String, Object> query, Map<String, Object> toCheck, Map<String, Object> collation) {
        if (query.isEmpty()) {
            return true;
        }

        // if (query.containsKey("$where")) {
        //     System.setProperty("polyglot.engine.WarnInterpreterOnly", "false");
        //     ScriptEngineManager mgr = new ScriptEngineManager();
        //     ScriptEngine engine = mgr.getEngineByExtension("js");
        //     // engine.eval("print('Hello World!');");
        //     engine.getContext().setAttribute("obj", toCheck, ScriptContext.ENGINE_SCOPE);
        //
        //     for (String k : toCheck.keySet()) {
        //         engine.getContext().setAttribute(k, toCheck.get(k), ScriptContext.ENGINE_SCOPE);
        //     }
        //
        //     // engine.getContext().setAttribute("this", toCheck, ScriptContext.ENGINE_SCOPE);
        //     try {
        //         Object result = engine.eval((String) query.get("$where"));
        //
        //         if (result == null || result.equals(Boolean.FALSE)) { return false; }
        //     } catch (ScriptException e) {
        //         throw new RuntimeException("Scripting error", e);
        //     }
        // }
        //noinspection LoopStatementThatDoesntLoop
        boolean ret = false;

        for (String keyQuery : query.keySet()) {
            switch (keyQuery) {
            case "$and": {
                // list of field queries
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> lst = ((List<Map<String, Object>>) query.get(keyQuery));

                for (Map<String, Object> q : lst) {
                    if (!matchesQuery(q, toCheck, collation)) {
                        return false;
                    }
                }

                return true;
            }

            case "$or": {
                // list of or queries
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> lst = ((List<Map<String, Object>>) query.get(keyQuery));

                for (Map<String, Object> q : lst) {
                    if (matchesQuery(q, toCheck, collation)) {
                        return true;
                    }
                }

                return false;
            }

            case "$not": {
                //noinspection unchecked
                return (!matchesQuery((Map<String, Object>) query.get(keyQuery), toCheck, collation));
            }

            case "$nor": {
                // list of or queries
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> lst = ((List<Map<String, Object>>) query.get(keyQuery));

                for (Map<String, Object> q : lst) {
                    if (matchesQuery(q, toCheck, collation)) {
                        return false;
                    }
                }

                return true;
            }

            case "$expr": {
                Expr expr = Expr.parse(query.get(keyQuery));
                var result = expr.evaluate(toCheck);

                if (result instanceof Expr) {
                    result = ((Expr) result).evaluate(toCheck);
                }

                return Boolean.TRUE.equals(result);
            }

            case "$where":
                ret = runWhere(query, toCheck);
                continue;

            default:

                // field check
                if (query.get(keyQuery) instanceof Map) {
                    // probably a query operand
                    @SuppressWarnings("unchecked")
                    Map<String, Object> commandMap = (Map<String, Object>) query.get(keyQuery);
                    String commandKey = commandMap.keySet().iterator().next();

                    if (keyQuery.equals("$expr")) {
                        Expr e = Expr.parse(commandMap);
                        return Boolean.TRUE.equals(e.evaluate(toCheck));
                    }

                    Collator coll = null;

                    if (collation != null && !collation.isEmpty()) {
                        coll = getCollator(collation);
                        // add support for caseLevel: <boolean>,
                        //   caseFirst: <string>,
                        //   strength: <int>,
                        //   numericOrdering: <boolean>,
                        //   alternate: <string>,
                        //   maxVariable: <string>,
                        //   backwards: <boolean>
                    }

                    Object checkValue = toCheck;

                    if (keyQuery.contains(".")) {
                        var pth = keyQuery.split("\\.");
                        checkValue = toCheck;

                        for (String p : pth) {
                            if (checkValue == null) {
                                break;
                            }

                            if (checkValue instanceof Map) {
                                checkValue = ((Map) checkValue).get(p);
                            } else if (checkValue instanceof List) {
                                try {
                                    int idx = Integer.valueOf(p);
                                    checkValue = ((List) checkValue).get(Integer.valueOf(p));
                                } catch (Exception e) {
                                    //not an integer, probably some reference _internal_ to the list
                                    var lst = new ArrayList<>();

                                    for (var o : ((List) checkValue)) {
                                        if (o instanceof Map) {
                                            lst.add(((Map) o).get(p));
                                        }
                                    }

                                    checkValue = lst;
                                    break;                     //cannot go further usually!
                                }
                            }
                        }
                    } else {
                        checkValue = toCheck.get(keyQuery);
                    }

                    switch (commandKey) {
                    case "$where":
                        return runWhere(query, toCheck);

                    case "$eq":
                        if (checkValue == null && commandMap.get(commandKey) == null) {
                            return true;
                        }

                        if (checkValue != null && commandMap.get(commandKey) == null) {
                            return false;
                        }

                        if (checkValue == null && commandMap.get(commandKey) != null) {
                            return false;
                        }

                        if (coll != null && (checkValue instanceof String)) {
                            return coll.equals((String) checkValue, (String) commandMap.get(commandKey));
                        }

                        if (checkValue instanceof List) {
                            return ((List) checkValue).contains(commandMap.get(commandKey));
                        }

                        //noinspection unchecked
                        return checkValue.equals(commandMap.get(commandKey));

                    case "$lte":
                    case "$lt":
                        List lst = null;
                        int offset = 0;

                        if (commandKey.equals("$lt")) { offset = -1; }

                        if (checkValue instanceof List) {
                            lst = (List) checkValue;
                        } else {
                            lst = List.of(checkValue);
                        }

                        for (var cv : lst) {
                            //noinspection unchecked
                            if (coll != null && (cv instanceof String)) {
                                if (coll.compare(cv, commandMap.get(commandKey)) <= offset) { return true; }

                                continue;
                            }

                            if (cv == null) {
                                return commandMap.get(commandKey) != null;
                            }

                            if (cv instanceof Number && commandMap.get(commandKey) instanceof Number) {
                                if (Double.valueOf(((Number) cv).doubleValue()).compareTo(((Number) commandMap.get(commandKey)).doubleValue()) <= offset) { return true; }

                                continue;
                            }

                            try {
                                if (((Comparable) cv).compareTo(commandMap.get(commandKey)) <= offset) { return true; }

                                continue;
                            } catch (Exception e) {
                                //type mismatch?
                                continue;
                            }
                        }

                        return false;

                    case "$gte":
                    case "$gt":
                        lst = null;
                        offset = 0;

                        if (commandKey.equals("$gt")) { offset = 1; }

                        if (checkValue instanceof List) {
                            lst = (List) checkValue;
                        } else {
                            lst = List.of(checkValue);
                        }

                        for (var cv : lst) {
                            //noinspection unchecked
                            if (coll != null && (cv instanceof String)) {
                                if (coll.compare(cv, commandMap.get(commandKey)) >= offset) { return true; }

                                continue;
                            }

                            if (cv == null) {
                                return commandMap.get(commandKey) != null;
                            }

                            if (cv instanceof Number && commandMap.get(commandKey) instanceof Number) {
                                if (Double.valueOf(((Number) cv).doubleValue()).compareTo(((Number) commandMap.get(commandKey)).doubleValue()) >= offset) { return true; }

                                continue;
                            }

                            try {
                                if (((Comparable) cv).compareTo(commandMap.get(commandKey)) >= offset) { return true; }

                                continue;
                            } catch (Exception e) {
                                //type mismatch?
                                continue;
                            }
                        }

                        return false;

                    case "$mod":
                        Number n = (Number) checkValue;
                        List arr = (List) commandMap.get(commandKey);
                        int div = ((Integer) arr.get(0));
                        int rem = ((Integer) arr.get(1));
                        return n.intValue() % div == rem;

                    case "$ne":
                        //noinspection unchecked
                        boolean contains = false;

                        if (checkValue instanceof List) {
                            @SuppressWarnings("unchecked")
                            List chk = Collections.synchronizedList(new CopyOnWriteArrayList((List) checkValue));

                            for (Object o : chk) {
                                if (coll != null && (o instanceof String)) {
                                    if (coll.equals((String) o, (String) commandMap.get(commandKey))) {
                                        contains = true;
                                        break;
                                    }
                                }

                                if (o != null && commandMap.get(commandKey) != null && o.equals(commandMap.get(commandKey))) {
                                    contains = true;
                                    break;
                                }
                            }

                            return !contains;
                        }

                        if (checkValue == null && commandMap.get(commandKey) != null) {
                            return true;
                        }

                        if (checkValue == null && commandMap.get(commandKey) == null) {
                            return false;
                        }

                        //noinspection unchecked
                        if (coll != null && (checkValue instanceof String)) {
                            return coll.compare(checkValue, commandMap.get(commandKey)) != 0;
                        }

                        if (checkValue instanceof List) {
                            return !((List) checkValue).contains(commandMap.get(commandKey));
                        }

                        return !checkValue.equals(commandMap.get(commandKey));

                    case "$exists":
                        boolean exists = (checkValue != null);

                        if (exists && (checkValue instanceof List)) {
                            exists = !((List)checkValue).isEmpty();
                        }

                        if (commandMap.get(commandKey).equals(Boolean.TRUE) || commandMap.get(commandKey).equals("true") || commandMap.get(commandKey).equals(1)) {
                            return exists;
                        } else {
                            return !exists;
                        }

                    case "$nin":
                        boolean found = false;

                        for (Object v : (List) commandMap.get(commandKey)) {
                            if (v instanceof MorphiumId) {
                                v = new ObjectId(v.toString());
                            }

                            if (checkValue == null) {
                                if (v == null) {
                                    found = true;
                                }
                            } else {
                                if (coll != null && checkValue instanceof String && coll.equals((String) toCheck.get(keyQuery), (String) v)) {
                                    found = true;
                                    break;
                                }

                                if (checkValue instanceof MorphiumId && v instanceof ObjectId) {
                                    if (checkValue.toString().equals(v.toString())) {
                                        found = true;
                                        break;
                                    }
                                }

                                if (checkValue.equals(v)) {
                                    found = true;
                                    break;
                                }

                                if (checkValue instanceof List) {
                                    for (Object v2 : (List) checkValue) {
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
                        for (Object v : (List) commandMap.get(commandKey)) {
                            if (v instanceof MorphiumId) {
                                v = new ObjectId(v.toString());
                            }

                            if (checkValue == null && v == null) {
                                return true;
                            }

                            if (coll != null && checkValue instanceof String && coll.equals((String) toCheck.get(keyQuery), (String) v)) {
                                return true;
                            }

                            if (checkValue != null && toCheck.get(keyQuery).equals(v)) {
                                return true;
                            }

                            if (checkValue != null && toCheck.get(keyQuery) instanceof List) {
                                for (Object v2 : (List) checkValue) {
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
                        Expr e = (Expr) commandMap.get(commandKey);
                        Object ev = e.evaluate(toCheck);
                        return ev != null && (ev.equals(Boolean.TRUE) || ev.equals(1) || ev.equals("true"));

                    case "$regex":
                    case "$regularExpression":
                    case "$text":
                        Object valtoCheck = null;

                        if (keyQuery.contains(".")) {
                            // path
                            String[] b = keyQuery.split("\\.");
                            Map<String, Object> val = toCheck;

                            for (int i = 0; i < b.length; i++) {
                                String el = b[i];
                                Object candidate = val.get(el);

                                if (candidate == null) {
                                    // did not find path in object
                                    return false;
                                }

                                if (!(candidate instanceof Map) && i < b.length - 1) {
                                    // found path element, but it is not a map
                                    return false;
                                } else if (i < b.length - 1) {
                                    val = (Map) candidate;
                                }

                                if (i == b.length - 1) {
                                    valtoCheck = candidate;
                                }
                            }
                        } else {
                            valtoCheck = checkValue;
                        }

                        int opts = 0;

                        if (commandMap.containsKey("$options")) {
                            String opt = commandMap.get("$options").toString().toLowerCase();

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
                                log.warn("There is no proper equivalent for the 'x' option" + " in mongodb!");
                                //
                                // opts=opts|Pattern.COMMENTS;
                            }
                        }

                        if (valtoCheck == null) {
                            return false;
                        }

                        if (commandKey.equals("$text")) {
                            String srch = commandMap.get("$text").toString().toLowerCase();
                            String[] tokens = null;

                            if (srch.contains("\"")) {
                                // phrase search
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
                                if (s.isEmpty() || s.isBlank()) {
                                    continue;
                                }

                                if (!v.contains(s)) {
                                    found = false;
                                    break;
                                }
                            }

                            return found;
                        } else {
                            String regex = null;

                            if (commandMap.get("$regex") != null) {
                                if (commandMap.get("$regex") instanceof Map) {
                                    var r = commandMap.get("$regex");

                                    if (r instanceof Map) {
                                        regex = (String)((Map)r).get("pattern");
                                    }
                                } else {
                                    regex=(String)commandMap.get("$regex");
                                }
                            }

                            if (regex == null) {
                                var r = commandMap.get("$regularExpression");

                                if (r instanceof Map) {
                                    regex = (String)((Map)r).get("pattern");
                                }
                            }

                            if (regex == null) {
                                return false;
                            }

                            if (!regex.startsWith("^")) {
                                regex = ".*" + regex;
                            }

                            Pattern p = Pattern.compile(regex, opts);
                            return p.matcher(valtoCheck.toString()).matches();
                        }

                    case "$type":
                        MongoType type = null;

                        if (commandMap.get(commandKey) instanceof Integer) {
                            type = MongoType.findByValue((Integer) commandMap.get(commandKey));
                        } else if (commandMap.get(commandKey) instanceof String) {
                            type = MongoType.findByTxt((String) commandMap.get(commandKey));
                        } else {
                            log.error("Type specification needs to be either int or string -" + " not " + commandMap.get(commandKey).getClass().getName());
                            return false;
                        }

                        List elements = new ArrayList();

                        if (checkValue instanceof List) {
                            elements = (List) checkValue;
                        } else {
                            elements.add(checkValue);
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

                            if (fnd) {
                                break;
                            }
                        }

                        return fnd;

                    case "$jsonSchema":
                    case "$geoIntersects":
                        break;

                    case "$nearSphere":
                    case "$near":
                        if (!(checkValue instanceof List)) {
                            log.warn("No proper coordinates found");
                            return false;
                        }

                        if ((commandMap.get(commandKey) instanceof Map)) {
                            Map qmap = ((Map) commandMap.get(commandKey));
                            Map geo = (Map) qmap.get("$geometry");

                            if (geo.containsKey("type")) {
                                if (!geo.get("type").equals("Point")) {
                                    log.warn("$near needs a point as parameter");
                                    return false;
                                }

                                List<Double> coord = (List<Double>) geo.get("coordinates");

                                if (coord == null || coord.size() != 2) {
                                    log.warn("Coordinates for point wrong!");
                                    return false;
                                }

                                double lat1 = (Double) coord.get(1);
                                double lon1 = (Double) coord.get(0);
                                double lat2 = ((List<Double>) checkValue).get(1);
                                double lon2 = ((List<Double>) checkValue).get(0);
                                // The math module contains a function
                                // named toRadians which converts from
                                // degrees to radians.
                                lon1 = Math.toRadians(lon1);
                                lon2 = Math.toRadians(lon2);
                                lat1 = Math.toRadians(lat1);
                                lat2 = Math.toRadians(lat2);
                                // Haversine toRadiansent,eol,start
                                double dlon = lon2 - lon1;
                                double dlat = lat2 - lat1;
                                double a = Math.pow(Math.sin(dlat / 2), 2) + Math.cos(lat1) * Math.cos(lat2) * Math.pow(Math.sin(dlon / 2), 2);
                                double c = 2 * Math.asin(Math.sqrt(a));
                                // Radius of earth
                                double r = 6371;
                                double distance = c * r;

                                if (qmap.containsKey("$minDistance") && distance < (Double) qmap.get("$minDistance")) {
                                    return false;
                                }

                                if (qmap.containsKey("$maxDistance") && distance > (Double) qmap.get("$maxDistance")) {
                                    return false;
                                }

                                if (!qmap.containsKey("$maxDistance") && !qmap.containsKey("$minDistance")) {
                                    log.warn("No distance given?");
                                    return true;
                                }

                                return true;
                            }

                            return false;
                        } else {
                            log.error("Could not parse near query");
                            return false;
                        }

                    case "$geoWithin":
                        if (!(checkValue instanceof List)) {
                            log.warn("No proper coordinates found");
                            return false;
                        }

                        if (!(commandMap.get(commandKey) instanceof Map)) {
                            log.warn("No proper geoWithin query for " + keyQuery);
                            return false;
                        }

                        Map<String, Object> geoQuery = (Map<String, Object>) commandMap.get(commandKey);
                        List coordToCheckWithin = (List) checkValue;

                        if (geoQuery.containsKey("$box")) {
                            // coordinates=list of 2 points
                            List coords = (List) geoQuery.get("$box");

                            if (coords.size() > 2) {
                                log.error("Box coordinates should be 2 Points!");
                            } else if (coords.size() < 2) {
                                log.error("No proper box coordinates");
                                return false;
                            }

                            // checking on a flat surface!
                            var upperLeftX = (Double)((List)(coords.get(0))).get(0);
                            var upperLeftY = (Double)((List)(coords.get(0))).get(1);
                            var lowerRightX = (Double)((List)(coords.get(1))).get(0);
                            var lowerRightY = (Double)((List)(coords.get(1))).get(1);

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
                                // single coordinate
                                var x = (Double)((List) coordToCheckWithin).get(0);
                                var y = (Double)((List) coordToCheckWithin).get(1);

                                if (x > lowerRightX || x < upperLeftX) {
                                    return false;
                                }

                                if (y > lowerRightY || y < upperLeftY) {
                                    return false;
                                }

                                return true;
                            }

                            for (Object o : coordToCheckWithin) {
                                for (List<Double> coord : ((List<List<Double>>) o)) {
                                    var x = coord.get(0);
                                    var y = coord.get(1);

                                    if (x > lowerRightX || x < upperLeftX) {
                                        return false;
                                    }

                                    if (y > lowerRightY || y < upperLeftY) {
                                        return false;
                                    }
                                }
                            }

                            return true;
                        }

                        break;

                    case "$all":
                        if (checkValue == null) {
                            return false;
                        }

                        if (!(checkValue instanceof List)) {
                            log.warn("Trying $all on non-list value");
                            return false;
                        }

                        if (!(commandMap.get(commandKey) instanceof List)) {
                            log.warn("Syntax: $all: [ v1,v2,v3... ]");
                            return false;
                        }

                        List toCheckValList = (List) checkValue;
                        List queryValues = (List) commandMap.get(commandKey);

                        for (Object o : queryValues) {
                            if (!toCheckValList.contains(o)) {
                                return false;
                            }
                        }

                        return true;

                    case "$size":
                        if (checkValue == null) {
                            return commandMap.get(commandKey).equals(0);
                        }

                        if (!(checkValue instanceof List)) {
                            log.warn("Trying $size on non-list value");
                            return false;
                        }

                        return commandMap.get(commandKey).equals(((List) checkValue).size());

                    case "$elemMatch":
                        if (checkValue == null) {
                            return false;
                        }

                        if (!(checkValue instanceof List)) {
                            log.warn("Trying $elemMatch on non-list value");
                            return false;
                        }

                        if (!(commandMap.get(commandKey) instanceof Map)) {
                            log.warn("Syntax: $elemMatch { field: QUERYMAP}");
                            return false;
                        }

                        List valList = (List) checkValue;
                        Map<String, Object> queryMap = (Map<String, Object>) commandMap.get(commandKey);

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
                        if (checkValue == null) {
                            return false;
                        }

                        long value = 0;

                        if (commandMap.get(commandKey) instanceof Integer) {
                            value = ((Integer) commandMap.get(commandKey)).longValue();
                        } else if (commandMap.get(commandKey) instanceof Long) {
                            value = ((Long) commandMap.get(commandKey));
                        } else if (commandMap.get(commandKey) instanceof List) {
                            for (Object o : ((List) commandMap.get(commandKey))) {
                                if (o instanceof Integer) {
                                    value = value | 1L << ((Integer) o);
                                }
                            }
                        } else if (commandMap.get(commandKey) instanceof byte[]) {
                            var b = (byte[]) commandMap.get(commandKey);
                            // long
                            var bits = 0;

                            for (int idx = b.length - 1; idx > 0; idx++) {
                                value = value | (b[idx] << bits);
                                bits += 8;
                            }
                        }

                        long chkVal = 0;

                        if (checkValue instanceof Integer) {
                            chkVal = ((Integer) checkValue).longValue();
                        } else if (checkValue instanceof Long) {
                            chkVal = ((Long) checkValue);
                        } else if (checkValue instanceof Byte) {
                            chkVal = ((Byte) checkValue).byteValue();
                        }

                        if (commandKey.equals("$bitsAnySet")) {
                            return (value & chkVal) != 0;
                        } else if (commandKey.equals("$bitsAllSet")) {
                            return (value & chkVal) == value;
                        } else if (commandKey.equals("$bitsAnyClear")) {
                            return (value & chkVal) < value;
                        } else {                                                                                         // if (k.equals("$bitsAllClear")
                            return (value & chkVal) == 0;
                        }

                    case "$options":
                        break;

                    default:

                        //equals check
                        if (toCheck.containsKey(commandKey)) {
                            return toCheck.get(commandKey).equals(commandMap.get(commandKey));
                        }

                        if (keyQuery.equals("value")) {
                            return false;
                        }

                        return commandMap.equals(toCheck);
                    }
                } else {
                    if (keyQuery.contains(".")) {
                        // getting the proper value
                        var path = keyQuery.split("\\.");
                        Object current = toCheck;
                        Object value = null;

                        for (int i = 0; i < path.length; i++) {
                            var k = path[i];

                            if (current instanceof Map && ((Map) current).get(k) == null) {
                                if (query.get(keyQuery) == null) {
                                    return true;
                                }

                                return false;
                            }

                            if (current instanceof Map && ((Map) current).get(k) instanceof List) {
                                List cnd = (List)((Map) current).get(k);
                                Integer idx = Integer.valueOf(path[i + 1]);

                                if (idx >= cnd.size()) {
                                    current = null;

                                    if (query.get(keyQuery) == null) {
                                        return true;
                                    }

                                    continue;
                                }

                                value = cnd.get(idx);
                                current = cnd.get(idx);
                                i++;
                            } else if (Map.class.isAssignableFrom(((Map) current).get(k).getClass())) {
                                current = (Map<String, Object>)((Map) current).get(k);
                            } else {
                                value = ((Map) current).get(k);
                            }
                        }

                        return value.equals(query.get(keyQuery));
                    }

                    if (toCheck.get(keyQuery) == null && query.get(keyQuery) == null) {
                        return true;
                    }

                    if (toCheck.get(keyQuery) == null && query.get(keyQuery) != null) {
                        return false;
                    }

                    // value comparison - should only be one here
                    assert(query.size() == 1);

                    if (toCheck.get(keyQuery) instanceof MorphiumId || toCheck.get(keyQuery) instanceof ObjectId) {
                        return toCheck.get(keyQuery).toString().equals(query.get(keyQuery).toString());
                    }

                    if (toCheck.get(keyQuery) instanceof List) {
                        return ((List) toCheck.get(keyQuery)).contains(query.get(keyQuery));
                    }

                    return toCheck.get(keyQuery).equals(query.get(keyQuery));
                }
            }
        }

        return ret;
    }

    private static boolean runWhere(Map<String, Object> query, Map<String, Object> toCheck) {
        System.setProperty("polyglot.engine.WarnInterpreterOnly", "false");
        ScriptEngineManager mgr = new ScriptEngineManager();
        ScriptEngine engine = mgr.getEngineByExtension("js");

        // engine.eval("print('Hello World!');");
        if (engine != null) {
            engine.getContext().setAttribute("obj", toCheck, ScriptContext.ENGINE_SCOPE);

            for (String k : toCheck.keySet()) {
                engine.getContext().setAttribute(k, toCheck.get(k), ScriptContext.ENGINE_SCOPE);
            }

            // engine.getContext().setAttribute("this", toCheck, ScriptContext.ENGINE_SCOPE);
            try {
                Object result = engine.eval((String) query.get("$where"));
                //if (result == null || result.equals(Boolean.FALSE)) { return false; }
                return result.equals(Boolean.TRUE);
            } catch (ScriptException e) {
                throw new RuntimeException("Scripting error", e);
            }
        } else {
            log.error("Could not create javscript engine!");
        }

        return false;
    }

    public static Collator getCollator(Map<String, Object> collation) {
        Locale locale = Locale.ROOT;

        if (collation == null) {
            return null;
        }

        if (collation.containsKey("locale") && !"simple".equals(collation.get("locale"))) {
            locale = Locale.forLanguageTag((String) collation.get("locale"));
        }

        var coll = Collator.getInstance(locale);

        if (collation.containsKey("caseFirst")) {
            if (Collation.CaseFirst.UPPER.getMongoText().equals(collation.get("caseFirst"))) {
                try {
                    coll = new RuleBasedCollator(
                        ((RuleBasedCollator) coll).getRules() + ",A<a,B<b,C<c,D<d,E<e,F<f,G<g,H<h,I<i,J<j,K<k,L<l,M<m,N<n,O<o,P<p,Q<q,R<r,S<s,T<t,U<u,V<v,W<w,X<x,Y<y,Z<z,Ö<ö,Ü<ü,Ä<ä");
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        if (collation.containsKey("strength")) {
            coll.setStrength((Integer) collation.get("strength"));
        }

        return coll;
    }
}
