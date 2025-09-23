package de.caluga.morphium.driver.inmem;

import java.math.BigDecimal;
import java.text.Collator;
import java.text.ParseException;
import java.text.RuleBasedCollator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
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

        // Special handling for map field queries and array index queries
        for (String key : query.keySet()) {
            if (query.get(key) instanceof Map) {
                Map<String, Object> queryMap = (Map<String, Object>) query.get(key);

                if (toCheck.get(key) instanceof Map) {
                    // Both query and document have Maps - standard map field query
                    Map<String, Object> checkMap = (Map<String, Object>) toCheck.get(key);

                    // Check if all key-value pairs in the query map are present in the check map
                    boolean allMatched = true;
                    for (String mapKey : queryMap.keySet()) {
                        if (!checkMap.containsKey(mapKey) || !compareValues(checkMap.get(mapKey), queryMap.get(mapKey), null)) {
                            allMatched = false;
                            break;
                        }
                    }

                    if (allMatched) {
                        return true;
                    }
                } else if (toCheck.get(key) instanceof List) {
                    // Query has Map but document has List - this could be array index access
                    List<?> checkList = (List<?>) toCheck.get(key);

                    // Check if all query map keys are array index operations
                    boolean allMatched = true;
                    for (String mapKey : queryMap.keySet()) {
                        try {
                            int idx = Integer.parseInt(mapKey);
                            Object indexQuery = queryMap.get(mapKey);

                            // Check if the index is within bounds
                            if (idx < 0 || idx >= checkList.size()) {
                                // Index out of bounds
                                if (indexQuery instanceof Map && ((Map<?, ?>) indexQuery).containsKey("$exists")) {
                                    // For $exists queries on out-of-bounds indices, return false
                                    Object existsValue = ((Map<?, ?>) indexQuery).get("$exists");
                                    if (Boolean.TRUE.equals(existsValue) || "true".equals(existsValue) || Integer.valueOf(1).equals(existsValue)) {
                                        allMatched = false;
                                        break;
                                    } else {
                                        // exists: false on out-of-bounds index should return true
                                        continue;
                                    }
                                } else {
                                    allMatched = false;
                                    break;
                                }
                            } else {
                                // Index is within bounds
                                if (indexQuery instanceof Map && ((Map<?, ?>) indexQuery).containsKey("$exists")) {
                                    // For $exists queries on valid indices, return true if exists query expects true
                                    Object existsValue = ((Map<?, ?>) indexQuery).get("$exists");
                                    if (Boolean.TRUE.equals(existsValue) || "true".equals(existsValue) || Integer.valueOf(1).equals(existsValue)) {
                                        // Element exists, so $exists: true should match
                                        continue;
                                    } else {
                                        // Element exists, but $exists: false doesn't match
                                        allMatched = false;
                                        break;
                                    }
                                } else {
                                    // Non-$exists queries on array elements - handle normally
                                    Object arrayElement = checkList.get(idx);
                                    if (arrayElement instanceof Map) {
                                        if (!matchesQuery((Map<String, Object>) indexQuery, (Map<String, Object>) arrayElement, collation)) {
                                            allMatched = false;
                                            break;
                                        }
                                    } else {
                                        // For primitive array elements, create synthetic document
                                        Map<String, Object> syntheticDoc = Map.of("value", arrayElement);
                                        Map<String, Object> elementQuery = Map.of("value", indexQuery);
                                        if (!matchesQuery(elementQuery, syntheticDoc, collation)) {
                                            allMatched = false;
                                            break;
                                        }
                                    }
                                }
                            }
                        } catch (NumberFormatException e) {
                            // Not a numeric index, so this doesn't match
                            allMatched = false;
                            break;
                        }
                    }

                    if (allMatched) {
                        return true;
                    }
                }
            }
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
                        List<Map<String, Object>> lst = ((List<Map<String, Object >> ) query.get(keyQuery));

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
                        List<Map<String, Object>> lst = ((List<Map<String, Object >> ) query.get(keyQuery));

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
                        List<Map<String, Object>> lst = ((List<Map<String, Object >> ) query.get(keyQuery));

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
                        List<String> operators = new ArrayList<>();

                        for (String opKey : commandMap.keySet()) {
                            if (!"$options".equals(opKey)) {
                                operators.add(opKey);
                            }
                        }

                        if (operators.size() > 1) {
                            for (String opKey : operators) {
                                Map<String, Object> singleCommand = new LinkedHashMap<>();
                                singleCommand.put(opKey, commandMap.get(opKey));

                                if (commandMap.containsKey("$options")) {
                                    singleCommand.put("$options", commandMap.get("$options"));
                                }

                                Map<String, Object> singleQuery = new LinkedHashMap<>();
                                singleQuery.put(keyQuery, singleCommand);

                                if (!matchesQuery(singleQuery, toCheck, collation)) {
                                    return false;
                                }
                            }

                            ret = true;
                            continue;
                        }
                        var it = commandMap.keySet().iterator();
                        String commandKey = it.next();

                        while (commandKey.equals("$options") && it.hasNext()) {
                            commandKey = it.next();
                        }

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
                            // Special handling for map field queries like "stringMap.key1"
                            String[] parts = keyQuery.split("\\.", 2);
                            if (parts.length == 2 && toCheck.get(parts[0]) instanceof Map) {
                                Map<String, Object> mapField = (Map<String, Object>) toCheck.get(parts[0]);
                                if (mapField.containsKey(parts[1])) {
                                    checkValue = mapField.get(parts[1]);
                                } else {
                                    // Handle nested paths
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
                                            }
                                        }
                                    }
                                }
                            } else {
                                // Handle nested paths
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
                                        }
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

                                if (checkValue == null || commandMap.get(commandKey) == null) {
                                    return false;
                                }

                                if (checkValue instanceof List) {
                                    for (Object value : (List) checkValue) {
                                        if (compareValues(value, commandMap.get(commandKey), coll)) {
                                            return true;
                                        }
                                    }

                                    return false;
                                }

                                return compareValues(checkValue, commandMap.get(commandKey), coll);

                            case "$lte":
                            case "$lt":
                                List lst = null;
                                int offset = 0;

                                if (commandKey.equals("$lt")) {
                                    offset = -1;
                                }

                                if (checkValue instanceof List) {
                                    lst = (List) checkValue;
                                } else if (checkValue != null) {
                                    lst = List.of(checkValue);
                                }

                                if (lst == null) {
                                    return false;
                                }

                                for (var cv : lst) {
                                    if (cv == null) {
                                        continue;
                                    }

                                    if (compareLessThan(cv, commandMap.get(commandKey), offset, coll)) {
                                        return true;
                                    }
                                }

                                return false;

                            case "$gte":
                            case "$gt":
                                lst = null;
                                offset = 0;

                                if (commandKey.equals("$gt")) {
                                    offset = 1;
                                }

                                if (checkValue instanceof List) {
                                    lst = (List) checkValue;
                                } else if (checkValue != null) {
                                    lst = List.of(checkValue);
                                }

                                if (lst == null) {
                                    return false;
                                }

                                for (var cv : lst) {
                                    if (cv == null) {
                                        continue;
                                    }

                                    if (compareGreaterThan(cv, commandMap.get(commandKey), offset, coll)) {
                                        return true;
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
                                if (checkValue instanceof List) {
                                    for (Object element : (List) checkValue) {
                                        if (compareValues(element, commandMap.get(commandKey), coll)) {
                                            return false;
                                        }
                                    }

                                    return true;
                                }

                                if (checkValue == null && commandMap.get(commandKey) == null) {
                                    return false;
                                }

                                return !compareValues(checkValue, commandMap.get(commandKey), coll);

                            case "$exists":
                                boolean exists;

                                if (keyQuery.contains(".")) {
                                    // For dotted field paths (like array access), the field resolution logic above
                                    // has already resolved the path. We check if it was resolved successfully:
                                    // - If checkValue != toCheck, the path was resolved to some value (including null)
                                    // - If checkValue == toCheck, the path doesn't exist in the document
                                    exists = (checkValue != toCheck);
                                } else {
                                    // For simple field names, use the original fieldExists logic
                                    exists = fieldExists(toCheck, keyQuery);

                                    // If not found with the given field name, try converting to camelCase
                                    // This handles cases where the field was stored with a different naming convention
                                    if (!exists && keyQuery.contains("_")) {
                                        // Convert snake_case to camelCase for field name parts
                                        String camelCaseField = convertSnakeToCamelCase(keyQuery);
                                        exists = fieldExists(toCheck, camelCaseField);
                                    }

                                    // If still not found, try converting from camelCase to snake_case
                                    if (!exists && !keyQuery.contains("_")) {
                                        String snakeCaseField = convertCamelToSnakeCase(keyQuery);
                                        exists = fieldExists(toCheck, snakeCaseField);
                                    }
                                }

                                if (commandMap.get(commandKey).equals(Boolean.TRUE) || commandMap.get(commandKey).equals("true") || commandMap.get(commandKey).equals(1)) {
                                    return exists;
                                }

                                return !exists;

                            case "$nin":
                                boolean found = false;

                                for (Object v : (List) commandMap.get(commandKey)) {
                                    Object normalized = normalizeId(v);

                                    if (checkValue instanceof List) {
                                        for (Object element : (List) checkValue) {
                                            if (compareValues(element, normalized, coll)) {
                                                found = true;
                                                break;
                                            }
                                        }
                                    } else if (compareValues(checkValue, normalized, coll)) {
                                        found = true;
                                    }

                                    if (found) {
                                        break;
                                    }
                                }

                                return !found;

                            case "$in":
                                for (Object v : (List) commandMap.get(commandKey)) {
                                    Object normalized = normalizeId(v);

                                    if (checkValue instanceof List) {
                                        for (Object element : (List) checkValue) {
                                            if (compareValues(element, normalized, coll)) {
                                                return true;
                                            }
                                        }
                                    } else if (compareValues(checkValue, normalized, coll)) {
                                        return true;
                                    }
                                }

                                return false;

                            case "$comment":
                                continue;

                            case "$expr":
                                Expr e = (Expr) commandMap.get(commandKey);
                                Object ev = e.evaluate(toCheck);
                                return ev != null && (ev.equals(Boolean.TRUE) || ev.equals(1) || ev.equals("true"));

                            case "$not":
                                return !(matchesQuery((Map) commandMap.get(commandKey), toCheck, collation));

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
                                        opts |= Pattern.CASE_INSENSITIVE;
                                    }

                                    if (opt.contains("m")) {
                                        opts |= Pattern.MULTILINE;
                                    }

                                    if (opt.contains("s")) {
                                        opts |= Pattern.DOTALL;
                                    }

                                    if (opt.contains("x")) {
                                        opts |= Pattern.COMMENTS;
                                    }
                                }

                                if (valtoCheck == null) {
                                    return false;
                                }

                                if (commandKey.equals("$text")) {
                                    String srch = commandMap.get("$text").toString().toLowerCase();
                                    List<String> tokens = new ArrayList<>();

                                    Pattern phrasePattern = Pattern.compile("\"([^\"]+)\"");
                                    var matcher = phrasePattern.matcher(srch);
                                    while (matcher.find()) {
                                        tokens.add(matcher.group(1));
                                    }

                                    srch = matcher.replaceAll(" ");
                                    srch = srch.replaceAll("[^a-z0-9 ]", " ");
                                    tokens.addAll(Arrays.asList(srch.trim().split(" +")));

                                    String valueText = valtoCheck.toString().toLowerCase();

                                    for (String token : tokens) {
                                        if (token == null || token.isBlank()) {
                                            continue;
                                        }

                                        if (!valueText.contains(token)) {
                                            return false;
                                        }
                                    }

                                    return !tokens.isEmpty();
                                } else {
                                    Pattern regexPattern = buildRegexPattern(commandMap, opts);

                                    if (regexPattern == null) {
                                        return false;
                                    }

                                    if (valtoCheck instanceof List) {
                                        for (Object element : (List) valtoCheck) {
                                            if (element != null && regexPattern.matcher(element.toString()).find()) {
                                                return true;
                                            }
                                        }

                                        return false;
                                    }

                                    return regexPattern.matcher(valtoCheck.toString()).find();
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

                                for (Object o : elements) {
                                    if (matchesType(o, type)) {
                                        return true;
                                    }
                                }

                                return false;

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
                                        for (List<Double> coord : ((List<List<Double >> ) o)) {
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

                            if (collation != null && value instanceof String && query.get(keyQuery) instanceof String) {
                                var c = getCollator(collation);
                                if (c != null) {
                                    return c.equals((String) value, (String) query.get(keyQuery));
                                }
                            }
                            return value != null && value.equals(query.get(keyQuery));
                        }

                        if (toCheck.get(keyQuery) == null && query.get(keyQuery) == null) {
                            return true;
                        }

                        if (toCheck.get(keyQuery) == null && query.get(keyQuery) != null) {
                            return false;
                        }

                        // value comparison - checking specific field in query
                        // Note: query may contain multiple fields due to auto-variables, but we only check keyQuery

                        if (toCheck.get(keyQuery) instanceof MorphiumId || toCheck.get(keyQuery) instanceof ObjectId) {
                            return toCheck.get(keyQuery).toString().equals(query.get(keyQuery).toString());
                        }

                        if (toCheck.get(keyQuery) instanceof List) {
                            List lst = (List) toCheck.get(keyQuery);
                            Object qv = query.get(keyQuery);
                            if (collation != null && qv instanceof String) {
                                var c = getCollator(collation);
                                if (c != null) {
                                    for (Object v2 : lst) {
                                        if (v2 instanceof String && c.equals((String) v2, (String) qv)) return true;
                                    }
                                    return false;
                                }
                            }
                            return lst.contains(qv);
                        }

                        if (collation != null && toCheck.get(keyQuery) instanceof String && query.get(keyQuery) instanceof String) {
                            var c = getCollator(collation);
                            if (c != null) {
                                return c.equals((String) toCheck.get(keyQuery), (String) query.get(keyQuery));
                            }
                        }
                        return toCheck.get(keyQuery) != null && toCheck.get(keyQuery).equals(query.get(keyQuery));
                    }
            }
        }

        return ret;
    }

    private static boolean compareValues(Object left, Object right, Collator coll) {
        Object normalizedLeft = normalizeId(left);
        Object normalizedRight = normalizeId(right);

        if (normalizedLeft == null || normalizedRight == null) {
            return normalizedLeft == null && normalizedRight == null;
        }

        if (normalizedLeft instanceof String && normalizedRight instanceof String && coll != null) {
            return coll.compare((String) normalizedLeft, (String) normalizedRight) == 0;
        }

        if (normalizedLeft instanceof Number && normalizedRight instanceof Number) {
            return Double.compare(((Number) normalizedLeft).doubleValue(), ((Number) normalizedRight).doubleValue()) == 0;
        }

        return normalizedLeft.equals(normalizedRight);
    }

    private static Object normalizeId(Object value) {
        if (value instanceof MorphiumId || value instanceof ObjectId) {
            return value == null ? null : value.toString();
        }

        return value;
    }

    private static boolean compareLessThan(Object left, Object right, int offset, Collator coll) {
        if (left == null || right == null) {
            return false;
        }

        if (left instanceof String && right instanceof String && coll != null) {
            return coll.compare((String) left, (String) right) <= offset;
        }

        if (left instanceof Number && right instanceof Number) {
            return Double.compare(((Number) left).doubleValue(), ((Number) right).doubleValue()) <= offset;
        }

        if (left instanceof Comparable && right instanceof Comparable) {
            try {
                //noinspection unchecked
                return ((Comparable) left).compareTo(right) <= offset;
            } catch (ClassCastException ignored) {
                return false;
            }
        }

        return false;
    }

    private static boolean compareGreaterThan(Object left, Object right, int offset, Collator coll) {
        if (left == null || right == null) {
            return false;
        }

        if (left instanceof String && right instanceof String && coll != null) {
            return coll.compare((String) left, (String) right) >= offset;
        }

        if (left instanceof Number && right instanceof Number) {
            return Double.compare(((Number) left).doubleValue(), ((Number) right).doubleValue()) >= offset;
        }

        if (left instanceof Comparable && right instanceof Comparable) {
            try {
                //noinspection unchecked
                return ((Comparable) left).compareTo(right) >= offset;
            } catch (ClassCastException ignored) {
                return false;
            }
        }

        return false;
    }

    private static Pattern buildRegexPattern(Map<String, Object> commandMap, int baseFlags) {
        int flags = baseFlags;
        Object regexObject = commandMap.get("$regex");

        if (regexObject instanceof Pattern) {
            return (Pattern) regexObject;
        }

        String pattern = null;

        if (regexObject instanceof String) {
            pattern = (String) regexObject;
        }

        Object regularExpression = commandMap.get("$regularExpression");

        if (regularExpression instanceof Map) {
            Map <?, ? > regexMap = (Map <?, ? >) regularExpression;

            if (regexMap.get("pattern") instanceof String) {
                pattern = (String) regexMap.get("pattern");
            }

            Object opt = regexMap.get("options");
            if (opt instanceof String) {
                flags = applyRegexOptions(((String) opt), flags);
            }
        }

        if (pattern == null) {
            return null;
        }

        return Pattern.compile(pattern, flags);
    }

    private static int applyRegexOptions(String options, int flags) {
        String opt = options.toLowerCase(Locale.ROOT);

        if (opt.contains("i")) {
            flags |= Pattern.CASE_INSENSITIVE;
        }

        if (opt.contains("m")) {
            flags |= Pattern.MULTILINE;
        }

        if (opt.contains("s")) {
            flags |= Pattern.DOTALL;
        }

        if (opt.contains("x")) {
            flags |= Pattern.COMMENTS;
        }

        return flags;
    }

    private static boolean matchesType(Object value, MongoType type) {
        if (value == null) {
            return type.equals(MongoType.NULL);
        }

        if (value instanceof byte[]) {
            return type.equals(MongoType.BINARY_DATA);
        }

        if (value instanceof List || value.getClass().isArray()) {
            return type.equals(MongoType.ARRAY);
        }

        if (value instanceof Pattern) {
            return type.equals(MongoType.REGEX);
        }

        if (value instanceof Map) {
            return type.equals(MongoType.OBJECT);
        }

        if (value instanceof Double || value instanceof Float) {
            return type.equals(MongoType.DOUBLE);
        }

        if (value instanceof Date) {
            return type.equals(MongoType.DATE);
        }

        if (value instanceof MorphiumId || value instanceof ObjectId) {
            return type.equals(MongoType.OBJECT_ID);
        }

        if (value instanceof BigDecimal) {
            return type.equals(MongoType.DECIMAL);
        }

        if (value instanceof String) {
            return type.equals(MongoType.STRING);
        }

        if (value instanceof Boolean) {
            return type.equals(MongoType.BOOLEAN);
        }

        if (value instanceof Long) {
            return type.equals(MongoType.LONG);
        }

        if (value instanceof Integer || value instanceof Short || value instanceof Byte) {
            return type.equals(MongoType.INTEGER);
        }

        return false;
    }

    private static boolean fieldExists(Map<String, Object> document, String fieldPath) {
        if (document == null || fieldPath == null) {
            return false;
        }

        if (!fieldPath.contains(".")) {
            return document.containsKey(fieldPath);
        }

        String[] parts = fieldPath.split("\\.");
        return fieldExists(document, parts, 0);
    }

    private static boolean fieldExists(Object current, String[] parts, int idx) {
        if (current == null) {
            return false;
        }

        if (idx >= parts.length) {
            return true;
        }

        String part = parts[idx];

        if (current instanceof Map) {
            Map <?, ? > map = (Map <?, ? >) current;

            if (!map.containsKey(part)) {
                return false;
            }
            if (idx + 1 >= parts.length) {
                return true;
            }

            return fieldExists(map.get(part), parts, idx + 1);
        }

        if (current instanceof List) {
            List<?> list = (List<?>) current;

            if (part.matches("\\d+")) {
                int pos = Integer.parseInt(part);
                if (pos < 0 || pos >= list.size()) {
                    return false;
                }
                if (idx + 1 >= parts.length) {
                    return true;
                }
                return fieldExists(list.get(pos), parts, idx + 1);
            }

            for (Object entry : list) {
                if (fieldExists(entry, parts, idx)) {
                    return true;
                }
            }

            return false;
        }

        return false;
    }

    private static String convertSnakeToCamelCase(String fieldPath) {
        if (!fieldPath.contains("_")) {
            return fieldPath;
        }

        String[] parts = fieldPath.split("\\.");
        StringBuilder result = new StringBuilder();

        for (int i = 0; i < parts.length; i++) {
            if (i > 0) {
                result.append(".");
            }
            String part = parts[i];
            if (part.matches("\\d+")) {
                // This is an array index, keep as is
                result.append(part);
            } else {
                // Convert snake_case to camelCase
                String[] words = part.split("_");
                result.append(words[0]);
                for (int j = 1; j < words.length; j++) {
                    if (words[j].length() > 0) {
                        result.append(Character.toUpperCase(words[j].charAt(0)));
                        if (words[j].length() > 1) {
                            result.append(words[j].substring(1));
                        }
                    }
                }
            }
        }

        return result.toString();
    }

    private static String convertCamelToSnakeCase(String fieldPath) {
        String[] parts = fieldPath.split("\\.");
        StringBuilder result = new StringBuilder();

        for (int i = 0; i < parts.length; i++) {
            if (i > 0) {
                result.append(".");
            }
            String part = parts[i];
            if (part.matches("\\d+")) {
                // This is an array index, keep as is
                result.append(part);
            } else {
                // Convert camelCase to snake_case
                StringBuilder converted = new StringBuilder();
                for (int j = 0; j < part.length(); j++) {
                    char c = part.charAt(j);
                    if (Character.isUpperCase(c)) {
                        if (j > 0) {
                            converted.append("_");
                        }
                        converted.append(Character.toLowerCase(c));
                    } else {
                        converted.append(c);
                    }
                }
                result.append(converted.toString());
            }
        }

        return result.toString();
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
