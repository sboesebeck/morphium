package de.caluga.morphium.driver.inmem;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.Collator;
import java.text.ParseException;
import java.text.RuleBasedCollator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.Set;

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

    // Pre-compiled patterns for $text search - avoid regex compilation on every query
    private static final Pattern TEXT_PHRASE_PATTERN = Pattern.compile("\"([^\"]+)\"");
    private static final Pattern TEXT_CLEANUP_PATTERN = Pattern.compile("[^a-z0-9 ]");
    private static final Pattern TEXT_SPLIT_PATTERN = Pattern.compile(" +");

    // Known MongoDB query operators - prevents false positives when checking for unknown operators
    private static final Set<String> KNOWN_OPERATORS = Set.of(
        // Comparison
        "$eq", "$ne", "$gt", "$gte", "$lt", "$lte", "$in", "$nin",
        // Logical
        "$and", "$or", "$not", "$nor",
        // Element
        "$exists", "$type",
        // Evaluation
        "$expr", "$jsonSchema", "$mod", "$regex", "$options", "$text", "$where",
        // Array
        "$all", "$elemMatch", "$size",
        // Bitwise
        "$bitsAllClear", "$bitsAllSet", "$bitsAnyClear", "$bitsAnySet",
        // Geospatial
        "$geoIntersects", "$geoWithin", "$near", "$nearSphere", "$box", "$center",
        "$centerSphere", "$geometry", "$maxDistance", "$minDistance", "$polygon",
        // Miscellaneous
        "$comment", "$meta", "$slice", "$natural"
    );

    private static boolean isKnownOperator(String op) {
        return KNOWN_OPERATORS.contains(op);
    }

    /**
     * Validates a query for unknown operators before execution.
     * Should be called before processing to catch invalid queries even on empty collections.
     * @param query the query to validate
     * @throws IllegalArgumentException if an unknown operator is found
     */
    public static void validateQuery(Map<String, Object> query) {
        if (query == null || query.isEmpty()) {
            return;
        }
        for (String key : query.keySet()) {
            // Check for unknown $ operators at top level (field names starting with $ that aren't known operators)
            if (key.startsWith("$") && !isKnownOperator(key)) {
                throw new IllegalArgumentException("unknown top level operator: " + key);
            }
            Object value = query.get(key);
            if (value instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> subQuery = (Map<String, Object>) value;
                // For field queries like {"field": {"$eq": value}}, validate the operators
                for (String subKey : subQuery.keySet()) {
                    if (subKey.startsWith("$") && !isKnownOperator(subKey)) {
                        throw new IllegalArgumentException("unknown operator: " + subKey);
                    }
                    // Recursively validate nested queries (e.g., $elemMatch, $not)
                    Object subValue = subQuery.get(subKey);
                    if (subValue instanceof Map) {
                        validateQuery((Map<String, Object>) subValue);
                    } else if (subValue instanceof List) {
                        // Handle $and, $or, $nor which contain lists of queries
                        for (Object item : (List<?>) subValue) {
                            if (item instanceof Map) {
                                validateQuery((Map<String, Object>) item);
                            }
                        }
                    }
                }
            } else if (value instanceof List) {
                // Handle top-level $and, $or, $nor
                for (Object item : (List<?>) value) {
                    if (item instanceof Map) {
                        validateQuery((Map<String, Object>) item);
                    }
                }
            }
        }
    }

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
                        org.slf4j.LoggerFactory.getLogger(QueryHelper.class).debug("QueryHelper: evaluating $expr = {}", query.get(keyQuery));
                        Expr expr = Expr.parse(query.get(keyQuery));
                        var result = expr.evaluate(toCheck);

                        if (result instanceof Expr) {
                            result = ((Expr) result).evaluate(toCheck);
                        }

                        org.slf4j.LoggerFactory.getLogger(QueryHelper.class).debug("QueryHelper: $expr result = {}", result);
                        return Boolean.TRUE.equals(result);
                    }

                case "$jsonSchema": {
                        Object schema = query.get(keyQuery);

                        if (!(schema instanceof Map)) {
                            return false;
                        }

                        if (!matchesJsonSchema((Map<String, Object>) schema, toCheck)) {
                            return false;
                        }

                        ret = true;
                        continue;
                    }

                case "$where":
                    ret = runWhere(query, toCheck);
                    continue;

                default:
                    // Check if this is an unknown $ operator (like $big$)
                    // But allow known operators that may appear in recursive calls
                    if (keyQuery.startsWith("$") && !isKnownOperator(keyQuery)) {
                        throw new IllegalArgumentException("unknown top level operator: " + keyQuery);
                    }

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
                            // Optimization: split once and reuse the path array
                            String[] pth = keyQuery.split("\\.");
                            // Special handling for map field queries like "stringMap.key1"
                            if (pth.length >= 2 && toCheck.get(pth[0]) instanceof Map) {
                                Map<String, Object> mapField = (Map<String, Object>) toCheck.get(pth[0]);
                                if (pth.length == 2 && mapField.containsKey(pth[1])) {
                                    checkValue = mapField.get(pth[1]);
                                } else {
                                    // Handle nested paths - reuse already split pth
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
                                // Handle nested paths - reuse already split pth
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
                                int div = ((Number) arr.get(0)).intValue();
                                int rem = ((Number) arr.get(1)).intValue();
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
                                    LookupResult lookup = resolveValuesForPath(toCheck, keyQuery.split("\\."), 0);
                                    exists = lookup.pathExists;
                                } else {
                                    exists = fieldExists(toCheck, keyQuery);

                                    if (!exists && keyQuery.contains("_")) {
                                        String camelCaseField = convertSnakeToCamelCase(keyQuery);
                                        exists = fieldExists(toCheck, camelCaseField);
                                    }

                                    if (!exists && !keyQuery.contains("_")) {
                                        String snakeCaseField = convertCamelToSnakeCase(keyQuery);
                                        exists = fieldExists(toCheck, snakeCaseField);
                                    }
                                }

                                Object existsOperand = commandMap.get(commandKey);
                                boolean expectExists = Boolean.TRUE.equals(existsOperand)
                                 || "true".equals(existsOperand)
                                 || Integer.valueOf(1).equals(existsOperand);

                                return expectExists ? exists : !exists;

                            case "$nin":
                                List<?> ninList = (List) commandMap.get(commandKey);
                                boolean foundNin = false;

                                // When collation is present, use O(n) comparison that respects collation
                                // Otherwise use O(1) HashSet lookup
                                if (coll != null) {
                                    for (Object v : ninList) {
                                        Object normalized = normalizeId(v);
                                        if (checkValue instanceof List) {
                                            for (Object element : (List) checkValue) {
                                                if (compareValues(element, normalized, coll)) {
                                                    foundNin = true;
                                                    break;
                                                }
                                            }
                                        } else if (compareValues(checkValue, normalized, coll)) {
                                            foundNin = true;
                                        }
                                        if (foundNin) break;
                                    }
                                } else {
                                    // Optimize: use HashSet for O(1) lookup when no collation
                                    Set<Object> ninSet = new HashSet<>(ninList.size());
                                    for (Object v : ninList) {
                                        ninSet.add(normalizeId(v));
                                    }
                                    if (checkValue instanceof List) {
                                        for (Object element : (List) checkValue) {
                                            Object normalizedElement = normalizeId(element);
                                            if (ninSet.contains(normalizedElement)) {
                                                foundNin = true;
                                                break;
                                            }
                                        }
                                    } else {
                                        Object normalizedCheck = normalizeId(checkValue);
                                        foundNin = ninSet.contains(normalizedCheck);
                                    }
                                }
                                return !foundNin;

                            case "$in":
                                List<?> inList = (List) commandMap.get(commandKey);

                                // When collation is present, use O(n) comparison that respects collation
                                // Otherwise use O(1) HashSet lookup
                                if (coll != null) {
                                    for (Object v : inList) {
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
                                } else {
                                    // Optimize: use HashSet for O(1) lookup when no collation
                                    Set<Object> inSet = new HashSet<>(inList.size());
                                    for (Object v : inList) {
                                        inSet.add(normalizeId(v));
                                    }
                                    if (checkValue instanceof List) {
                                        for (Object element : (List) checkValue) {
                                            Object normalizedElement = normalizeId(element);
                                            if (inSet.contains(normalizedElement)) {
                                                return true;
                                            }
                                        }
                                    } else {
                                        Object normalizedCheck = normalizeId(checkValue);
                                        if (inSet.contains(normalizedCheck)) {
                                            return true;
                                        }
                                    }
                                    return false;
                                }

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

                                    // Use pre-compiled patterns for performance
                                    var matcher = TEXT_PHRASE_PATTERN.matcher(srch);
                                    while (matcher.find()) {
                                        tokens.add(matcher.group(1));
                                    }

                                    srch = matcher.replaceAll(" ");
                                    srch = TEXT_CLEANUP_PATTERN.matcher(srch).replaceAll(" ");
                                    tokens.addAll(Arrays.asList(TEXT_SPLIT_PATTERN.split(srch.trim())));

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

                                if (commandMap.get(commandKey) instanceof Number) {
                                    type = MongoType.findByValue(((Number) commandMap.get(commandKey)).intValue());
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
                                break;

                            case "$geoIntersects":
                                if (!geoIntersects(checkValue, commandMap.get(commandKey))) {
                                    return false;
                                }

                                ret = true;
                                continue;

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

                                // Optimize: use HashSet for O(1) lookups instead of O(n) List.contains()
                                Set<Object> checkSet = new HashSet<>(toCheckValList);
                                for (Object o : queryValues) {
                                    if (!checkSet.contains(o)) {
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
                                        if (o instanceof Number) {
                                            value = value | 1L << ((Number) o).intValue();
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
                                // Check if this is an unknown $ operator
                                if (commandKey.startsWith("$") && !isKnownOperator(commandKey)) {
                                    throw new IllegalArgumentException("unknown operator: " + commandKey);
                                }

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
                            String[] path = keyQuery.split("\\.");
                            LookupResult lookup = resolveValuesForPath(toCheck, path, 0);
                            Object expected = query.get(keyQuery);

                            if (!lookup.pathExists) {
                                return expected == null;
                            }

                            if (expected == null) {
                                if (lookup.values.isEmpty()) {
                                    return true;
                                }

                                for (Object candidate : lookup.values) {
                                    if (candidate == null) {
                                        return true;
                                    }
                                }

                                return false;
                            }

                            Collator coll = null;

                            if (collation != null && !collation.isEmpty()) {
                                coll = getCollator(collation);
                            }

                            for (Object candidate : lookup.values) {
                                if (candidate instanceof List) {
                                    for (Object element : (List) candidate) {
                                        if (compareValues(element, expected, coll)) {
                                            return true;
                                        }
                                    }
                                    continue;
                                }

                                if (compareValues(candidate, expected, coll)) {
                                    return true;
                                }
                            }

                            return false;
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
                            Object queryValue = query.get(keyQuery);
                            if (queryValue == null) {
                                return false;
                            }
                            return toCheck.get(keyQuery).toString().equals(queryValue.toString());
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

    private static boolean matchesJsonSchema(Map<String, Object> schema, Map<String, Object> document) {
        if (schema == null) {
            return true;
        }

        try {
            return validateJsonSchema(schema, document);
        } catch (ClassCastException e) {
            log.debug("$jsonSchema evaluation failed because of incompatible schema definition: {}", e.getMessage());
            return false;
        }
    }

    private interface Geometry {
        boolean intersects(Geometry other);
    }

    private static final class PointGeometry implements Geometry {
        private final double x;
        private final double y;

        PointGeometry(List<?> coordinates) {
            this.x = toDouble(coordinates, 0);
            this.y = toDouble(coordinates, 1);
        }

        @Override
        public boolean intersects(Geometry other) {
            if (other instanceof PointGeometry) {
                PointGeometry p = (PointGeometry) other;
                return Double.compare(x, p.x) == 0 && Double.compare(y, p.y) == 0;
            }

            if (other instanceof LineStringGeometry) {
                return ((LineStringGeometry) other).contains(x, y);
            }

            if (other instanceof PolygonGeometry) {
                return ((PolygonGeometry) other).contains(x, y);
            }

            if (other instanceof MultiGeometry) {
                return ((MultiGeometry) other).intersects(this);
            }

            return false;
        }
    }

    private static final class LineStringGeometry implements Geometry {
        private final double[] xs;
        private final double[] ys;

        LineStringGeometry(List<?> coordinates) {
            int len = coordinates.size();
            xs = new double[len];
            ys = new double[len];

            for (int i = 0; i < len; i++) {
                List<?> point = toListView(coordinates.get(i));
                xs[i] = toDouble(point, 0);
                ys[i] = toDouble(point, 1);
            }
        }

        boolean contains(double px, double py) {
            for (int i = 0; i < xs.length - 1; i++) {
                double x1 = xs[i];
                double y1 = ys[i];
                double x2 = xs[i + 1];
                double y2 = ys[i + 1];

                if (pointOnSegment(px, py, x1, y1, x2, y2)) {
                    return true;
                }
            }

            return false;
        }

        boolean intersectsLine(LineStringGeometry other) {
            for (int i = 0; i < xs.length - 1; i++) {
                double x1 = xs[i];
                double y1 = ys[i];
                double x2 = xs[i + 1];
                double y2 = ys[i + 1];

                for (int j = 0; j < other.xs.length - 1; j++) {
                    double x3 = other.xs[j];
                    double y3 = other.ys[j];
                    double x4 = other.xs[j + 1];
                    double y4 = other.ys[j + 1];

                    if (segmentsIntersect(x1, y1, x2, y2, x3, y3, x4, y4)) {
                        return true;
                    }
                }
            }

            return false;
        }

        @Override
        public boolean intersects(Geometry other) {
            if (other instanceof PointGeometry) {
                PointGeometry p = (PointGeometry) other;
                return contains(p.x, p.y);
            }

            if (other instanceof LineStringGeometry) {
                LineStringGeometry line = (LineStringGeometry) other;
                return intersectsLine(line) || line.intersectsLine(this);
            }

            if (other instanceof PolygonGeometry) {
                PolygonGeometry polygon = (PolygonGeometry) other;
                if (polygon.contains(xs[0], ys[0])) {
                    return true;
                }

                return polygon.intersectsLine(this);
            }

            if (other instanceof MultiGeometry) {
                return other.intersects(this);
            }

            return false;
        }
    }

    private static final class PolygonGeometry implements Geometry {
        private final List<LineStringGeometry> rings;

        PolygonGeometry(List<?> coordinates) {
            rings = new ArrayList<>();

            for (Object ringObj : coordinates) {
                List<?> ring = toListView(ringObj);
                if (ring == null || ring.size() < 4) {
                    continue;
                }

                rings.add(new LineStringGeometry(ring));
            }
        }

        boolean contains(double px, double py) {
            if (rings.isEmpty()) {
                return false;
            }

            if (!pointInPolygon(px, py, rings.get(0))) {
                return false;
            }

            for (int i = 1; i < rings.size(); i++) {
                if (pointInPolygon(px, py, rings.get(i))) {
                    return false;
                }
            }

            return true;
        }

        boolean intersectsLine(LineStringGeometry line) {
            if (contains(line.xs[0], line.ys[0])) {
                return true;
            }

            for (LineStringGeometry ring : rings) {
                if (ring.intersectsLine(line)) {
                    return true;
                }
            }

            return false;
        }

        @Override
        public boolean intersects(Geometry other) {
            if (other instanceof PointGeometry) {
                PointGeometry point = (PointGeometry) other;
                return contains(point.x, point.y);
            }

            if (other instanceof LineStringGeometry) {
                return intersectsLine((LineStringGeometry) other);
            }

            if (other instanceof PolygonGeometry) {
                PolygonGeometry polygon = (PolygonGeometry) other;

                if (this.contains(polygon.rings.get(0).xs[0], polygon.rings.get(0).ys[0])
                 || polygon.contains(rings.get(0).xs[0], rings.get(0).ys[0])) {
                    return true;
                }

                for (LineStringGeometry ring : rings) {
                    if (polygon.intersectsLine(ring)) {
                        return true;
                    }
                }

                for (LineStringGeometry otherRing : polygon.rings) {
                    if (this.intersectsLine(otherRing)) {
                        return true;
                    }
                }

                return false;
            }

            if (other instanceof MultiGeometry) {
                return other.intersects(this);
            }

            return false;
        }
    }

    private static final class MultiGeometry implements Geometry {
        private final List<Geometry> components = new ArrayList<>();

        MultiGeometry(List<?> coordinates, String componentType) {
            for (Object component : coordinates) {
                Geometry geometry = toGeometry(componentType, toListView(component));

                if (geometry != null) {
                    components.add(geometry);
                }
            }
        }

        @Override
        public boolean intersects(Geometry other) {
            for (Geometry component : components) {
                if (component.intersects(other)) {
                    return true;
                }
            }

            return false;
        }
    }

    private static Geometry toGeometry(String type, List<?> coordinates) {
        if (type == null || coordinates == null) {
            return null;
        }

        type = type.toLowerCase(Locale.ROOT);

        switch (type) {
        case "point":
            return new PointGeometry(coordinates);

        case "linestring":
            return new LineStringGeometry(coordinates);

        case "polygon":
            return new PolygonGeometry(coordinates);

        case "multipoint":
            return new MultiGeometry(coordinates, "point");

        case "multilinestring":
            return new MultiGeometry(coordinates, "linestring");

        case "multipolygon":
            return new MultiGeometry(coordinates, "polygon");

        default:
            log.warn("Unsupported geo type '{}'", type);
            return null;
        }
    }

    private static double toDouble(List<?> point, int index) {
        if (point == null || point.size() <= index) {
            return Double.NaN;
        }

        Object value = point.get(index);

        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }

        if (value instanceof String) {
            try {
                return Double.parseDouble((String) value);
            } catch (NumberFormatException ignored) {
                return Double.NaN;
            }
        }

        return Double.NaN;
    }

    private static boolean pointOnSegment(double px, double py, double x1, double y1, double x2, double y2) {
        double cross = (px - x1) * (y2 - y1) - (py - y1) * (x2 - x1);

        if (Math.abs(cross) > 1e-9) {
            return false;
        }

        double dot = (px - x1) * (px - x2) + (py - y1) * (py - y2);
        return dot <= 0;
    }

    private static boolean segmentsIntersect(double x1, double y1, double x2, double y2,
            double x3, double y3, double x4, double y4) {
        double d1 = direction(x3, y3, x4, y4, x1, y1);
        double d2 = direction(x3, y3, x4, y4, x2, y2);
        double d3 = direction(x1, y1, x2, y2, x3, y3);
        double d4 = direction(x1, y1, x2, y2, x4, y4);

        if (((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0))
         && ((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0))) {
            return true;
        }

        if (Math.abs(d1) < 1e-9 && pointOnSegment(x1, y1, x3, y3, x4, y4)) {
            return true;
        }

        if (Math.abs(d2) < 1e-9 && pointOnSegment(x2, y2, x3, y3, x4, y4)) {
            return true;
        }

        if (Math.abs(d3) < 1e-9 && pointOnSegment(x3, y3, x1, y1, x2, y2)) {
            return true;
        }

        if (Math.abs(d4) < 1e-9 && pointOnSegment(x4, y4, x1, y1, x2, y2)) {
            return true;
        }

        return false;
    }

    private static double direction(double xi, double yi, double xj, double yj, double xk, double yk) {
        return (xk - xi) * (yj - yi) - (xj - xi) * (yk - yi);
    }

    private static boolean pointInPolygon(double px, double py, LineStringGeometry ring) {
        int intersections = 0;

        for (int i = 0; i < ring.xs.length - 1; i++) {
            double x1 = ring.xs[i];
            double y1 = ring.ys[i];
            double x2 = ring.xs[i + 1];
            double y2 = ring.ys[i + 1];

            if (pointOnSegment(px, py, x1, y1, x2, y2)) {
                return true;
            }

            boolean intersect = ((y1 > py) != (y2 > py))
                               && (px < (x2 - x1) * (py - y1) / (y2 - y1 + 1e-12) + x1);

            if (intersect) {
                intersections++;
            }
        }

        return intersections % 2 == 1;
    }
    private static Map<String, Object> asGeometry(Object value) {
        if (!(value instanceof Map)) {
            return null;
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) value;
        if (!map.containsKey("type") || !map.containsKey("coordinates")) {
            return null;
        }

        return map;
    }

    private static boolean geoIntersects(Object documentGeometry, Object queryGeometry) {
        Map<String, Object> docGeo = asGeometry(documentGeometry);
        Map<String, Object> queryGeo = asGeometry(queryGeometry instanceof Map && ((Map<?, ?>) queryGeometry).containsKey("$geometry")
                                   ? ((Map<String, Object>) queryGeometry).get("$geometry")
                                   : queryGeometry);

        if (docGeo == null || queryGeo == null) {
            return false;
        }

        String docType = (String) docGeo.get("type");
        String queryType = (String) queryGeo.get("type");

        if (docType == null || queryType == null) {
            return false;
        }

        List<?> docCoords = toListView(docGeo.get("coordinates"));
        List<?> queryCoords = toListView(queryGeo.get("coordinates"));

        if (docCoords == null || queryCoords == null) {
            return false;
        }

        Geometry docGeometryObj = toGeometry(docType, docCoords);
        Geometry queryGeometryObj = toGeometry(queryType, queryCoords);

        if (docGeometryObj == null || queryGeometryObj == null) {
            return false;
        }

        return docGeometryObj.intersects(queryGeometryObj);
    }

    @SuppressWarnings("unchecked")
    private static boolean validateJsonSchema(Map<String, Object> schema, Object value) {
        if (schema == null) {
            return true;
        }

        if (schema.containsKey("allOf")) {
            for (Object entry : asList(schema.get("allOf"))) {
                if (entry instanceof Map) {
                    if (!validateJsonSchema((Map<String, Object>) entry, value)) {
                        return false;
                    }
                }
            }
        }

        if (schema.containsKey("anyOf")) {
            boolean matched = false;

            for (Object entry : asList(schema.get("anyOf"))) {
                if (entry instanceof Map && validateJsonSchema((Map<String, Object>) entry, value)) {
                    matched = true;
                    break;
                }
            }

            if (!matched) {
                return false;
            }
        }

        if (schema.containsKey("oneOf")) {
            int matches = 0;

            for (Object entry : asList(schema.get("oneOf"))) {
                if (entry instanceof Map && validateJsonSchema((Map<String, Object>) entry, value)) {
                    matches++;
                }
            }

            if (matches != 1) {
                return false;
            }
        }

        if (schema.containsKey("not") && schema.get("not") instanceof Map) {
            if (validateJsonSchema((Map<String, Object>) schema.get("not"), value)) {
                return false;
            }
        }

        if (schema.containsKey("if") && schema.get("if") instanceof Map) {
            boolean condition = validateJsonSchema((Map<String, Object>) schema.get("if"), value);

            if (condition) {
                Object thenSchema = schema.get("then");

                if (thenSchema instanceof Map && !validateJsonSchema((Map<String, Object>) thenSchema, value)) {
                    return false;
                }
            } else {
                Object elseSchema = schema.get("else");

                if (elseSchema instanceof Map && !validateJsonSchema((Map<String, Object>) elseSchema, value)) {
                    return false;
                }
            }
        }

        if (schema.containsKey("enum")) {
            List<Object> allowedValues = asList(schema.get("enum"));

            if (!allowedValues.isEmpty()) {
                boolean anyMatch = false;

                for (Object allowed : allowedValues) {
                    if (compareValues(value, allowed, null)) {
                        anyMatch = true;
                        break;
                    }
                }

                if (!anyMatch) {
                    return false;
                }
            }
        }

        if (schema.containsKey("const")) {
            if (!compareValues(value, schema.get("const"), null)) {
                return false;
            }
        }

        if (schema.containsKey("bsonType") || schema.containsKey("type")) {
            Object typeSpec = schema.containsKey("bsonType") ? schema.get("bsonType") : schema.get("type");
            List<Object> types = asList(typeSpec);
            boolean typeMatch = false;

            for (Object spec : types) {
                if (matchesTypeSpecification(value, spec)) {
                    typeMatch = true;
                    break;
                }
            }

            if (!typeMatch) {
                return false;
            }
        }

        if (value == null) {
            return true;
        }

        BigDecimal numeric = toBigDecimal(value);

        if (numeric != null) {
            if (schema.containsKey("minimum")) {
                BigDecimal minimum = toBigDecimal(schema.get("minimum"));

                if (minimum != null && numeric.compareTo(minimum) < 0) {
                    return false;
                }
            }

            if (schema.containsKey("maximum")) {
                BigDecimal maximum = toBigDecimal(schema.get("maximum"));

                if (maximum != null && numeric.compareTo(maximum) > 0) {
                    return false;
                }
            }

            if (schema.containsKey("exclusiveMinimum")) {
                Object exclusiveMinimum = schema.get("exclusiveMinimum");

                if (exclusiveMinimum instanceof Boolean) {
                    if ((Boolean) exclusiveMinimum && schema.containsKey("minimum")) {
                        BigDecimal minimum = toBigDecimal(schema.get("minimum"));

                        if (minimum != null && numeric.compareTo(minimum) <= 0) {
                            return false;
                        }
                    }
                } else {
                    BigDecimal minimum = toBigDecimal(exclusiveMinimum);

                    if (minimum != null && numeric.compareTo(minimum) <= 0) {
                        return false;
                    }
                }
            }

            if (schema.containsKey("exclusiveMaximum")) {
                Object exclusiveMaximum = schema.get("exclusiveMaximum");

                if (exclusiveMaximum instanceof Boolean) {
                    if ((Boolean) exclusiveMaximum && schema.containsKey("maximum")) {
                        BigDecimal maximum = toBigDecimal(schema.get("maximum"));

                        if (maximum != null && numeric.compareTo(maximum) >= 0) {
                            return false;
                        }
                    }
                } else {
                    BigDecimal maximum = toBigDecimal(exclusiveMaximum);

                    if (maximum != null && numeric.compareTo(maximum) >= 0) {
                        return false;
                    }
                }
            }

            if (schema.containsKey("multipleOf")) {
                BigDecimal multiple = toBigDecimal(schema.get("multipleOf"));

                if (multiple != null && multiple.compareTo(BigDecimal.ZERO) != 0) {
                    BigDecimal remainder = numeric.remainder(multiple);

                    if (remainder == null || remainder.compareTo(BigDecimal.ZERO) != 0) {
                        return false;
                    }
                }
            }
        }

        if (value instanceof String) {
            String str = (String) value;

            if (schema.containsKey("minLength")) {
                Number minLength = asNumber(schema.get("minLength"));

                if (minLength != null && str.codePointCount(0, str.length()) < minLength.intValue()) {
                    return false;
                }
            }

            if (schema.containsKey("maxLength")) {
                Number maxLength = asNumber(schema.get("maxLength"));

                if (maxLength != null && str.codePointCount(0, str.length()) > maxLength.intValue()) {
                    return false;
                }
            }

            if (schema.containsKey("pattern") && schema.get("pattern") instanceof String patternText) {
                try {
                    Pattern compiled = Pattern.compile(patternText);

                    if (!compiled.matcher(str).find()) {
                        return false;
                    }
                } catch (PatternSyntaxException pse) {
                    log.warn("Invalid regex in $jsonSchema pattern '{}': {}", patternText, pse.getMessage());
                    return false;
                }
            }
        }

        List<?> listValue = toListView(value);

        if (listValue != null) {
            if (schema.containsKey("minItems")) {
                Number minItems = asNumber(schema.get("minItems"));

                if (minItems != null && listValue.size() < minItems.intValue()) {
                    return false;
                }
            }

            if (schema.containsKey("maxItems")) {
                Number maxItems = asNumber(schema.get("maxItems"));

                if (maxItems != null && listValue.size() > maxItems.intValue()) {
                    return false;
                }
            }

            if (Boolean.TRUE.equals(schema.get("uniqueItems"))) {
                Set<Object> seen = new HashSet<>();

                for (Object element : listValue) {
                    Object normalized = normalizeId(element);

                    if (normalized instanceof byte[]) {
                        normalized = Arrays.hashCode((byte[]) normalized);
                    }

                    if (!seen.add(normalized)) {
                        return false;
                    }
                }
            }

            if (schema.containsKey("items")) {
                Object items = schema.get("items");

                if (items instanceof Map) {
                    for (Object element : listValue) {
                        if (!validateJsonSchema((Map<String, Object>) items, element)) {
                            return false;
                        }
                    }
                } else if (items instanceof List<?>) {
                    List<?> tupleSchemas = (List<?>) items;
                    int index = 0;

                    for (; index < tupleSchemas.size() && index < listValue.size(); index++) {
                        Object tupleSchema = tupleSchemas.get(index);

                        if (tupleSchema instanceof Map && !validateJsonSchema((Map<String, Object>) tupleSchema, listValue.get(index))) {
                            return false;
                        }
                    }

                    if (listValue.size() > tupleSchemas.size()) {
                        Object additionalItems = schema.get("additionalItems");

                        if (additionalItems instanceof Boolean) {
                            if (!(Boolean) additionalItems) {
                                return false;
                            }
                        } else if (additionalItems instanceof Map) {
                            for (int i = tupleSchemas.size(); i < listValue.size(); i++) {
                                if (!validateJsonSchema((Map<String, Object>) additionalItems, listValue.get(i))) {
                                    return false;
                                }
                            }
                        }
                    }
                }
            }

            if (schema.containsKey("contains") && schema.get("contains") instanceof Map) {
                boolean containsMatch = false;

                for (Object element : listValue) {
                    if (validateJsonSchema((Map<String, Object>) schema.get("contains"), element)) {
                        containsMatch = true;
                        break;
                    }
                }

                if (!containsMatch) {
                    return false;
                }
            }
        }

        if (value instanceof Map) {
            Map<String, Object> mapValue = (Map<String, Object>) value;

            if (schema.containsKey("minProperties")) {
                Number minProperties = asNumber(schema.get("minProperties"));

                if (minProperties != null && mapValue.size() < minProperties.intValue()) {
                    return false;
                }
            }

            if (schema.containsKey("maxProperties")) {
                Number maxProperties = asNumber(schema.get("maxProperties"));

                if (maxProperties != null && mapValue.size() > maxProperties.intValue()) {
                    return false;
                }
            }

            if (schema.containsKey("required")) {
                for (Object requiredField : asList(schema.get("required"))) {
                    if (requiredField instanceof String fieldName) {
                        if (!mapValue.containsKey(fieldName)) {
                            return false;
                        }
                    }
                }
            }

            Map<String, Object> properties = schema.get("properties") instanceof Map
                                            ? (Map<String, Object>) schema.get("properties")
                                            : Collections.emptyMap();
            Set<String> allowedKeys = new HashSet<>(properties.keySet());
            allowedKeys.add("_id");

            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                if (!(entry.getValue() instanceof Map)) {
                    continue;
                }

                if (mapValue.containsKey(entry.getKey())) {
                    if (!validateJsonSchema((Map<String, Object>) entry.getValue(), mapValue.get(entry.getKey()))) {
                        return false;
                    }
                }
            }

            Map<String, Object> patternProperties = schema.get("patternProperties") instanceof Map
                                                     ? (Map<String, Object>) schema.get("patternProperties")
                                                     : Collections.emptyMap();

            if (!patternProperties.isEmpty()) {
                for (Map.Entry<String, Object> patternEntry : patternProperties.entrySet()) {
                    try {
                        Pattern compiled = Pattern.compile(patternEntry.getKey());

                        for (Map.Entry<String, Object> docEntry : mapValue.entrySet()) {
                            if (compiled.matcher(docEntry.getKey()).find()) {
                                allowedKeys.add(docEntry.getKey());

                                if (patternEntry.getValue() instanceof Map && !validateJsonSchema((Map<String, Object>) patternEntry.getValue(), docEntry.getValue())) {
                                    return false;
                                }
                            }
                        }
                    } catch (PatternSyntaxException pse) {
                        log.warn("Invalid regex in patternProperties '{}': {}", patternEntry.getKey(), pse.getMessage());
                        return false;
                    }
                }
            }

            if (schema.containsKey("additionalProperties")) {
                Object additional = schema.get("additionalProperties");

                if (additional instanceof Boolean) {
                    if (!(Boolean) additional) {
                        for (String key : mapValue.keySet()) {
                            if (!allowedKeys.contains(key)) {
                                return false;
                            }
                        }
                    }
                } else if (additional instanceof Map) {
                    for (Map.Entry<String, Object> entry : mapValue.entrySet()) {
                        if (!allowedKeys.contains(entry.getKey())) {
                            if (!validateJsonSchema((Map<String, Object>) additional, entry.getValue())) {
                                return false;
                            }
                        }
                    }
                }
            }
        }

        return true;
    }

    @SuppressWarnings("unchecked")
    private static boolean matchesTypeSpecification(Object value, Object specification) {
        if (specification == null) {
            return true;
        }

        if (specification instanceof Map) {
            return validateJsonSchema((Map<String, Object>) specification, value);
        }

        if (specification instanceof Number) {
            MongoType mongoType = MongoType.findByValue(((Number) specification).intValue());
            return mongoType != null && matchesType(value, mongoType);
        }

        if (specification instanceof String typeName) {
            String normalized = typeName.toLowerCase(Locale.ROOT);
            MongoType mongoType = MongoType.findByTxt(normalized);

            if (mongoType != null) {
                return matchesType(value, mongoType);
            }

            switch (normalized) {
            case "number":
                return value instanceof Number || value instanceof BigDecimal;

            case "integer":
                return isIntegralNumber(value);

            case "object":
                return value instanceof Map;

            case "array":
                return value instanceof List || (value != null && value.getClass().isArray());

            case "boolean":
                return value instanceof Boolean;

            case "string":
                return value instanceof String;

            case "null":
                return value == null;

            case "date":
                return value instanceof Date;

            case "timestamp":
                return value instanceof Date || matchesType(value, MongoType.TIMESTAMP);

            case "decimal128":
            case "decimal":
                return matchesType(value, MongoType.DECIMAL);

            case "objectid":
                return value instanceof MorphiumId || value instanceof ObjectId;

            default:
                return false;
            }
        }

        return false;
    }

    private static List<Object> asList(Object value) {
        if (value == null) {
            return Collections.emptyList();
        }

        if (value instanceof List) {
            return (List<Object>) value;
        }

        if (value.getClass().isArray()) {
            int length = Array.getLength(value);
            List<Object> result = new ArrayList<>(length);

            for (int i = 0; i < length; i++) {
                result.add(Array.get(value, i));
            }

            return result;
        }

        return Collections.singletonList(value);
    }

    private static List<?> toListView(Object value) {
        if (value instanceof List) {
            return (List<?>) value;
        }

        if (value != null && value.getClass().isArray()) {
            int length = Array.getLength(value);
            List<Object> result = new ArrayList<>(length);

            for (int i = 0; i < length; i++) {
                result.add(Array.get(value, i));
            }

            return result;
        }

        return null;
    }

    private static Number asNumber(Object value) {
        if (value instanceof Number) {
            return (Number) value;
        }

        if (value instanceof String) {
            try {
                return new BigDecimal((String) value);
            } catch (NumberFormatException ignored) {
                return null;
            }
        }

        return null;
    }

    private static BigDecimal toBigDecimal(Object value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }

        if (value instanceof BigInteger) {
            return new BigDecimal((BigInteger) value);
        }

        if (value instanceof Number) {
            if (value instanceof Double || value instanceof Float) {
                return BigDecimal.valueOf(((Number) value).doubleValue());
            }

            return BigDecimal.valueOf(((Number) value).longValue());
        }

        if (value instanceof String) {
            try {
                return new BigDecimal((String) value);
            } catch (NumberFormatException ignored) {
                return null;
            }
        }

        return null;
    }

    private static boolean isIntegralNumber(Object value) {
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long || value instanceof BigInteger) {
            return true;
        }

        if (value instanceof BigDecimal) {
            BigDecimal bd = ((BigDecimal) value).stripTrailingZeros();
            return bd.scale() <= 0;
        }

        if (value instanceof Float || value instanceof Double) {
            double dbl = ((Number) value).doubleValue();
            return Math.rint(dbl) == dbl;
        }

        return false;
    }

    private static LookupResult resolveValuesForPath(Object current, String[] path, int position) {
        LookupResult result = new LookupResult();

        if (position >= path.length) {
            result.pathExists = true;

            if (current instanceof List) {
                for (Object element : (List) current) {
                    result.values.add(element);
                }

                return result;
            }

            result.values.add(current);
            return result;
        }

        if (current instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) current;
            String key = path[position];

            if (!map.containsKey(key)) {
                return result;
            }

            Object next = map.get(key);

            if (next == null) {
                if (position == path.length - 1) {
                    result.pathExists = true;
                    result.values.add(null);
                }

                return result;
            }

            LookupResult sub = resolveValuesForPath(next, path, position + 1);
            result.pathExists = sub.pathExists;
            result.values.addAll(sub.values);
            return result;
        }

        if (current instanceof List) {
            List<?> list = (List<?>) current;
            String key = path[position];

            if (isInteger(key)) {
                int index = Integer.parseInt(key);

                if (index < 0 || index >= list.size()) {
                    return result;
                }

                LookupResult sub = resolveValuesForPath(list.get(index), path, position + 1);
                result.pathExists = sub.pathExists;
                result.values.addAll(sub.values);
                return result;
            }

            boolean anyExists = false;

            for (Object element : list) {
                LookupResult sub = resolveValuesForPath(element, path, position);

                if (sub.pathExists) {
                    anyExists = true;
                }

                result.values.addAll(sub.values);
            }

            result.pathExists = anyExists;
            return result;
        }

        return result;
    }

    private static boolean isInteger(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }

        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);

            if (i == 0 && ch == '-') {
                if (value.length() == 1) {
                    return false;
                }

                continue;
            }

            if (!Character.isDigit(ch)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Optimized check for non-negative integer strings (equivalent to regex \\d+ but O(n) faster)
     * Used for array index validation without regex compilation overhead
     */
    private static boolean isNonNegativeInteger(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            if (!Character.isDigit(value.charAt(i))) {
                return false;
            }
        }
        return true;
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

    private static final class LookupResult {
        boolean pathExists;
        final List<Object> values = new ArrayList<>();
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

            if (isNonNegativeInteger(part)) {
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
            if (isNonNegativeInteger(part)) {
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
            if (isNonNegativeInteger(part)) {
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
