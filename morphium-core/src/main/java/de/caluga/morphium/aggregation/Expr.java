package de.caluga.morphium.aggregation;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.bson.BsonEncoder;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalAdjusters;
import java.util.*;

@SuppressWarnings("unchecked")
public abstract class Expr {

    public abstract Object toQueryObject();

    public abstract Object evaluate(Map<String, Object> context);

    private static Map parseMap(Map <?, ? > o) {
        // LinkedHashMap: key order carries meaning for some operator documents
        // (e.g. a multi-field sortBy spec of $sortArray)
        Map<String, Expr> ret = new LinkedHashMap<>();

        for (Map.Entry <?, ? > e : o.entrySet()) {
            ret.put((String) e.getKey(), parse(e.getValue()));
        }

        return ret;
    }

    @SuppressWarnings("ConstantConditions")
    public static Expr parse(Object o) {
        Logger log = LoggerFactory.getLogger(Expr.class);

        if (o instanceof Map) {
            String k = (String)((Map) o).keySet().stream().findFirst().get();

            if (!k.startsWith("$")) {
                // A map whose first key is not an operator is a document literal in expression
                // position (e.g. {n: 1} as the sortBy spec of $sortArray) - MongoDB evaluates
                // each value; failing here made every literal sub-document a parse error.
                return mapExpr((Map<String, Expr>) parseMap((Map) o));
            }

            k = k.replaceAll("\\$", "");

            if (k.equals("map") && ((Map) o).get("$map") instanceof Map) {
                // Dispatch $map explicitly: the reflective k+"Expr" lookup would also match
                // mapExpr() (a document literal factory) and the winner would depend on
                // reflection order.
                return map((Map) parseMap((Map)((Map) o).get("$map")));
            }

            for (Method m : Expr.class.getDeclaredMethods()) {
                if (Modifier.isStatic(m.getModifiers())) {
                    if (k.equals("toString")) {
                        k = "toStr";
                    }

                    if (m.getName().equals(k) || m.getName().equals(k + "Expr")) {
                        // log.info("Got method for op: " + k + "  method: " + m.getName());
                        try {
                            Object p = ((Map) o).get("$" + k);

                            if (p instanceof List) {
                                @SuppressWarnings("unchecked")
                                List<Expr> l = new ArrayList();

                                for (Object param : (List) p) {
                                    l.add(parse(param));
                                }

                                if (m.getParameterCount() == 1 && m.getParameterTypes()[0].equals(List.class)) {
                                    p = l;
                                } else if (l.size() != m.getParameterCount() && !m.getParameterTypes()[0].isArray()) {
                                    //log.warn("wrong number of method params, maybe overloaded?");
                                    continue;
                                } else if (l.size() == 1) {
                                    p = l.get(0);
                                } else {
                                    p = l.toArray(new Expr[0]);
                                }
                            } else if (p instanceof Map) {
                                p = parseMap((Map) p);
                            } else {
                                p = parse(p);
                            }

                            if (p.getClass().isArray()) {
                                if (!m.getParameterTypes()[0].isArray() && m.getParameterCount() != ((Object[]) p).length) {
                                    log.debug("Wrong method, maybe... parameter count mismatch");
                                    continue;
                                }
                            } else if (m.getParameterCount() > 1) {
                                log.debug("Wrong method, maybe... method needs more than one param, but we only have one");
                                continue;
                            } else if (m.getParameterCount() == 1 && m.getParameterTypes()[0].isArray() && !p.getClass().isArray()) {
                                m.setAccessible(true);
                                return (Expr) m.invoke(null, (Object) new Expr[] {(Expr) p});
                            }

                            if (m.getParameterCount() > 1 && p.getClass().isArray()) {
                                m.setAccessible(true);
                                Object[] prm = new Object[m.getParameterCount()];

                                for (int i = 0; i < prm.length; i++) {
                                    prm[i] = ((Object[]) p)[i];
                                }

                                return (Expr) m.invoke(null, prm);
                            }

                            m.setAccessible(true);
                            return (Expr) m.invoke(null, p);
                        } catch (Exception e) {
                            log.error("Error during parsing of expr", e);
                        }
                    }
                }
            }

            throw new IllegalArgumentException("could not parse operation " + k);
        } else if (o instanceof List) {
            List<Expr> ret = new ArrayList<>();

            for (Object e : ((List) o)) {
                ret.add(parse(e));
            }

            return new ValueExpr() {
                @Override
                public Object toQueryObject() {
                    return ret.stream().map(Expr::toQueryObject).collect(java.util.stream.Collectors.toList());
                }

                @Override
                public Object evaluate(Map<String, Object> context) {
                    return ret.stream().map(e -> e.evaluate(context)).collect(java.util.stream.Collectors.toList());
                }
            };
        } else if (o instanceof String && ((String) o).startsWith("$")) {
            //field Ref
            return field(((String) o));
        } else {
            return new ValueExpr() {
                @Override
                public Object toQueryObject() {
                    return o;
                }
            };
        }

        //throw new RuntimeException("parsing failed");
    }

    public static Expr abs(Expr e) {
        return new OpExprNoList("abs", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.abs(((Number) e.evaluate(context)).doubleValue());
            }
        };
    }

    public static Expr date(Date d) {
        return new ValueExpr() {
            @Override
            public Object toQueryObject() {
                return d;
            }
        };
    }

    public static Expr now() {
        return new ValueExpr() {
            @Override
            public Object toQueryObject() {
                return new Date();
            }
        };
    }

    /**
     * returning a hard coded field reference, better use field(Enum,Class, Morphium) instead!
     * field name here is hardcoded to field.name()!
     *
     * @param field
     * @return
     */
    public static Expr field(Enum field) {
        return field(field.name());
    }

    /**
     * generate field mapping according to @entity settings in class/Morphium config
     *
     * @param name
     * @param type
     * @param m
     * @return
     */
    public static Expr field(Enum name, Class type, Morphium m) {
        return field(m.getARHelper().getMongoFieldName(type, name.name()));
    }

    /**
     * returning a hard-coded field reference
     *
     * @param name
     * @return
     */
    public static Expr field(String name) {
        return new FieldExpr(name);
    }

    public static Expr mapExpr(Map<String, Expr> map) {
        return new ValueExpr() {
            @Override
            public Object toQueryObject() {
                Map<String, Object> ret = new LinkedHashMap<>();

                for (Map.Entry<String, Expr> e : map.entrySet()) {
                    ret.put(e.getKey(), e.getValue().toQueryObject());
                }

                return ret;
            }

            @Override
            public Object evaluate(Map<String, Object> context) {
                Map<String, Object> ret = new LinkedHashMap<>();

                for (Map.Entry<String, Expr> e : map.entrySet()) {
                    ret.put(e.getKey(), e.getValue().evaluate(context));
                }

                return ret;
            }
        };
    }

    public static Expr add(Expr... expr) {
        return new OpExpr("add", Arrays.asList(expr)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Number sum = 0;

                for (Expr f : expr) {
                    Object v = eval(f, context);

                    if (v instanceof Number) {
                        sum = sum.doubleValue() + ((Number) v).doubleValue();
                    } else {
                        LoggerFactory.getLogger(Expr.class).error("Cannot evaluate");
                    }
                }

                return sum;
            }
        };
    }

    public static Expr doubleExpr(double d) {
        return new ValueExpr() {
            @Override
            public Object toQueryObject() {
                return d;
            }
        };
    }

    public static Expr intExpr(int i) {
        return new ValueExpr() {
            @Override
            public Object toQueryObject() {
                return i;
            }
        };
    }

    public static Expr bool(boolean v) {
        return new ValueExpr() {
            @Override
            public Object toQueryObject() {
                return v;
            }
        };
    }

    public static Expr arrayExpr(Expr... elem) {
        return new ArrayExpr(elem);
    }

    public static Expr string(String str) {
        return new ValueExpr() {
            @Override
            public Object toQueryObject() {
                return str;
            }
        };
    }

    public static Expr ceil(Expr e) {
        return new OpExprNoList("ceil", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.ceil(((Number) e.evaluate(context)).doubleValue());
            }
        };
    }

    public static Expr divide(Expr divident, Expr divisor) {
        return new OpExpr("divide", Arrays.asList(divident, divisor)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return ((Number) eval(divident, context)).doubleValue() / ((Number) eval(divisor, context)).doubleValue();
            }
        };
    }

    public static Expr exp(Expr e) {
        return new OpExprNoList("exp", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.exp(((Number) eval(e, context)).doubleValue());
            }
        };
    }

    public static Expr floor(Expr e) {
        return new OpExprNoList("floor", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.floor(((Number) eval(e, context)).doubleValue());
            }
        };
    }

    public static Expr ln(Expr e) {
        return new OpExprNoList("ln", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.log(((Number) eval(e, context)).doubleValue());
            }
        };
    }

    public static Expr log(Expr num, Expr base) {
        return new OpExpr("log", Arrays.asList(num, base)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.log(((Number) eval(num, context)).doubleValue()) / Math.log(((Number) eval(base, context)).doubleValue());
            }
        };
    }

    public static Expr log10(Expr e) {
        return new OpExprNoList("log10", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.log10(((Number) eval(e, context)).doubleValue());
            }
        };
    }

    public static Expr mod(Expr divident, Expr divisor) {
        return new OpExpr("mod", Arrays.asList(divident, divisor)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object div = eval(divident, context);
                Object dis = eval(divisor, context);
                return ((Number) div).doubleValue() % ((Number) dis).doubleValue();
            }
        };
    }

    private static Object eval(Expr e, Map<String, Object> context) {
        Object r = e.evaluate(context);

        while (r instanceof Expr) {
            r = eval(((Expr) r), context);
        }

        return r;
    }

    public static Expr multiply(Expr e1, Expr e2) {
        return new OpExpr("multiply", Arrays.asList(e1, e2)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return ((Number) eval(e1, context)).doubleValue() * ((Number) eval(e2, context)).doubleValue();
            }
        };
    }

    public static Expr pow(Expr num, Expr exponent) {
        return new OpExpr("pow", Arrays.asList(num, exponent)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.pow(((Number) eval(num, context)).doubleValue(), ((Number) eval(exponent, context)).doubleValue());
            }
        };
    }

    public static Expr round(Expr e) {
        return new OpExprNoList("round", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return roundHalfEven(eval(e, context), 0);
            }
        };
    }

    public static Expr round(Expr e, Expr place) {
        return new OpExpr("round", Arrays.asList(e, place)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object p = eval(place, context);
                return roundHalfEven(eval(e, context), p == null ? 0 : ((Number) p).intValue());
            }
        };
    }

    /**
     * MongoDB's $round rounds half to even ("banker's rounding"), unlike Math.round; a
     * negative place rounds to tens/hundreds/... The result keeps the input's numeric type.
     */
    private static Number roundHalfEven(Object v, int place) {
        if (v == null) {
            return null;
        }

        BigDecimal bd = new BigDecimal(v.toString()).setScale(place, RoundingMode.HALF_EVEN);

        if (v instanceof Integer) {
            return bd.intValue();
        }

        if (v instanceof Long) {
            return bd.longValue();
        }

        return bd.doubleValue();
    }

    public static Expr sqrt(Expr e) {
        return new OpExprNoList("sqrt", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.sqrt(((Number) eval(e, context)).doubleValue());
            }
        };
    }

    public static Expr subtract(Expr e1, Expr e2) {
        return new OpExpr("substract", Arrays.asList(e1, e2)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return ((Number) eval(e1, context)).doubleValue() - ((Number) eval(e2, context)).doubleValue();
            }
        };
    }

    public static Expr trunc(Expr num, Expr place) {
        return new OpExpr("trunc", Arrays.asList(num, place)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                double n = ((Number) eval(num, context)).doubleValue();
                double m = 1;

                if (eval(place, context) != null || !eval(place, context).equals(0)) {
                    m = Math.pow(10, ((Number) eval(place, context)).intValue());
                }

                n = (int) (n * m);
                n = n / m;
                return n;
            }
        };
    }

    //Array Expression Operators
    public static Expr arrayElemAt(Expr array, Expr index) {
        return new OpExpr("arrayElemAt", Arrays.asList(array, index)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return ((List) eval(array, context)).get(((Number) eval(index, context)).intValue());
            }
        };
    }

    private static Expr arrayElemAt(List lst) {
        //noinspection unchecked
        return new OpExpr("arrayElemAt", lst) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return ((List) (eval((Expr) lst.get(0), context))).get(((Number) (eval((Expr) lst.get(1), context))).intValue());
            }
        };
    }

    public static Expr arrayToObject(Expr array) {
        return new OpExpr("arrayToObject", Collections.singletonList(array)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object v = eval(array, context);

                if (v == null) {
                    return null;
                }

                if (!(v instanceof List)) {
                    throw new IllegalArgumentException("$arrayToObject requires an array, got " + v.getClass().getName());
                }

                // Two input shapes per MongoDB: [[k, v], ...] or [{k: ..., v: ...}, ...];
                // later duplicate keys win, as on the server.
                Map<String, Object> ret = new LinkedHashMap<>();

                for (Object el : (List<?>) v) {
                    if (el instanceof List && ((List<?>) el).size() == 2) {
                        ret.put(((List<?>) el).get(0).toString(), ((List<?>) el).get(1));
                    } else if (el instanceof Map && ((Map<?, ?>) el).containsKey("k") && ((Map<?, ?>) el).containsKey("v")) {
                        ret.put(((Map<?, ?>) el).get("k").toString(), ((Map<?, ?>) el).get("v"));
                    } else {
                        throw new IllegalArgumentException("$arrayToObject: elements must be [k,v] pairs or {k,v} documents");
                    }
                }

                return ret;
            }
        };
    }

    public static Expr concatArrays(Expr... arrays) {
        return new OpExpr("concatArrays", Arrays.asList(arrays)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                List<?> ret = new ArrayList<>();

                for (Expr e : arrays) {
                    //noinspection unchecked
                    ret.addAll(((List) eval(e, context)));
                }

                return ret;
            }
        };
    }

    public static Expr filter(Expr inputArray, String as, Expr cond) {
        Map<String, Expr> input = UtilsMap.of("input", inputArray);
        input.put("as", string(as));
        input.put("cond", cond);
        return new MapOpExpr("filter", input) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                List<Object> ret = new ArrayList<>();

                for (Object el : (List) eval(inputArray, context)) {
                    if (el instanceof Expr) {
                        el = eval(((Expr) el), context);
                    }

                    Map<String, Object> ctx = new ObjectMapperImpl().serialize(el);
                    String fld = as;

                    if (fld == null) {
                        fld = "this";
                    }

                    context.put(fld, el);

                    if (eval(cond, context).equals(Boolean.TRUE)) {
                        ret.add(el);
                    }
                }

                return ret;
            }
        };
    }

    public static Expr first(Expr e) {
        return new OpExprNoList("first", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                // Outside $group, $first is the array operator (MongoDB 4.4+): first element,
                // null for null/missing input, "missing" (represented as null) for empty arrays.
                Object v = eval(e, context);

                if (v == null) {
                    return null;
                }

                if (v instanceof List) {
                    return ((List<?>) v).isEmpty() ? null : ((List<?>) v).get(0);
                }

                throw new IllegalArgumentException("$first requires an array outside of $group, got " + v.getClass().getName());
            }
        };
    }

    public static Expr in(Expr elem, Expr array) {
        return new OpExpr("in", Arrays.asList(elem, array)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object v = eval(elem, context);
                Object arr = eval(array, context);

                if (!(arr instanceof List) && (arr == null || !arr.getClass().isArray())) {
                    throw new IllegalArgumentException("$in requires an array as its second operand");
                }

                int size = arr instanceof List ? ((List<?>) arr).size() : Array.getLength(arr);

                for (int i = 0; i < size; i++) {
                    Object value = arr instanceof List ? ((List<?>) arr).get(i) : Array.get(arr, i);

                    if (value instanceof Expr) {
                        value = eval((Expr) value, context);
                    }

                    if (Objects.equals(value, v)) {
                        return true;
                    }
                }

                return false;
            }
        };
    }

    public static Expr indexOfArray(Expr array, Expr search, Expr start, Expr end) {
        return new OpExpr("indexOfArray", Arrays.asList(array, search, start, end)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                int startNum = ((Number) eval(start, context)).intValue();
                int endNum = ((Number) eval(end, context)).intValue();
                List lst = (List) eval(array, context);
                Object o = eval(search, context);
                return lst.indexOf(o);
            }
        };
    }

    public static Expr isArray(Expr array) {
        return new OpExpr("isArray", Collections.singletonList(array)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return eval(array, context) instanceof List;
            }
        };
    }

    public static Expr last(Expr e) {
        return new OpExprNoList("last", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                // Outside $group, $last is the array operator (MongoDB 4.4+) - see first(Expr).
                Object v = eval(e, context);

                if (v == null) {
                    return null;
                }

                if (v instanceof List) {
                    return ((List<?>) v).isEmpty() ? null : ((List<?>) v).get(((List<?>) v).size() - 1);
                }

                throw new IllegalArgumentException("$last requires an array outside of $group, got " + v.getClass().getName());
            }
        };
    }

    public static Expr map(Expr inputArray, Expr as, Expr in) {
        Map<String, Expr> m = new LinkedHashMap<>();
        m.put("input", inputArray);

        if (as != null) {
            m.put("as", as);
        }

        m.put("in", in);
        return map(m);
    }

    private static Expr map(Map m) {
        @SuppressWarnings("unchecked")
        Map<String, Expr> params = (Map<String, Expr>) m;
        return new MapOpExpr("map", params) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object input = params.get("input") == null ? null : eval(params.get("input"), context);

                if (input == null) {
                    return null;
                }

                if (!(input instanceof List)) {
                    throw new IllegalArgumentException("$map: input must resolve to an array, got " + input.getClass().getName());
                }

                String varName = "this";

                if (params.get("as") != null) {
                    Object as = eval(params.get("as"), context);

                    if (as != null) {
                        varName = as.toString().replaceFirst("^\\$\\$", "");
                    }
                }

                // Work on a copy so the variable binding never leaks into the caller's context.
                Map<String, Object> eff = new HashMap<>(context);
                List<Object> out = new ArrayList<>();

                for (Object el : (List<?>) input) {
                    // Bind both spellings: programmatic Expr.field("x") resolves "x", the JSON
                    // variable reference "$$x" resolves via FieldExpr to the key "$x".
                    eff.put(varName, el);
                    eff.put("$" + varName, el);
                    out.add(eval(params.get("in"), eff));
                }

                return out;
            }
        };
    }

    public static Expr objectToArray(Expr obj) {
        return new OpExprNoList("objectToArray", obj) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                List ret = new ArrayList();

                //noinspection unchecked
                for (Map.Entry e : ((Map<String, Object>) eval(obj, context)).entrySet()) {
                    //noinspection unchecked
                    ret.add(e.getKey());
                    //noinspection unchecked
                    ret.add(e.getValue());
                }

                return ret;
            }
        };
    }

    public static Expr range(Expr start, Expr end, Expr step) {
        return new OpExpr("range", Arrays.asList(start, end, step)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                int startNum = ((Number) eval(start, context)).intValue();
                int endNum = ((Number) eval(end, context)).intValue();
                int stepNum = ((Number) eval(step, context)).intValue();
                List<Number> lst = new ArrayList<>();

                if (endNum < startNum) {
                    if (stepNum > 0) {
                        stepNum = -stepNum;
                    }
                } else if (endNum > startNum) {
                    if (stepNum < 0) {
                        stepNum = -stepNum;
                    }
                }

                // Loop must honour the direction: ascending stops at i >= end, descending stops at
                // i <= end. The old condition (i < end) was always false on the first check for a
                // descending range, so $range(5,1,-1) wrongly returned an empty list.
                for (int i = startNum; stepNum > 0 ? i < endNum : i > endNum; i = i + stepNum) {
                    lst.add(i);
                }

                return lst;
            }
        };
    }

    public static Expr reverseArray(Expr array) {
        return new OpExprNoList("reverseArray", array) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                // Copy before reversing: $reverseArray is a pure read and must not mutate the
                // source list (which may be a live reference into the source document, shared with
                // other pipeline stages).
                List lst = new ArrayList((List) eval(array, context));
                Collections.reverse(lst);
                return lst;
            }
        };
    }

    public static Expr size(Expr array) {
        return new OpExprNoList("size", array) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return ((List) eval(array, context)).size();
            }
        };
    }

    public static Expr reduce(Expr inputArray, Expr initValue, Expr in) {
        return new MapOpExpr("reduce", UtilsMap.of("input", inputArray, "initialValue", initValue, "in", in));
    }

    public static Expr slice(Expr array, Expr pos, Expr n) {
        return new OpExpr("slice", Arrays.asList(array, pos, n)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                List lst = (List) eval(array, context);
                int posN = ((Number) eval(pos, context)).intValue();
                int len = ((Number) eval(n, context)).intValue();
                return lst.subList(posN, posN + len);
            }
        };
    }

    public static Expr slice(Expr array, Expr n) {
        return new OpExpr("slice", Arrays.asList(array, n)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                List lst = (List) eval(array, context);
                int len = ((Number) eval(n, context)).intValue();
                return lst.subList(0, len);
            }
        };
    }

    public static Expr sortArray(Expr input, Expr sortBy) {
        Map<String, Expr> m = new LinkedHashMap<>();
        m.put("input", input);
        m.put("sortBy", sortBy);
        return sortArray(m);
    }

    private static Expr sortArray(Map m) {
        @SuppressWarnings("unchecked")
        Map<String, Expr> params = (Map<String, Expr>) m;
        return new MapOpExpr("sortArray", params) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object in = evalParam(params, "input", context);

                if (in == null) {
                    return null;
                }

                if (!(in instanceof List)) {
                    throw new IllegalArgumentException("$sortArray: input must resolve to an array, got " + in.getClass().getName());
                }

                List<Object> lst = new ArrayList<>((List<?>) in);
                Object sb = evalParam(params, "sortBy", context);
                Comparator<Object> cmp;

                if (sb instanceof Map) {
                    // field spec: {field: 1|-1, ...} - order of the entries is significant
                    @SuppressWarnings("unchecked")
                    Map<String, Object> spec = (Map<String, Object>) sb;
                    cmp = (a, b) -> {
                        for (Map.Entry<String, Object> en : spec.entrySet()) {
                            int dir = ((Number) en.getValue()).intValue() < 0 ? -1 : 1;
                            int c = compareValues(pathValue(a, en.getKey()), pathValue(b, en.getKey())) * dir;

                            if (c != 0) {
                                return c;
                            }
                        }

                        return 0;
                    };
                } else {
                    int dir = sb instanceof Number && ((Number) sb).intValue() < 0 ? -1 : 1;
                    cmp = (a, b) -> compareValues(a, b) * dir;
                }

                lst.sort(cmp);
                return lst;
            }
        };
    }

    public static Expr firstN(Expr input, Expr n) {
        return nOp("firstN", input, n);
    }

    private static Expr firstN(Map m) {
        return nOpFromMap("firstN", m);
    }

    public static Expr lastN(Expr input, Expr n) {
        return nOp("lastN", input, n);
    }

    private static Expr lastN(Map m) {
        return nOpFromMap("lastN", m);
    }

    public static Expr maxN(Expr input, Expr n) {
        return nOp("maxN", input, n);
    }

    private static Expr maxN(Map m) {
        return nOpFromMap("maxN", m);
    }

    public static Expr minN(Expr input, Expr n) {
        return nOp("minN", input, n);
    }

    private static Expr minN(Map m) {
        return nOpFromMap("minN", m);
    }

    private static Expr nOp(String op, Expr input, Expr n) {
        Map<String, Expr> m = new LinkedHashMap<>();
        m.put("n", n);
        m.put("input", input);
        return nOpFromMap(op, m);
    }

    private static Expr nOpFromMap(String op, Map m) {
        @SuppressWarnings("unchecked")
        Map<String, Expr> params = (Map<String, Expr>) m;
        return new MapOpExpr(op, params) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object in = evalParam(params, "input", context);

                if (in == null) {
                    return null;
                }

                if (!(in instanceof List)) {
                    throw new IllegalArgumentException("$" + op + ": input must resolve to an array, got " + in.getClass().getName());
                }

                Object nVal = evalParam(params, "n", context);

                if (!(nVal instanceof Number) || ((Number) nVal).intValue() < 1) {
                    throw new IllegalArgumentException("$" + op + ": n must be a positive integer");
                }

                List<Object> lst = new ArrayList<>((List<?>) in);
                int n = ((Number) nVal).intValue();

                switch (op) {
                    case "firstN":
                        return new ArrayList<>(lst.subList(0, Math.min(n, lst.size())));

                    case "lastN":
                        return new ArrayList<>(lst.subList(Math.max(0, lst.size() - n), lst.size()));

                    case "maxN":
                    case "minN":
                        // MongoDB ignores null/missing elements for $maxN/$minN
                        lst.removeIf(Objects::isNull);
                        lst.sort("maxN".equals(op)
                            ? (a, b) -> compareValues(b, a)
                            : Expr::compareValues);
                        return new ArrayList<>(lst.subList(0, Math.min(n, lst.size())));

                    default:
                        throw new IllegalArgumentException("unknown n-operator " + op);
                }
            }
        };
    }

    /**
     * Null-safe evaluation of an optional operator parameter.
     */
    private static Object evalParam(Map<String, Expr> params, String key, Map<String, Object> context) {
        Expr e = params.get(key);
        return e == null ? null : eval(e, context);
    }

    /**
     * Type-tolerant comparison used by $sortArray/$maxN/$minN: nulls sort lowest, numbers
     * compare numerically across types; otherwise Comparable is used when both sides share
     * a type, with a deterministic class-name fallback (approximating BSON type ordering).
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compareValues(Object a, Object b) {
        if (a == null && b == null) {
            return 0;
        }

        if (a == null) {
            return -1;
        }

        if (b == null) {
            return 1;
        }

        if (a instanceof Number && b instanceof Number) {
            return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
        }

        if (a instanceof Comparable && a.getClass().isInstance(b)) {
            return ((Comparable) a).compareTo(b);
        }

        return a.getClass().getName().compareTo(b.getClass().getName());
    }

    /**
     * Resolves a (possibly dotted) path inside a document element; used by $sortArray's
     * field-spec form.
     */
    private static Object pathValue(Object doc, String path) {
        Object cur = doc;

        for (String p : path.split("\\.")) {
            if (cur instanceof Map) {
                cur = ((Map<?, ?>) cur).get(p);
            } else {
                return null;
            }
        }

        return cur;
    }

    //Boolean Expression Operators
    public static Expr and (Expr... expressions) {
        return new OpExpr("and", Arrays.asList(expressions)) {
            @SuppressWarnings("ConstantConditions")
            @Override
            public Object evaluate(Map<String, Object> context) {
                boolean result = true;
                int idx = 0;

                while (result && idx < expressions.length) {
                    result = result && ((Boolean) eval(expressions[idx], context));
                    idx++;
                }

                return result;
            }
        };
    }

    public static Expr or (Expr... expressions) {
        return new OpExpr("or", Arrays.asList(expressions)) {
            @SuppressWarnings("ConstantConditions")
            @Override
            public Object evaluate(Map<String, Object> context) {
                boolean result = false;
                int idx = 0;

                while (!result && idx < expressions.length) {
                    result = result || ((Boolean) eval(expressions[idx], context));
                    idx++;
                }

                return result;
            }
        };
    }

    public static Expr zip(List<Expr> inputs, Expr useLongestLength, Expr defaults) {
        return new MapOpExpr("zip", UtilsMap.of("inputs", (Expr) new ArrayExpr(inputs.toArray(new Expr[0])), "useLongestLength", useLongestLength, "defaults", defaults));
    }

    public static Expr not(Expr expression) {
        return new OpExprNoList("not", expression) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object evaluate = eval(expression, context);

                if (evaluate instanceof Boolean) {
                    return !((Boolean) evaluate);
                } else if (evaluate instanceof Number) {
                    return ((Number) evaluate).intValue() != 0;
                } else if (evaluate instanceof String) {
                    return "true".equalsIgnoreCase((String) evaluate);
                } else {
                    throw new IllegalArgumentException("Wrong type for not: expr is " + evaluate);
                }
            }
        };
    }

    //Comparison Expression Operators
    public static Expr cmp(Expr e1, Expr e2) {
        return new OpExpr("cmp", Arrays.asList(e1, e2)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                //noinspection unchecked
                return ((Comparable) eval(e1, context)).compareTo(eval(e2, context));
            }
        };
    }

    public static Expr eq(Expr e1) {
        return new OpExprNoList("eq", e1) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return true;
            }
        };
    }

    public static Expr eq(Expr e1, Expr e2) {
        return new OpExpr("eq", Arrays.asList(e1, e2)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                var v1 = eval(e1, context);
                var v2 = eval(e2, context);

                if (v1 == null) {
                    return v2 == null;
                }

                if (v2 == null) {
                    return v1 == null;
                }

                return v1.equals(v2);
                // return eval(e1, context).equals(eval(e2, context));
            }
        };
    }

    public static Expr ne(Expr e1) {
        return new OpExprNoList("ne", e1) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return true;
            }
        };
    }

    public static Expr ne(Expr e1, Expr e2) {
        return new OpExpr("ne", Arrays.asList(e1, e2)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                var v1 = eval(e1, context);
                var v2 = eval(e2, context);

                if (v1 == null) {
                    return v2 != null;
                }

                if (v2 == null) {
                    return v1 != null;
                }

                return !v1.equals(v2);
            }
        };
    }

    public static Expr gt(Expr e1) {
        return new OpExprNoList("gt", e1) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return true;
            }
        };
    }

    public static Expr gt(Expr e1, Expr e2) {
        return new OpExpr("gt", Arrays.asList(e1, e2)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                //noinspection unchecked
                Object eval1 = eval(e1, context);
                Object eval2 = eval(e2, context);

                if (eval1.getClass().equals(eval2.getClass())) {
                    return ((Comparable) eval1).compareTo(eval2) > 0;
                }

                var dv1 = Expr.getDoubleValue(eval1);
                var dv2 = Expr.getDoubleValue(eval2);
                return (dv1 > dv2);
            }
        };
    }

    private static double getDoubleValue(Object o) {
        if (o instanceof Integer) {
            return ((Integer) o).doubleValue();
        }

        if (o instanceof Long) {
            return ((Long) o).doubleValue();
        }

        if (o instanceof Double) {
            return ((Double) o).doubleValue();
        }

        if (o instanceof Float) {
            return ((Float) o).doubleValue();
        }

        if (o instanceof Byte) {
            return ((Byte) o).doubleValue();
        }

        return 0;
    }

    public static Expr lt(Expr e1) {
        return new OpExprNoList("lt", e1) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return true;
            }
        };
    }

    public static Expr lt(Expr e1, Expr e2) {
        return new OpExpr("lt", Arrays.asList(e1, e2)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                //noinspection unchecked
                Object eval1 = eval(e1, context);
                Object eval2 = eval(e2, context);

                if (eval1.getClass().equals(eval2.getClass())) {
                    return ((Comparable) eval1).compareTo(eval2) < 0;
                }

                var dv1 = Expr.getDoubleValue(eval1);
                var dv2 = Expr.getDoubleValue(eval2);
                return (dv1 < dv2);
            }
        };
    }

    public static Expr gte(Expr e1) {
        return new OpExprNoList("gte", e1) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return true;
            }
        };
    }

    public static Expr gte(Expr e1, Expr e2) {
        return new OpExpr("gte", Arrays.asList(e1, e2)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                //noinspection unchecked
                Object eval1 = eval(e1, context);
                Object eval2 = eval(e2, context);

                if (eval1.getClass().equals(eval2.getClass())) {
                    return ((Comparable) eval1).compareTo(eval2) >= 0;
                }

                var dv1 = Expr.getDoubleValue(eval1);
                var dv2 = Expr.getDoubleValue(eval2);
                return (dv1 >= dv2);
            }
        };
    }

    public static Expr lte(Expr e1) {
        return new OpExprNoList("lte", e1) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return true;
            }
        };
    }

    public static Expr lte(Expr e1, Expr e2) {
        return new OpExpr("lte", Arrays.asList(e1, e2)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                //noinspection unchecked
                Object eval1 = eval(e1, context);
                Object eval2 = eval(e2, context);

                if (eval1.getClass().equals(eval2.getClass())) {
                    return ((Comparable) eval1).compareTo(eval2) <= 0;
                }

                var dv1 = Expr.getDoubleValue(eval1);
                var dv2 = Expr.getDoubleValue(eval2);
                return (dv1 <= dv2);
            }
        };
    }

    //Conditional Expression Operators
    public static Expr cond(Expr condition, Expr caseTrue, Expr caseFalse) {
        return new OpExpr("cond", Arrays.asList(condition, caseTrue, caseFalse)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object condResult = eval(condition, context);
                boolean isTrue = condResult != null
                        && !Boolean.FALSE.equals(condResult)
                        && !Integer.valueOf(0).equals(condResult);
                return isTrue ? eval(caseTrue, context) : eval(caseFalse, context);
            }
        };
    }

    public static Expr ifNull(Expr toCheck, Expr resultIfNull) {
        return new OpExpr("ifNull", Arrays.asList(toCheck, resultIfNull)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object result = eval(toCheck, context);

                if (result == null) {
                    return eval(resultIfNull, context);
                }

                return result;
            }
        };
    }

    /**
     * @param branches a map, where key is the condition and value is the result if true
     * @return
     */
    public static Expr switchExpr(Map<Expr, Expr> branches, Expr defaultCase) {
        List<Map<String, Object>> branchList = new ArrayList<>();

        for (Map.Entry<Expr, Expr> ex : branches.entrySet()) {
            branchList.add(UtilsMap.of("case", ex.getKey().toQueryObject(), "then", ex.getValue().toQueryObject()));
        }

        Map<String, Expr> branches1 = UtilsMap.of("branches", new ValueExpr() {
            @Override
            public Object toQueryObject() {
                return branchList;
            }
        });
        branches1.put("default", defaultCase);
        return new MapOpExpr("switch", branches1);
    }

    //Data Size Operators
    public static Expr binarySize(Expr e) {
        return new OpExprNoList("binarySize", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object v = eval(e, context);

                if (v == null) {
                    return null;
                }

                if (v instanceof String) {
                    // strings are measured in UTF-8 bytes, without a trailing null byte
                    return ((String) v).getBytes(StandardCharsets.UTF_8).length;
                }

                if (v instanceof byte[]) {
                    return ((byte[]) v).length;
                }

                throw new IllegalArgumentException("$binarySize requires a string or binData argument, got " + v.getClass().getName());
            }
        };
    }

    public static Expr bsonSize(Expr e) {
        return new OpExprNoList("bsonSize", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object v = eval(e, context);

                if (v == null) {
                    return null;
                }

                if (v instanceof Map) {
                    //noinspection unchecked
                    return BsonEncoder.encodeDocument((Map<String, Object>) v).length;
                }

                throw new IllegalArgumentException("$bsonSize requires a document argument, got " + v.getClass().getName());
            }
        };
    }

    //Custom Aggregation
    public static Expr function(String code, Expr args) {
        return function(code, args, null);
    }

    public static Expr function(String code, Expr args, String lang) {
        if (lang == null) {
            lang = "js";
        }

        return new MapOpExpr("function", UtilsMap.of("body", string(code), "args", args, "lang", string(lang)));
    }

    // $accumulator: {
    //    init: <code>,
    //    initArgs: <array expression>,        // Optional
    //    accumulate: <code>,
    //    accumulateArgs: <array expression>,
    //    merge: <code>,
    //    finalize: <code>,                    // Optional
    //    lang: <string>
    //  }
    public static Expr accumulator(String initCode, String accumulateCode, Expr accArgs, String mergeCode) {
        return accumulator(initCode, null, accumulateCode, accArgs, mergeCode, null, null);
    }

    public static Expr accumulator(String initCode, Expr initArgs, String accumulateCode, Expr accArgs, String mergeCode) {
        return accumulator(initCode, initArgs, accumulateCode, accArgs, mergeCode, null, null);
    }

    public static Expr accumulator(String initCode, Expr initArgs, String accumulateCode, Expr accArgs, String mergeCode, String finalizeCode) {
        return accumulator(initCode, initArgs, accumulateCode, accArgs, mergeCode, finalizeCode, null);
    }

    public static Expr accumulator(String initCode, Expr initArgs, String accumulateCode, Expr accArgs, String mergeCode, String finalizeCode, String lang) {
        if (lang == null) {
            lang = "js";
        }

        Map<String, Expr> map = UtilsMap.of("init", string(initCode), "initArgs", initArgs, "accumulate", string(accumulateCode), "accumulateArgs", accArgs, "merge", string(mergeCode), "finalize",
                                            string(finalizeCode), "lang", string(lang));
        return new MapOpExpr("accumulator", map);
    }

    public static Expr dayOfMonth(Expr date) {
        return new OpExprNoList("dayOfMonth", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                // MongoDB's date operators default to UTC; the JVM default zone made results depend
                // on the deployment environment (#250).
                GregorianCalendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
                Object val = eval(date, context);

                if (val instanceof Date) {
                    cal.setTime((Date) val);
                } else if (val instanceof Number) {
                    cal.setTimeInMillis(((Number) val).longValue());
                } else {
                    throw new IllegalArgumentException("second expr got wrong type: " + val.getClass());
                }

                return cal.get(Calendar.DAY_OF_MONTH);
            }
        };
    }

    public static Expr dayOfWeek(Expr date) {
        return new OpExprNoList("dayOfWeek", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                // MongoDB's date operators default to UTC; the JVM default zone made results depend
                // on the deployment environment (#250).
                GregorianCalendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
                Object val = eval(date, context);

                if (val instanceof Date) {
                    cal.setTime((Date) val);
                } else if (val instanceof Number) {
                    cal.setTimeInMillis(((Number) val).longValue());
                } else {
                    throw new IllegalArgumentException("second expr got wrong type: " + val.getClass());
                }

                return cal.get(Calendar.DAY_OF_WEEK);
            }
        };
    }

    public static Expr dateFromParts(Expr year) {
        return new MapOpExpr("dateFromParts", UtilsMap.of("year", year));
    }

    private static Expr dateFromParts(Map m) {
        //noinspection unchecked
        return new MapOpExpr("dateFromParts", m);
    }

    //Date Expression Operators

    public static Expr dateFromParts(Expr year, Expr month) {
        return new MapOpExpr("dateFromParts", UtilsMap.of("year", year, "month", month));
    }

    public static Expr dateFromParts(Expr year, Expr month, Expr day, Expr hour) {
        return new MapOpExpr("dateFromParts", UtilsMap.of("year", year, "month", month, "day", day, "hour", hour));
    }

    public static Expr dateFromParts(Expr year, Expr month, Expr day, Expr hour, Expr min, Expr sec) {
        return new MapOpExpr("dateFromParts", UtilsMap.of("year", year, "month", month, "day", day, "hour", hour, "minute", min, "second", sec));
    }

    public static Expr dateFromParts(Expr year, Expr month, Expr day, Expr hour, Expr min, Expr sec, Expr ms) {
        return new MapOpExpr("dateFromParts", UtilsMap.of("year", year, "month", month, "day", day, "hour", hour, "minute", min, "second", sec, "millisecond", ms));
    }

    public static Expr dateFromParts(Expr year, Expr month, Expr day, Expr hour, Expr min, Expr sec, Expr ms, Expr timeZone) {
        return new MapOpExpr("dateFromParts", UtilsMap.of("year", year, "month", month, "day", day, "hour", hour, "minute", min, "second", sec, "millisecond", ms, "timezone", timeZone));
    }

    public static Expr isoDateFromParts(Expr isoWeekYear) {
        return new MapOpExpr("dateFromParts", UtilsMap.of("isoWeekYear", isoWeekYear));
    }

    public static Expr isoDateFromParts(Expr isoWeekYear, Expr isoWeek) {
        return new MapOpExpr("dateFromParts", UtilsMap.of("isoWeekYear", isoWeekYear, "isoWeek", isoWeek));
    }

    public static Expr isoDateFromParts(Expr isoWeekYear, Expr isoWeek, Expr isoDayOfWeek) {
        return new MapOpExpr("dateFromParts", UtilsMap.of("isoWeekYear", isoWeekYear, "isoWeek", isoWeek, "isoDayOfWeek", isoDayOfWeek));
    }

    public static Expr isoDateFromParts(Expr isoWeekYear, Expr isoWeek, Expr isoDayOfWeek, Expr hour) {
        return new MapOpExpr("dateFromParts", UtilsMap.of("isoWeekYear", isoWeekYear, "isoWeek", isoWeek, "isoDayOfWeek", isoDayOfWeek, "hour", hour));
    }

    public static Expr isoDateFromParts(Expr isoWeekYear, Expr isoWeek, Expr isoDayOfWeek, Expr hour, Expr min) {
        return new MapOpExpr("dateFromParts", UtilsMap.of("isoWeekYear", isoWeekYear, "isoWeek", isoWeek, "isoDayOfWeek", isoDayOfWeek, "hour", hour, "minute", min));
    }

    public static Expr isoDateFromParts(Expr isoWeekYear, Expr isoWeek, Expr isoDayOfWeek, Expr hour, Expr min, Expr sec) {
        return new MapOpExpr("dateFromParts", UtilsMap.of("isoWeekYear", isoWeekYear, "isoWeek", isoWeek, "isoDayOfWeek", isoDayOfWeek, "hour", hour, "minute", min, "second", sec));
    }

    public static Expr isoDateFromParts(Expr isoWeekYear, Expr isoWeek, Expr isoDayOfWeek, Expr hour, Expr min, Expr sec, Expr ms) {
        return new MapOpExpr("dateFromParts", UtilsMap.of("isoWeekYear", isoWeekYear, "isoWeek", isoWeek, "isoDayOfWeek", isoDayOfWeek, "hour", hour, "minute", min, "second", sec, "millisecond", ms));
    }

    public static Expr isoDateFromParts(Expr isoWeekYear, Expr isoWeek, Expr isoDayOfWeek, Expr hour, Expr min, Expr sec, Expr ms, Expr timeZone) {
        return new MapOpExpr("dateFromParts",
                             UtilsMap.of("isoWeekYear", isoWeekYear, "isoWeek", isoWeek, "isoDayOfWeek", isoDayOfWeek, "hour", hour, "minute", min, "second", sec, "millisecond", ms, "timezone", timeZone));
    }

    public static Expr dateFromString(Expr dateString, Expr format, Expr timezone, Expr onError, Expr onNull) {
        return new MapOpExpr("dateFromString", UtilsMap.of("dateString", dateString, "format", format, "timezone", timezone, "onError", onError, "onNull", onNull));
    }

    public static Expr dateToParts(Expr date, Expr timezone, boolean iso8601) {
        return new MapOpExpr("dateToParts", UtilsMap.of("date", date, "timezone", timezone, "iso8601", bool(iso8601)));
    }

    public static Expr dateToString(Expr date, Expr format, Expr timezone, Expr onNull) {
        return new MapOpExpr("dateToString", UtilsMap.of("dateString", date, "format", format, "timezone", timezone, "onNull", onNull));
    }

    public static Expr dayOfYear(Expr date) {
        return new OpExprNoList("dayOfYear", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                // MongoDB's date operators default to UTC; the JVM default zone made results depend
                // on the deployment environment (#250).
                GregorianCalendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
                Object val = eval(date, context);

                if (val instanceof Date) {
                    cal.setTime((Date) val);
                } else if (val instanceof Number) {
                    cal.setTimeInMillis(((Number) val).longValue());
                } else {
                    throw new IllegalArgumentException("second expr got wrong type: " + val.getClass());
                }

                return cal.get(Calendar.DAY_OF_YEAR);
            }
        };
    }

    public static Expr hour(Expr date) {
        return new OpExprNoList("hour", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                // MongoDB's date operators default to UTC; the JVM default zone made results depend
                // on the deployment environment (#250).
                GregorianCalendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
                Object val = eval(date, context);

                if (val instanceof Date) {
                    cal.setTime((Date) val);
                } else if (val instanceof Number) {
                    cal.setTimeInMillis(((Number) val).longValue());
                } else {
                    throw new IllegalArgumentException("second expr got wrong type: " + val.getClass());
                }

                return cal.get(Calendar.HOUR_OF_DAY);
            }
        };
    }

    public static Expr isoDayOfWeek(Expr date) {
        return new OpExprNoList("isoDayOfWeek", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                // MongoDB's date operators default to UTC; the JVM default zone made results depend
                // on the deployment environment (#250).
                GregorianCalendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
                Object val = eval(date, context);

                if (val instanceof Date) {
                    cal.setTime((Date) val);
                } else if (val instanceof Number) {
                    cal.setTimeInMillis(((Number) val).longValue());
                } else {
                    throw new IllegalArgumentException("second expr got wrong type: " + val.getClass());
                }

                // Calendar.DAY_OF_WEEK is Sunday=1..Saturday=7 (identical to the non-ISO $dayOfWeek).
                // MongoDB's $isoDayOfWeek is ISO-8601: Monday=1..Sunday=7 (#250).
                return LocalDate.ofInstant(cal.toInstant(), ZoneOffset.UTC).getDayOfWeek().getValue();
            }
        };
    }

    public static Expr isoWeek(Expr date) {
        return new OpExprNoList("isoWeek", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                // MongoDB's date operators default to UTC; the JVM default zone made results depend
                // on the deployment environment (#250).
                GregorianCalendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
                Object val = eval(date, context);

                if (val instanceof Date) {
                    cal.setTime((Date) val);
                } else if (val instanceof Number) {
                    cal.setTimeInMillis(((Number) val).longValue());
                } else {
                    throw new IllegalArgumentException("second expr got wrong type: " + val.getClass());
                }

                // Was Calendar.WEEK_OF_MONTH (1-5), a structurally different value from the ISO-8601
                // week-of-year (1-53) that $isoWeek is documented to return (#250).
                return LocalDate.ofInstant(cal.toInstant(), ZoneOffset.UTC).get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
            }
        };
    }

    public static Expr isoWeekYear(Expr date) {
        return new OpExprNoList("isoWeekYear", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                // MongoDB's date operators default to UTC; the JVM default zone made results depend
                // on the deployment environment (#250).
                GregorianCalendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
                Object val = eval(date, context);

                if (val instanceof Date) {
                    cal.setTime((Date) val);
                } else if (val instanceof Number) {
                    cal.setTimeInMillis(((Number) val).longValue());
                } else {
                    throw new IllegalArgumentException("second expr got wrong type: " + val.getClass());
                }

                // Was Calendar.WEEK_OF_YEAR, i.e. a week number (1-53) where $isoWeekYear is
                // documented to return a 4-digit ISO week-based year, e.g. 2026 (#250).
                return LocalDate.ofInstant(cal.toInstant(), ZoneOffset.UTC).get(IsoFields.WEEK_BASED_YEAR);
            }
        };
    }

    public static Expr millisecond(Expr date) {
        return new OpExprNoList("millisecond", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                // MongoDB's date operators default to UTC; the JVM default zone made results depend
                // on the deployment environment (#250).
                GregorianCalendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
                Object val = eval(date, context);

                if (val instanceof Date) {
                    cal.setTime((Date) val);
                } else if (val instanceof Number) {
                    cal.setTimeInMillis(((Number) val).longValue());
                } else {
                    throw new IllegalArgumentException("second expr got wrong type: " + val.getClass());
                }

                return cal.get(Calendar.MILLISECOND);
            }
        };
    }

    public static Expr minute(Expr date) {
        return new OpExprNoList("minute", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                // MongoDB's date operators default to UTC; the JVM default zone made results depend
                // on the deployment environment (#250).
                GregorianCalendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
                Object val = eval(date, context);

                if (val instanceof Date) {
                    cal.setTime((Date) val);
                } else if (val instanceof Number) {
                    cal.setTimeInMillis(((Number) val).longValue());
                } else {
                    throw new IllegalArgumentException("second expr got wrong type: " + val.getClass());
                }

                return cal.get(Calendar.MINUTE);
            }
        };
    }

    public static Expr month(Expr date) {
        return new OpExprNoList("month", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                // MongoDB's date operators default to UTC; the JVM default zone made results depend
                // on the deployment environment (#250).
                GregorianCalendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
                Object val = eval(date, context);

                if (val instanceof Date) {
                    cal.setTime((Date) val);
                } else if (val instanceof Number) {
                    cal.setTimeInMillis(((Number) val).longValue());
                } else {
                    throw new IllegalArgumentException("second expr got wrong type: " + val.getClass());
                }

                // Calendar.MONTH is 0-based; MongoDB's $month is 1-12 (#250).
                return cal.get(Calendar.MONTH) + 1;
            }
        };
    }

    public static Expr second(Expr date) {
        return new OpExprNoList("second", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                // MongoDB's date operators default to UTC; the JVM default zone made results depend
                // on the deployment environment (#250).
                GregorianCalendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
                Object val = eval(date, context);

                if (val instanceof Date) {
                    cal.setTime((Date) val);
                } else if (val instanceof Number) {
                    cal.setTimeInMillis(((Number) val).longValue());
                } else {
                    throw new IllegalArgumentException("second expr got wrong type: " + val.getClass());
                }

                return cal.get(Calendar.SECOND);
            }
        };
    }

    public static Expr toDate(Expr e) {
        return new OpExprNoList("toDate", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return toDateValue(eval(e, context), "$toDate");
            }
        };
    }

    /**
     * Shared date coercion for $toDate and the date arithmetic operators: Date passthrough,
     * epoch millis for numbers, ObjectId timestamp, ISO-8601 strings; null stays null.
     */
    private static Date toDateValue(Object val, String op) {
        if (val == null) {
            return null;
        }

        if (val instanceof Date) {
            return (Date) val;
        }

        if (val instanceof Number) {
            return new Date(((Number) val).longValue());
        }

        if (val instanceof MorphiumId) {
            return new Date(((MorphiumId) val).getTime());
        }

        if (val instanceof ObjectId) {
            return ((ObjectId) val).getDate();
        }

        if (val instanceof String) {
            return parseDateString((String) val, op);
        }

        throw new IllegalArgumentException(op + ": cannot convert " + val.getClass().getName() + " to a date");
    }

    private static Date parseDateString(String s, String op) {
        try {
            return Date.from(Instant.parse(s));
        } catch (Exception ignored) {
        }

        try {
            return Date.from(OffsetDateTime.parse(s).toInstant());
        } catch (Exception ignored) {
        }

        try {
            // date-time without offset is interpreted as UTC, matching MongoDB's default timezone
            return Date.from(LocalDateTime.parse(s).atZone(ZoneOffset.UTC).toInstant());
        } catch (Exception ignored) {
        }

        try {
            return Date.from(LocalDate.parse(s).atStartOfDay(ZoneOffset.UTC).toInstant());
        } catch (Exception ignored) {
        }

        throw new IllegalArgumentException(op + ": cannot parse date string '" + s + "'");
    }

    public static Expr dateAdd(Expr startDate, Expr unit, Expr amount) {
        return dateArith("dateAdd", dateArithParams(startDate, unit, amount, null), 1);
    }

    public static Expr dateAdd(Expr startDate, Expr unit, Expr amount, Expr timezone) {
        return dateArith("dateAdd", dateArithParams(startDate, unit, amount, timezone), 1);
    }

    private static Expr dateAdd(Map m) {
        return dateArith("dateAdd", (Map<String, Expr>) m, 1);
    }

    public static Expr dateSubtract(Expr startDate, Expr unit, Expr amount) {
        return dateArith("dateSubtract", dateArithParams(startDate, unit, amount, null), -1);
    }

    public static Expr dateSubtract(Expr startDate, Expr unit, Expr amount, Expr timezone) {
        return dateArith("dateSubtract", dateArithParams(startDate, unit, amount, timezone), -1);
    }

    private static Expr dateSubtract(Map m) {
        return dateArith("dateSubtract", (Map<String, Expr>) m, -1);
    }

    private static Map<String, Expr> dateArithParams(Expr startDate, Expr unit, Expr amount, Expr timezone) {
        Map<String, Expr> m = new LinkedHashMap<>();
        m.put("startDate", startDate);
        m.put("unit", unit);
        m.put("amount", amount);

        if (timezone != null) {
            m.put("timezone", timezone);
        }

        return m;
    }

    private static Expr dateArith(String opName, Map<String, Expr> params, int sign) {
        return new MapOpExpr(opName, params) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                String op = "$" + opName;
                Object start = evalParam(params, "startDate", context);
                Object unitV = evalParam(params, "unit", context);
                Object amountV = evalParam(params, "amount", context);

                if (start == null || unitV == null || amountV == null) {
                    return null;
                }

                long amount = sign * ((Number) amountV).longValue();
                ZoneId zone = zoneParam(params, context);
                ZonedDateTime zdt = toDateValue(start, op).toInstant().atZone(zone);

                switch (unitV.toString()) {
                    case "year":
                        zdt = zdt.plusYears(amount);
                        break;

                    case "quarter":
                        zdt = zdt.plusMonths(3 * amount);
                        break;

                    case "month":
                        zdt = zdt.plusMonths(amount);
                        break;

                    case "week":
                        zdt = zdt.plusWeeks(amount);
                        break;

                    case "day":
                        zdt = zdt.plusDays(amount);
                        break;

                    case "hour":
                        zdt = zdt.plusHours(amount);
                        break;

                    case "minute":
                        zdt = zdt.plusMinutes(amount);
                        break;

                    case "second":
                        zdt = zdt.plusSeconds(amount);
                        break;

                    case "millisecond":
                        zdt = zdt.plus(amount, ChronoUnit.MILLIS);
                        break;

                    default:
                        throw new IllegalArgumentException(op + ": unknown unit '" + unitV + "'");
                }

                return Date.from(zdt.toInstant());
            }
        };
    }

    public static Expr dateDiff(Expr startDate, Expr endDate, Expr unit) {
        return dateDiff(startDate, endDate, unit, null, null);
    }

    public static Expr dateDiff(Expr startDate, Expr endDate, Expr unit, Expr timezone, Expr startOfWeek) {
        Map<String, Expr> m = new LinkedHashMap<>();
        m.put("startDate", startDate);
        m.put("endDate", endDate);
        m.put("unit", unit);

        if (timezone != null) {
            m.put("timezone", timezone);
        }

        if (startOfWeek != null) {
            m.put("startOfWeek", startOfWeek);
        }

        return dateDiff(m);
    }

    private static Expr dateDiff(Map m) {
        @SuppressWarnings("unchecked")
        Map<String, Expr> params = (Map<String, Expr>) m;
        return new MapOpExpr("dateDiff", params) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object sv = evalParam(params, "startDate", context);
                Object ev = evalParam(params, "endDate", context);
                Object uv = evalParam(params, "unit", context);

                if (sv == null || ev == null || uv == null) {
                    return null;
                }

                String unit = uv.toString();
                ZoneId zone = zoneParam(params, context);
                DayOfWeek sow = startOfWeekParam(params, context);
                // $dateDiff counts unit *boundaries crossed*, not elapsed units: truncate both
                // ends to the unit first, then take the calendar difference.
                ZonedDateTime s = truncDate(toDateValue(sv, "$dateDiff").toInstant().atZone(zone), unit, sow, "$dateDiff");
                ZonedDateTime e = truncDate(toDateValue(ev, "$dateDiff").toInstant().atZone(zone), unit, sow, "$dateDiff");

                switch (unit) {
                    case "year":
                        return (long)(e.getYear() - s.getYear());

                    case "quarter":
                        return (long)((e.getYear() * 4 + (e.getMonthValue() - 1) / 3) - (s.getYear() * 4 + (s.getMonthValue() - 1) / 3));

                    case "month":
                        return (long)((e.getYear() * 12 + e.getMonthValue()) - (s.getYear() * 12 + s.getMonthValue()));

                    case "week":
                        return ChronoUnit.DAYS.between(s, e) / 7;

                    case "day":
                        return ChronoUnit.DAYS.between(s, e);

                    case "hour":
                        return ChronoUnit.HOURS.between(s, e);

                    case "minute":
                        return ChronoUnit.MINUTES.between(s, e);

                    case "second":
                        return ChronoUnit.SECONDS.between(s, e);

                    case "millisecond":
                        return ChronoUnit.MILLIS.between(s, e);

                    default:
                        throw new IllegalArgumentException("$dateDiff: unknown unit '" + unit + "'");
                }
            }
        };
    }

    public static Expr dateTrunc(Expr date, Expr unit) {
        return dateTrunc(date, unit, null, null, null);
    }

    public static Expr dateTrunc(Expr date, Expr unit, Expr binSize, Expr timezone, Expr startOfWeek) {
        Map<String, Expr> m = new LinkedHashMap<>();
        m.put("date", date);
        m.put("unit", unit);

        if (binSize != null) {
            m.put("binSize", binSize);
        }

        if (timezone != null) {
            m.put("timezone", timezone);
        }

        if (startOfWeek != null) {
            m.put("startOfWeek", startOfWeek);
        }

        return dateTrunc(m);
    }

    private static Expr dateTrunc(Map m) {
        @SuppressWarnings("unchecked")
        Map<String, Expr> params = (Map<String, Expr>) m;
        return new MapOpExpr("dateTrunc", params) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object dv = evalParam(params, "date", context);
                Object uv = evalParam(params, "unit", context);

                if (dv == null || uv == null) {
                    return null;
                }

                String unit = uv.toString();
                ZoneId zone = zoneParam(params, context);
                DayOfWeek sow = startOfWeekParam(params, context);
                Object bsV = evalParam(params, "binSize", context);
                long binSize = bsV == null ? 1L : ((Number) bsV).longValue();

                if (binSize < 1) {
                    throw new IllegalArgumentException("$dateTrunc: binSize must be a positive integer");
                }

                Date dateVal = toDateValue(dv, "$dateTrunc");

                if (binSize == 1) {
                    return Date.from(truncDate(dateVal.toInstant().atZone(zone), unit, sow, "$dateTrunc").toInstant());
                }

                // binSize > 1: bins are anchored at MongoDB's reference point 2000-01-01T00:00:00.
                switch (unit) {
                    case "year":
                    case "quarter":
                    case "month": {
                        int monthsPerUnit = "year".equals(unit) ? 12 : "quarter".equals(unit) ? 3 : 1;
                        long binMonths = binSize * monthsPerUnit;
                        LocalDate ld = dateVal.toInstant().atZone(zone).toLocalDate();
                        long monthsSinceRef = (ld.getYear() - 2000L) * 12 + (ld.getMonthValue() - 1);
                        long floored = Math.floorDiv(monthsSinceRef, binMonths) * binMonths;
                        return Date.from(LocalDate.of(2000, 1, 1).plusMonths(floored).atStartOfDay(zone).toInstant());
                    }

                    default: {
                        long unitMs = unitMillis(unit, "$dateTrunc");
                        LocalDate anchorDay = "week".equals(unit)
                            ? LocalDate.of(2000, 1, 1).with(TemporalAdjusters.previousOrSame(sow))
                            : LocalDate.of(2000, 1, 1);
                        long anchor = anchorDay.atStartOfDay(zone).toInstant().toEpochMilli();
                        long span = binSize * unitMs;
                        return new Date(Math.floorDiv(dateVal.getTime() - anchor, span) * span + anchor);
                    }
                }
            }
        };
    }

    private static long unitMillis(String unit, String op) {
        switch (unit) {
            case "millisecond":
                return 1L;

            case "second":
                return 1000L;

            case "minute":
                return 60_000L;

            case "hour":
                return 3_600_000L;

            case "day":
                return 86_400_000L;

            case "week":
                return 604_800_000L;

            default:
                throw new IllegalArgumentException(op + ": unknown unit '" + unit + "'");
        }
    }

    /**
     * Truncates to the start of the given unit; used by $dateDiff and $dateTrunc.
     */
    private static ZonedDateTime truncDate(ZonedDateTime zdt, String unit, DayOfWeek startOfWeek, String op) {
        switch (unit) {
            case "year":
                return zdt.toLocalDate().withDayOfYear(1).atStartOfDay(zdt.getZone());

            case "quarter": {
                LocalDate ld = zdt.toLocalDate();
                return LocalDate.of(ld.getYear(), ((ld.getMonthValue() - 1) / 3) * 3 + 1, 1).atStartOfDay(zdt.getZone());
            }

            case "month":
                return zdt.toLocalDate().withDayOfMonth(1).atStartOfDay(zdt.getZone());

            case "week":
                return zdt.toLocalDate().with(TemporalAdjusters.previousOrSame(startOfWeek)).atStartOfDay(zdt.getZone());

            case "day":
                return zdt.truncatedTo(ChronoUnit.DAYS);

            case "hour":
                return zdt.truncatedTo(ChronoUnit.HOURS);

            case "minute":
                return zdt.truncatedTo(ChronoUnit.MINUTES);

            case "second":
                return zdt.truncatedTo(ChronoUnit.SECONDS);

            case "millisecond":
                return zdt.truncatedTo(ChronoUnit.MILLIS);

            default:
                throw new IllegalArgumentException(op + ": unknown unit '" + unit + "'");
        }
    }

    private static ZoneId zoneParam(Map<String, Expr> params, Map<String, Object> context) {
        // MongoDB's date operators default to UTC unless an explicit timezone is given
        Object tz = evalParam(params, "timezone", context);
        return tz == null ? ZoneOffset.UTC : ZoneId.of(tz.toString());
    }

    private static DayOfWeek startOfWeekParam(Map<String, Expr> params, Map<String, Object> context) {
        Object v = evalParam(params, "startOfWeek", context);

        if (v == null) {
            // MongoDB's documented default for $dateDiff/$dateTrunc
            return DayOfWeek.SUNDAY;
        }

        String s = v.toString().toLowerCase(Locale.ROOT);

        for (DayOfWeek dow : DayOfWeek.values()) {
            if (dow.name().toLowerCase(Locale.ROOT).startsWith(s.substring(0, Math.min(3, s.length())))) {
                return dow;
            }
        }

        throw new IllegalArgumentException("unknown startOfWeek '" + v + "'");
    }

    public static Expr week(Expr date) {
        return new OpExprNoList("week", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                // MongoDB's date operators default to UTC; the JVM default zone made results depend
                // on the deployment environment (#250).
                GregorianCalendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
                Object val = eval(date, context);

                if (val instanceof Date) {
                    cal.setTime((Date) val);
                } else if (val instanceof Number) {
                    cal.setTimeInMillis(((Number) val).longValue());
                } else {
                    throw new IllegalArgumentException("second expr got wrong type: " + val.getClass());
                }

                // MongoDB's $week is Sunday-based and 0-53: week 1 starts on the year's first Sunday,
                // and the days before it are week 0. Calendar.WEEK_OF_YEAR follows the JVM locale's
                // week rules instead, so this was both wrong and locale-dependent.
                LocalDate day = LocalDate.ofInstant(cal.toInstant(), ZoneOffset.UTC);
                int dayOfYear = day.getDayOfYear();
                // getDayOfWeek(): Monday=1..Sunday=7 -> days from Jan 1 to the first Sunday
                int jan1Dow = day.withDayOfYear(1).getDayOfWeek().getValue();
                int firstSundayDayOfYear = 1 + ((7 - jan1Dow) % 7);
                return dayOfYear < firstSundayDayOfYear ? 0 : ((dayOfYear - firstSundayDayOfYear) / 7) + 1;
            }
        };
    }

    public static Expr year(Expr date) {
        return new OpExprNoList("year", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                // MongoDB's date operators default to UTC; the JVM default zone made results depend
                // on the deployment environment (#250).
                GregorianCalendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
                Object val = eval(date, context);

                if (val instanceof Date) {
                    cal.setTime((Date) val);
                } else if (val instanceof Number) {
                    cal.setTimeInMillis(((Number) val).longValue());
                } else {
                    throw new IllegalArgumentException("second expr got wrong type: " + val.getClass());
                }

                return cal.get(Calendar.YEAR);
            }
        };
    }

    public static Expr literal(Expr e) {
        return new OpExprNoList("literal", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return eval(e, context);
            }
        };
    }

    public static Expr mergeObjects(Expr doc) {
        return new OpExprNoList("mergeObjects", doc) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return eval(doc, context);
            }
        };
    }

    public static Expr mergeObjects(Expr... docs) {
        return new OpExpr("mergeObjects", Arrays.asList(docs)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Map<String, Object> res = new HashMap<>();

                for (Expr e : docs) {
                    Object val = eval(e, context);

                    if (!(val instanceof Map)) {
                        throw new IllegalArgumentException("cannot merge non documents!");
                    }

                    //noinspection unchecked
                    res.putAll((Map <? extends String, ? >) val);
                }

                return res;
            }
        };
    }

    public static Expr allElementsTrue(Expr... e) {
        return new OpExpr("allElementsTrue", Arrays.asList(e)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                boolean ret = true;

                for (Expr el : e) {
                    if (!Boolean.TRUE.equals(eval(el, context))) {
                        ret = false;
                        break;
                    }
                }

                return ret;
            }
        };
    }

    private static Expr allElementsTrue(List lst) {
        //noinspection unchecked
        return new OpExpr("alleElementsTrue", lst) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                boolean ret = true;

                //noinspection unchecked
                for (Expr el : (List<Expr>) lst) {
                    if (!Boolean.TRUE.equals(eval(el, context))) {
                        ret = false;
                        break;
                    }
                }

                return ret;
            }
        };
    }

    public static Expr anyElementTrue(Expr... e) {
        return new OpExpr("anyElementsTrue", Arrays.asList(e)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                boolean ret = false;

                for (Expr el : e) {
                    if (Boolean.TRUE.equals(eval(el, context))) {
                        ret = true;
                        break;
                    }
                }

                return ret;
            }
        };
    }

    public static Expr setDifference(Expr e1, Expr e2) {
        return new OpExpr("setDifference", Arrays.asList(e1, e2)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                List<Object> a = setOperand(eval(e1, context), "$setDifference");
                List<Object> b = setOperand(eval(e2, context), "$setDifference");

                if (a == null || b == null) {
                    return null;
                }

                List<Object> ret = distinctList(a);
                ret.removeIf(v -> containsVal(b, v));
                return ret;
            }
        };
    }

    //Set Expression Operators

    public static Expr setEquals(Expr... e) {
        return new OpExpr("setEquals", Arrays.asList(e)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                List<List<Object>> sets = new ArrayList<>();

                for (Expr el : e) {
                    List<Object> cur = setOperand(eval(el, context), "$setEquals");

                    if (cur == null) {
                        throw new IllegalArgumentException("$setEquals requires array arguments");
                    }

                    sets.add(distinctList(cur));
                }

                List<Object> first = sets.get(0);

                for (List<Object> distinct : sets) {
                    if (first.size() != distinct.size() || !distinct.stream().allMatch(v -> containsVal(first, v))) {
                        return false;
                    }
                }

                return true;
            }
        };
    }

    public static Expr setIntersection(Expr... e) {
        return new OpExpr("setIntersection", Arrays.asList(e)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                List<Object> ret = null;

                for (Expr el : e) {
                    List<Object> cur = setOperand(eval(el, context), "$setIntersection");

                    if (cur == null) {
                        return null;
                    }

                    if (ret == null) {
                        ret = distinctList(cur);
                    } else {
                        List<Object> finalCur = cur;
                        ret.removeIf(v -> !containsVal(finalCur, v));
                    }
                }

                return ret == null ? new ArrayList<>() : ret;
            }
        };
    }

    public static Expr setIsSubset(Expr e1, Expr e2) {
        return new OpExpr("setIsSubset", Arrays.asList(e1, e2)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                List<Object> a = setOperand(eval(e1, context), "$setIsSubset");
                List<Object> b = setOperand(eval(e2, context), "$setIsSubset");

                if (a == null || b == null) {
                    throw new IllegalArgumentException("$setIsSubset requires array arguments");
                }

                return a.stream().allMatch(v -> containsVal(b, v));
            }
        };
    }

    public static Expr setUnion(Expr... e) {
        return new OpExpr("setUnion", Arrays.asList(e)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                // The old implementation added each evaluated *array* as a single set element
                // instead of unioning the elements themselves.
                List<Object> ret = new ArrayList<>();

                for (Expr el : e) {
                    List<Object> cur = setOperand(eval(el, context), "$setUnion");

                    if (cur == null) {
                        return null;
                    }

                    for (Object v : cur) {
                        if (!containsVal(ret, v)) {
                            ret.add(v);
                        }
                    }
                }

                return ret;
            }
        };
    }

    private static List<Object> setOperand(Object v, String op) {
        if (v == null) {
            return null;
        }

        if (v instanceof Collection) {
            return new ArrayList<>((Collection<?>) v);
        }

        throw new IllegalArgumentException(op + " requires array arguments, got " + v.getClass().getName());
    }

    private static List<Object> distinctList(Collection<?> c) {
        List<Object> out = new ArrayList<>();

        for (Object o : c) {
            if (!containsVal(out, o)) {
                out.add(o);
            }
        }

        return out;
    }

    /**
     * Set membership with MongoDB's numeric equality: 1 and 1.0 are the same set element.
     */
    private static boolean containsVal(Collection<?> c, Object v) {
        for (Object o : c) {
            if (Objects.equals(o, v)) {
                return true;
            }

            if (o instanceof Number && v instanceof Number
                    && Double.compare(((Number) o).doubleValue(), ((Number) v).doubleValue()) == 0) {
                return true;
            }
        }

        return false;
    }

    public static Expr concat(Expr... e) {
        return new OpExpr("concat", Arrays.asList(e)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                StringBuilder b = new StringBuilder();

                for (Expr ex : e) {
                    Object evaluate = eval(ex, context);

                    if (evaluate != null) {
                        b.append(evaluate);
                    }
                }

                return b.toString();
            }
        };
    }

    public static Expr indexOfBytes(Expr str, Expr substr, Expr start, Expr end) {
        return new OpExpr("indexOfBytes", Arrays.asList(str, substr, start, end)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return indexOfImpl("$indexOfBytes", true, Arrays.asList(str, substr, start, end), context);
            }
        };
    }

    private static Expr indexOfBytes(List lst) {
        //noinspection unchecked
        return new OpExpr("indexOfBytes", lst) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return indexOfImpl("$indexOfBytes", true, lst, context);
            }
        };
    }

    public static Expr indexOfCP(Expr str, Expr substr, Expr start, Expr end) {
        return new OpExpr("indexOfCP", Arrays.asList(str, substr, start, end)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return indexOfImpl("$indexOfCP", false, Arrays.asList(str, substr, start, end), context);
            }
        };
    }

    private static Expr indexOfCP(List lst) {
        //noinspection unchecked
        return new OpExpr("indexOfCP", lst) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return indexOfImpl("$indexOfCP", false, lst, context);
            }
        };
    }

    /**
     * $indexOfBytes/$indexOfCP: index of the first occurrence of a substring within an
     * optional [start, end) range, counted in UTF-8 bytes or code points. A null string
     * input yields null (MongoDB semantics); the match must lie entirely inside the range.
     */
    private static Object indexOfImpl(String op, boolean bytes, List<Expr> args, Map<String, Object> context) {
        Object sv = args.get(0) == null ? null : eval(args.get(0), context);

        if (sv == null) {
            return null;
        }

        String s = stringArg(op, sv);
        String sub = stringArg(op, eval(args.get(1), context));
        int start = args.size() > 2 && args.get(2) != null ? ((Number) eval(args.get(2), context)).intValue() : 0;
        Integer endArg = args.size() > 3 && args.get(3) != null ? ((Number) eval(args.get(3), context)).intValue() : null;

        if (start < 0) {
            throw new IllegalArgumentException(op + ": start must be non-negative");
        }

        if (bytes) {
            byte[] b = s.getBytes(StandardCharsets.UTF_8);
            byte[] sb = sub.getBytes(StandardCharsets.UTF_8);
            int end = endArg == null ? b.length : Math.min(endArg, b.length);

            outer:
            for (int i = start; i + sb.length <= end; i++) {
                for (int j = 0; j < sb.length; j++) {
                    if (b[i + j] != sb[j]) {
                        continue outer;
                    }
                }

                return i;
            }

            return -1;
        }

        int n = s.codePointCount(0, s.length());
        int subCp = sub.codePointCount(0, sub.length());
        int end = endArg == null ? n : Math.min(endArg, n);

        for (int cp = start; cp + subCp <= end; cp++) {
            if (s.startsWith(sub, s.offsetByCodePoints(0, cp))) {
                return cp;
            }
        }

        return -1;
    }

    private static String stringArg(String op, Object v) {
        if (v instanceof String) {
            return (String) v;
        }

        throw new IllegalArgumentException(op + " requires a string argument, got " + (v == null ? "null" : v.getClass().getName()));
    }

    //concat

    public static Expr match(Expr expr) {
        return new OpExprNoList("$match", expr) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return eval(expr, context);
            }
        };
    }

    public static Expr expr(Expr expr) {
        return new OpExprNoList("expr", expr) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                LoggerFactory.getLogger(Expr.class).error("not implemented yet,sorry");
                return eval(expr, context);
            }
        };
    }

    public static Expr ltrim(Expr str, Expr charsToTrim) {
        return new MapOpExpr("ltrim", UtilsMap.of("input", str, "chars", charsToTrim));
    }

    public static Expr regex(Expr field, Expr regex, Expr options) {
        return new Expr() {
            @Override
            public Object toQueryObject() {
                return UtilsMap.of(field.toQueryObject().toString().substring(1), UtilsMap.of("$regex", regex.toQueryObject(), "$options", options.toQueryObject()));
            }

            @Override
            public Object evaluate(Map<String, Object> context) {
                return null;
            }
        };
    }

    public static Expr regexFind(Expr input, Expr regex, Expr options) {
        return new MapOpExpr("regexFind", UtilsMap.of("input", input, "regex", regex, "options", options));
    }

    public static Expr regexFindAll(Expr input, Expr regex, Expr options) {
        return new MapOpExpr("regexFindAll", UtilsMap.of("input", input, "regex", regex, "options", options));
    }

    public static Expr project(Map<String, Expr> expr) {
        return new MapOpExpr("$project", expr);
    }

    public static Expr split(Expr str, Expr delimiter) {
        return new OpExpr("split", Arrays.asList(str, delimiter)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                String s = eval(str, context).toString();
                return Arrays.asList(s.split(eval(delimiter, context).toString()));
            }
        };
    }

    public static Expr strLenBytes(Expr str) {
        return new OpExpr("strLenBytes", Collections.singletonList(str)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                // MongoDB errors (does not return null) when the argument is not a string
                return stringArg("$strLenBytes", eval(str, context)).getBytes(StandardCharsets.UTF_8).length;
            }
        };
    }

    public static Expr regexMatch(Expr input, Expr regex, Expr options) {
        return new MapOpExpr("regexMatch", UtilsMap.of("input", input, "regex", regex, "options", options));
    }

    public static Expr replaceOne(Expr input, Expr find, Expr replacement) {
        return new MapOpExpr("replaceOne", UtilsMap.of("input", input, "find", find, "replacement", replacement));
    }

    public static Expr replaceAll(Expr input, Expr find, Expr replacement) {
        return new MapOpExpr("replaceAll", UtilsMap.of("input", input, "find", find, "replacement", replacement));
    }

    public static Expr rtrim(Expr str, Expr charsToTrim) {
        return new MapOpExpr("rtrim", UtilsMap.of("input", str, "chars", charsToTrim));
    }

    public static Expr strLenCP(Expr str) {
        return new OpExpr("strLenCP", Collections.singletonList(str)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                String s = stringArg("$strLenCP", eval(str, context));
                return s.codePointCount(0, s.length());
            }
        };
    }

    public static Expr strcasecmp(Expr e1, Expr e2) {
        return new OpExpr("strcasecmp", Arrays.asList(e1, e2)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                // null arguments are coerced to "" like MongoDB's string comparison operators
                Object v1 = eval(e1, context);
                Object v2 = eval(e2, context);
                String s1 = v1 == null ? "" : v1.toString();
                String s2 = v2 == null ? "" : v2.toString();
                return Integer.signum(s1.compareToIgnoreCase(s2));
            }
        };
    }

    public static Expr substr(Expr str, Expr start, Expr len) {
        return new OpExpr("substr", Arrays.asList(str, start, len)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                String s = (String) eval(str, context);
                int st = ((Number) eval(start, context)).intValue();
                int l = ((Number) eval(len, context)).intValue();
                return s.substring(st, st + l);
            }
        };
    }

    public static Expr substrBytes(Expr str, Expr index, Expr count) {
        return new OpExpr("substrBytes", Arrays.asList(str, index, count)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                String s = stringArg("$substrBytes", eval(str, context));
                int st = ((Number) eval(index, context)).intValue();
                int cnt = ((Number) eval(count, context)).intValue();

                if (st < 0 || cnt < 0) {
                    throw new IllegalArgumentException("$substrBytes: index and count must be non-negative");
                }

                byte[] b = s.getBytes(StandardCharsets.UTF_8);

                if (st >= b.length) {
                    return "";
                }

                int end = Math.min(b.length, st + cnt);

                // UTF-8 continuation bytes are 10xxxxxx; landing on one means the range splits a
                // multi-byte character, which MongoDB rejects
                if ((b[st] & 0xC0) == 0x80 || (end < b.length && (b[end] & 0xC0) == 0x80)) {
                    throw new IllegalArgumentException("$substrBytes: byte range splits a UTF-8 character");
                }

                return new String(b, st, end - st, StandardCharsets.UTF_8);
            }
        };
    }

    public static Expr substrCP(Expr str, Expr cpIdx, Expr cpCount) {
        return new OpExpr("substrCP", Arrays.asList(str, cpIdx, cpCount)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                String s = stringArg("$substrCP", eval(str, context));
                int st = ((Number) eval(cpIdx, context)).intValue();
                int cnt = ((Number) eval(cpCount, context)).intValue();

                if (st < 0 || cnt < 0) {
                    throw new IllegalArgumentException("$substrCP: index and count must be non-negative");
                }

                int n = s.codePointCount(0, s.length());

                if (st >= n) {
                    return "";
                }

                int startChar = s.offsetByCodePoints(0, st);
                int endChar = s.offsetByCodePoints(startChar, Math.min(cnt, n - st));
                return s.substring(startChar, endChar);
            }
        };
    }

    public static Expr toLower(Expr e) {
        return new OpExprNoList("toLower", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                if (eval(e, context) == null) {
                    return null;
                }

                return ((String) eval(e, context)).toLowerCase(Locale.ROOT);
            }
        };
    }

    public static Expr toStr(Expr e) {
        return new OpExprNoList("toString", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return (eval(e, context) != null) ? eval(e, context).toString() : null;
            }
        };
    }

    public static Expr toUpper(Expr e) {
        return new OpExprNoList("toUpper", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                if (eval(e, context) == null) {
                    return null;
                }

                return eval(e, context).toString().toUpperCase();
            }
        };
    }

    public static Expr meta(String metaDataKeyword) {
        return new ValueExpr() {
            @Override
            public Object toQueryObject() {
                return UtilsMap.of("$meta", metaDataKeyword);
            }
        };
    }

    public static Expr trim(Expr str, Expr charsToTrim) {
        return new MapOpExpr("trim", UtilsMap.of("input", str, "chars", charsToTrim));
    }

    public static Expr sin(Expr e) {
        return new OpExprNoList("sin", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.sin(((Number) eval(e, context)).doubleValue());
            }
        };
    }

    public static Expr cos(Expr e) {
        return new OpExprNoList("cos", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.cos(((Number) eval(e, context)).doubleValue());
            }
        };
    }

    public static Expr tan(Expr e) {
        return new OpExprNoList("tan", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.tan(((Number) eval(e, context)).doubleValue());
            }
        };
    }

    //Trigonometry Expression Operators

    public static Expr asin(Expr e) {
        return new OpExprNoList("asin", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.asin(((Number) eval(e, context)).doubleValue());
            }
        };
    }

    public static Expr acos(Expr e) {
        return new OpExprNoList("acos", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.acos(((Number) eval(e, context)).doubleValue());
            }
        };
    }

    public static Expr atan(Expr e) {
        return new OpExprNoList("atan", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.atan(((Number) eval(e, context)).doubleValue());
            }
        };
    }

    public static Expr atan2(Expr e, Expr e2) {
        return new OpExpr("atan2", Arrays.asList(e, e2)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.atan2(((Number) eval(e, context)).doubleValue(), ((Number) eval(e2, context)).doubleValue());
            }
        };
    }

    private static Expr atan2(List lst) {
        //noinspection unchecked
        return new OpExpr("atan2", lst) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.atan2(((Number) (eval((Expr) lst.get(0), context))).doubleValue(), ((Number) (eval((Expr) lst.get(1), context))).doubleValue());
            }
        };
    }

    public static Expr sinh(Expr e) {
        return new OpExprNoList("sinh", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object v = eval(e, context);
                return v == null ? null : Math.sinh(((Number) v).doubleValue());
            }
        };
    }

    public static Expr cosh(Expr e) {
        return new OpExprNoList("cosh", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object v = eval(e, context);
                return v == null ? null : Math.cosh(((Number) v).doubleValue());
            }
        };
    }

    public static Expr tanh(Expr e) {
        return new OpExprNoList("tanh", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object v = eval(e, context);
                return v == null ? null : Math.tanh(((Number) v).doubleValue());
            }
        };
    }

    public static Expr asinh(Expr e) {
        return new OpExprNoList("asinh", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object v = eval(e, context);

                if (v == null) {
                    return null;
                }

                // inverse hyperbolic sine; the old implementation computed sinh instead
                double x = ((Number) v).doubleValue();
                return Math.log(x + Math.sqrt(x * x + 1));
            }
        };
    }

    public static Expr acosh(Expr e) {
        return new OpExprNoList("acosh", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object v = eval(e, context);

                if (v == null) {
                    return null;
                }

                double x = ((Number) v).doubleValue();
                return Math.log(x + Math.sqrt(x * x - 1));
            }
        };
    }

    public static Expr atanh(Expr e) {
        return new OpExprNoList("atanh", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object v = eval(e, context);

                if (v == null) {
                    return null;
                }

                double x = ((Number) v).doubleValue();
                return 0.5 * Math.log((1 + x) / (1 - x));
            }
        };
    }

    /**
     * @deprecated MongoDB's $atanh takes exactly one argument - use {@link #atanh(Expr)}.
     */
    @Deprecated
    public static Expr atanh(Expr e1, Expr e2) {
        return new OpExpr("atanh", Arrays.asList(e1, e2)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                // Previously logged and silently returned 0, which is a valid atanh result and
                // therefore indistinguishable from a correct answer.
                throw new IllegalArgumentException("$atanh takes exactly one argument");
            }
        };
    }

    private static Expr atanh(List lst) {
        if (lst.size() == 1) {
            return atanh((Expr) lst.get(0));
        }

        //noinspection unchecked
        return new OpExpr("atanh", lst) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                throw new IllegalArgumentException("$atanh takes exactly one argument");
            }
        };
    }

    /**
     * @deprecated misspelled - MongoDB's operator is $degreesToRadians; kept for source
     * compatibility, use {@link #degreesToRadians(Expr)}.
     */
    @Deprecated
    public static Expr degreesToRadian(Expr e) {
        return degreesToRadians(e);
    }

    public static Expr degreesToRadians(Expr e) {
        return new OpExprNoList("degreesToRadians", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object v = eval(e, context);
                return v == null ? null : Math.toRadians(((Number) v).doubleValue());
            }
        };
    }

    public static Expr radiansToDegrees(Expr e) {
        return new OpExprNoList("radiansToDegrees", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.toDegrees(((Number) eval(e, context)).doubleValue());
            }
        };
    }

    public static Expr convert(Expr input, Expr to, Expr onError, Expr onNull) {
        return new MapOpExpr("convert", UtilsMap.of("input", input, "to", to, "onError", onError, "onNull", onNull)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object in = eval(input, context);

                if (in == null) {
                    return eval(onNull, context);
                }

                //type check???
                return in; //TODO: migrate data?
            }
        };
    }

    private static Expr convert(Map map) {
        //noinspection unchecked
        return new MapOpExpr("convert", map) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object in = eval((Expr) map.get("input"), context);

                if (in == null) {
                    return eval((Expr) map.get("onNull"), context);
                }

                //type check???
                return in; //TODO: migrate data?
            }
        };
    }

    public static Expr isNumber(Expr e) {
        return new OpExprNoList("isNumber", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return eval(e, context) instanceof Number;
            }
        };
    }

    public static Expr convert(Expr input, Expr to) {
        return convert(input, to, null, null);
    }

    //Type Expression

    public static Expr convert(Expr input, Expr to, Expr onError) {
        return convert(input, to, onError, null);
    }

    public static Expr toBool(Expr e) {
        return new OpExprNoList("toBool", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Boolean.valueOf(eval(e, context).toString());
            }
        };
    }

    public static Expr toDecimal(Expr e) {
        return new OpExprNoList("toDecimal", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return (Double.valueOf(eval(e, context).toString()));
            }
        };
    }

    public static Expr toDouble(Expr e) {
        return new OpExprNoList("toDouble", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                //LoggerFactory.getLogger(Expr.class).error("not implemented yet,sorry");
                var res = eval(e, context);

                if (res instanceof Number) {
                    return ((Number) res).doubleValue();
                }

                if (res instanceof String) {
                    return Integer.valueOf((String) res).doubleValue();
                }
                return 0;
            }
        };
    }

    public static Expr toInt(Expr e) {
        return new OpExprNoList("toInt", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                var res = eval(e, context);

                if (res instanceof Number) {
                    return ((Number) res).intValue();
                }

                if (res instanceof String) {
                    return Integer.valueOf((String) res).intValue();
                }
                return 0;
            }
        };
    }

    public static Expr toLong(Expr e) {
        return new OpExprNoList("toLong", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                var res = eval(e, context);

                if (res instanceof Number) {
                    return ((Number) res).longValue();
                }

                if (res instanceof String) {
                    return Integer.valueOf((String) res).longValue();
                }
                return 0;
            }
        };
    }

    public static Expr toObjectId(Expr e) {
        return new OpExprNoList("toObjectId", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                //LoggerFactory.getLogger(Expr.class).error("not implemented yet,sorry");
                Object o = eval(e, context);

                if (o instanceof String) {
                    o = new MorphiumId((String) o);
                } else if (o instanceof ObjectId) {
                    o = new MorphiumId(((ObjectId) o).toHexString());
                }

                return o;
            }
        };
    }

    public static Expr type(Expr e) {
        return new OpExprNoList("type", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return bsonTypeName(eval(e, context));
            }
        };
    }

    /**
     * BSON type name as returned by MongoDB's $type expression operator. A missing field
     * cannot be distinguished from an explicit null here, so both report "null" (the server
     * would report "missing" for absent fields).
     */
    private static String bsonTypeName(Object v) {
        if (v == null) {
            return "null";
        }

        if (v instanceof Double || v instanceof Float) {
            return "double";
        }

        if (v instanceof String) {
            return "string";
        }

        if (v instanceof Map) {
            return "object";
        }

        if (v instanceof byte[]) {
            return "binData";
        }

        if (v instanceof List || v.getClass().isArray()) {
            return "array";
        }

        if (v instanceof MorphiumId || v instanceof ObjectId) {
            return "objectId";
        }

        if (v instanceof Boolean) {
            return "bool";
        }

        if (v instanceof Date) {
            return "date";
        }

        if (v instanceof java.util.regex.Pattern) {
            return "regex";
        }

        if (v instanceof Integer || v instanceof Short || v instanceof Byte) {
            return "int";
        }

        if (v instanceof Long) {
            return "long";
        }

        if (v instanceof BigDecimal) {
            return "decimal";
        }

        return v.getClass().getSimpleName();
    }

    public static Expr nullExpr() {
        return new ValueExpr() {
            @Override
            public Object toQueryObject() {
                return null;
            }
        };
    }

    public static Expr addToSet(Expr e) {
        return new OpExprNoList("addToSet", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                // Accumulator without a group context; returning null here silently produced
                // wrong pipeline results. The in-memory $group stage handles $addToSet itself.
                throw new IllegalArgumentException("$addToSet is an accumulator and only valid inside $group / $setWindowFields");
            }
        };
    }

    public static Expr avg(Expr... e) {
        return new OpExpr("avg", Arrays.asList(e)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                double sum = 0;

                for (Expr expr : e) {
                    sum += ((Number) eval(expr, context)).doubleValue();
                }

                return sum / e.length;
            }
        };
    }

    public static Expr avg(Expr e) {
        return new OpExprNoList("avg", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object value = eval(e, context);
                if (value instanceof List) {
                    // Single-argument form over an array: reduce to the mean of numeric elements
                    // (mirrors sum(Expr)); returning the array unchanged was a bug (#246).
                    double sum = 0.0;
                    int count = 0;
                    for (Object item : (List<?>) value) {
                        if (item instanceof Number) {
                            sum += ((Number) item).doubleValue();
                            count++;
                        }
                    }
                    return count > 0 ? sum / count : null;
                }
                return value;
            }
        };
    }

    //Group stage

    public static Expr max(Expr... e) {
        return new OpExpr("max", Arrays.asList(e)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object max = null;

                for (Expr expr : e) {
                    Object v = eval(expr, context);

                    if (max == null) {
                        max = v;
                    } else //noinspection unchecked
                        if (((Comparable) max).compareTo(v) < 0) {
                            max = v;
                        }
                }

                return max;
            }
        };
    }

    public static Expr max(Expr e) {
        return new OpExprNoList("max", e) {
            @Override
            @SuppressWarnings("unchecked")
            public Object evaluate(Map<String, Object> context) {
                Object value = eval(e, context);
                if (value instanceof List) {
                    // Single-argument form over an array: reduce to the largest element,
                    // preserving its type (#246). Returning the array unchanged was a bug.
                    Object max = null;
                    for (Object item : (List<?>) value) {
                        if (item == null) {
                            continue;
                        }
                        if (max == null || ((Comparable) max).compareTo(item) < 0) {
                            max = item;
                        }
                    }
                    return max;
                }
                return value;
            }
        };
    }

    public static Expr min(Expr... e) {
        return new OpExpr("min", Arrays.asList(e)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object min = null;

                for (Expr expr : e) {
                    Object v = eval(expr, context);

                    if (min == null) {
                        min = v;
                    } else //noinspection unchecked
                        if (((Comparable) min).compareTo(v) > 0) {
                            min = v;
                        }
                }

                return min;
            }
        };
    }

    public static Expr min(Expr e) {
        return new OpExprNoList("min", e) {
            @Override
            @SuppressWarnings("unchecked")
            public Object evaluate(Map<String, Object> context) {
                Object value = eval(e, context);
                if (value instanceof List) {
                    // Single-argument form over an array: reduce to the smallest element,
                    // preserving its type (#246). Returning the array unchanged was a bug.
                    Object min = null;
                    for (Object item : (List<?>) value) {
                        if (item == null) {
                            continue;
                        }
                        if (min == null || ((Comparable) min).compareTo(item) > 0) {
                            min = item;
                        }
                    }
                    return min;
                }
                return value;
            }
        };
    }

    public static Expr push(Expr e) {
        return new OpExprNoList("push", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                // Accumulator without a group context; returning null here silently produced
                // wrong pipeline results. The in-memory $group stage handles $push itself.
                throw new IllegalArgumentException("$push is an accumulator and only valid inside $group / $setWindowFields");
            }
        };
    }

    public static Expr stdDevPop(Expr e) {
        return new OpExprNoList("stdDevPop", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return computeStdDevPop(asCollection(eval(e, context)));
            }
        };
    }

    public static Expr stdDevPop(Expr... e) {
        return new OpExpr("stdDevPop", Arrays.asList(e)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                List<Object> values = new ArrayList<>();

                for (Expr expr : e) {
                    values.add(eval(expr, context));
                }

                return computeStdDevPop(values);
            }
        };
    }

    public static Expr stdDevSamp(Expr e) {
        return new OpExprNoList("stdDevSamp", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return computeStdDevSamp(asCollection(eval(e, context)));
            }
        };
    }

    public static Expr stdDevSamp(Expr... e) {
        return new OpExpr("stdDevSamp", Arrays.asList(e)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                List<Object> values = new ArrayList<>();

                for (Expr expr : e) {
                    values.add(eval(expr, context));
                }

                return computeStdDevSamp(values);
            }
        };
    }

    /**
     * Wraps a single evaluated value into a one-element collection so the single-argument
     * $stdDevPop/$stdDevSamp form (which usually receives an array, e.g. a field reference
     * to an array field) also works when the expression evaluates to a scalar or to null.
     */
    private static Collection<?> asCollection(Object value) {
        if (value instanceof Collection) {
            return (Collection<?>) value;
        }

        if (value == null) {
            return Collections.emptyList();
        }

        return Collections.singletonList(value);
    }

    /**
     * Population standard deviation ($stdDevPop) over a set of raw values.
     *
     * MongoDB semantics (https://www.mongodb.com/docs/manual/reference/operator/aggregation/stdDevPop/):
     * non-numeric and missing values are ignored; if no numeric values remain, the result is
     * {@code null}.
     *
     * Implementation note: two-pass algorithm (first pass computes the mean, second pass sums
     * the squared deviations from that mean). Chosen over Welford's single-pass, streaming
     * algorithm because the in-memory driver already materializes every group's raw values
     * before finalizing the accumulator, so the numerical-stability benefit of Welford's
     * method isn't needed here and the two-pass form is simpler to read/verify.
     */
    public static Double computeStdDevPop(Collection<?> rawValues) {
        List<Double> numbers = numericValuesOf(rawValues);

        if (numbers.isEmpty()) {
            return null;
        }

        double mean = meanOf(numbers);
        double sumSquaredDiff = sumSquaredDiffOf(numbers, mean);
        return Math.sqrt(sumSquaredDiff / numbers.size());
    }

    /**
     * Sample standard deviation ($stdDevSamp) over a set of raw values.
     *
     * MongoDB semantics (https://www.mongodb.com/docs/manual/reference/operator/aggregation/stdDevSamp/):
     * non-numeric and missing values are ignored; with fewer than two numeric values the
     * sample standard deviation is undefined and the result is {@code null}.
     *
     * Same two-pass algorithm as {@link #computeStdDevPop(Collection)}, dividing the sum of
     * squared deviations by (n - 1) instead of n (Bessel's correction).
     */
    public static Double computeStdDevSamp(Collection<?> rawValues) {
        List<Double> numbers = numericValuesOf(rawValues);

        if (numbers.size() < 2) {
            return null;
        }

        double mean = meanOf(numbers);
        double sumSquaredDiff = sumSquaredDiffOf(numbers, mean);
        return Math.sqrt(sumSquaredDiff / (numbers.size() - 1));
    }

    private static List<Double> numericValuesOf(Collection<?> rawValues) {
        List<Double> numbers = new ArrayList<>();

        if (rawValues == null) {
            return numbers;
        }

        for (Object v : rawValues) {
            if (v instanceof Number) {
                numbers.add(((Number) v).doubleValue());
            }
            //non-numeric / missing values are ignored, per MongoDB $stdDevPop/$stdDevSamp semantics
        }

        return numbers;
    }

    private static double meanOf(List<Double> numbers) {
        double sum = 0.0;

        for (double d : numbers) {
            sum += d;
        }

        return sum / numbers.size();
    }

    private static double sumSquaredDiffOf(List<Double> numbers, double mean) {
        double sum = 0.0;

        for (double d : numbers) {
            sum += (d - mean) * (d - mean);
        }

        return sum;
    }

    public static Expr sum(Expr e) {
        return new OpExprNoList("sum", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object value = e.evaluate(context);
                if (value instanceof List) {
                    // Sum array elements
                    double sum = 0.0;
                    for (Object item : (List<?>) value) {
                        if (item instanceof Number) {
                            sum += ((Number) item).doubleValue();
                        }
                    }
                    return sum;
                } else if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                }
                return 0;
            }
        };
    }

    public static Expr sum(Expr... e) {
        return new OpExpr("sum", Arrays.asList(e)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                double sum = 0.0;
                for (Expr expr : Arrays.asList(e)) {
                    Object value = expr.evaluate(context);
                    if (value instanceof Number) {
                        sum += ((Number) value).doubleValue();
                    } else if (value instanceof List) {
                        for (Object item : (List<?>) value) {
                            if (item instanceof Number) {
                                sum += ((Number) item).doubleValue();
                            }
                        }
                    }
                }
                return sum;
            }
        };
    }

    public static Expr rand() {
        return new MapOpExpr("rand", new LinkedHashMap<>()) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.random();
            }
        };
    }

    private static Expr rand(Map m) {
        // JSON form is {$rand: {}} - the (empty) parameter document is irrelevant
        return rand();
    }

    public static Expr sampleRate(Expr rate) {
        return new OpExprNoList("sampleRate", rate) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                double r = ((Number) eval(rate, context)).doubleValue();

                if (r < 0 || r > 1) {
                    throw new IllegalArgumentException("$sampleRate must be between 0 and 1");
                }

                // Math.random() is in [0,1), so rate 1.0 always matches and 0.0 never does
                return Math.random() < r;
            }
        };
    }

    public static Expr median(Expr input) {
        Map<String, Expr> m = new LinkedHashMap<>();
        m.put("input", input);
        m.put("method", string("approximate"));
        return median(m);
    }

    private static Expr median(Map m) {
        @SuppressWarnings("unchecked")
        Map<String, Expr> params = (Map<String, Expr>) m;
        return new MapOpExpr("median", params) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                List<Double> nums = numericList(evalParam(params, "input", context));

                if (nums == null || nums.isEmpty()) {
                    return null;
                }

                Collections.sort(nums);
                return nearestRank(nums, 0.5);
            }
        };
    }

    public static Expr percentile(Expr input, Expr p) {
        Map<String, Expr> m = new LinkedHashMap<>();
        m.put("input", input);
        m.put("p", p);
        m.put("method", string("approximate"));
        return percentile(m);
    }

    private static Expr percentile(Map m) {
        @SuppressWarnings("unchecked")
        Map<String, Expr> params = (Map<String, Expr>) m;
        return new MapOpExpr("percentile", params) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                List<Double> nums = numericList(evalParam(params, "input", context));
                Object ps = evalParam(params, "p", context);

                if (nums == null || nums.isEmpty() || ps == null) {
                    return null;
                }

                if (!(ps instanceof List)) {
                    throw new IllegalArgumentException("$percentile: p must be an array of numbers");
                }

                Collections.sort(nums);
                List<Object> out = new ArrayList<>();

                for (Object p : (List<?>) ps) {
                    out.add(nearestRank(nums, ((Number) p).doubleValue()));
                }

                return out;
            }
        };
    }

    /**
     * Nearest-rank percentile over a pre-sorted list; like MongoDB's 'approximate' method it
     * always returns an actual input value (no interpolation).
     */
    private static Double nearestRank(List<Double> sorted, double p) {
        int idx = (int) Math.ceil(p * sorted.size()) - 1;

        if (idx < 0) {
            idx = 0;
        }

        if (idx >= sorted.size()) {
            idx = sorted.size() - 1;
        }

        return sorted.get(idx);
    }

    /**
     * Numeric values of an input that may be an array, a scalar or null; non-numeric
     * elements are ignored like in MongoDB's statistical accumulators.
     */
    private static List<Double> numericList(Object v) {
        if (v == null) {
            return null;
        }

        List<Double> out = new ArrayList<>();

        if (v instanceof Collection) {
            for (Object o : (Collection<?>) v) {
                if (o instanceof Number) {
                    out.add(((Number) o).doubleValue());
                }
            }
        } else if (v instanceof Number) {
            out.add(((Number) v).doubleValue());
        }

        return out;
    }

    public static Expr let(Map<String, Expr> vars, Expr in) {
        return new ValueExpr() {
            @Override
            public Object toQueryObject() {
                Map<String, Object> map = new HashMap<>();

                for (Map.Entry<String, Expr> e : vars.entrySet()) {
                    map.put(e.getKey(), e.getValue().toQueryObject());
                }

                return UtilsMap.of("$let", UtilsMap.of("vars", (Object) map, "in", in.toQueryObject()));
            }

            @Override
            public Object evaluate(Map<String, Object> context) {
                Map<String, Object> effectiveContext = new HashMap<>(context);

                for (String k : vars.keySet()) {
                    effectiveContext.put(k, eval(vars.get(k), context));
                }

                return eval(in, effectiveContext);
            }
        };
    }

    private static Expr let(Map m) {
        return new ValueExpr() {
            @Override
            public Object toQueryObject() {
                return m;
            }

            @Override
            public Object evaluate(Map<String, Object> context) {
                Map<String, Object> effectiveContext = new HashMap<>(context);

                //noinspection unchecked
                for (String k : ((Map<String, Expr>) m.get("vars")).keySet()) {
                    //noinspection unchecked
                    effectiveContext.put(k, eval(((Map<String, Expr>) m.get("vars")).get(k), context));
                }

                return eval(((Expr) m.get("in")), effectiveContext);
            }
        };
    }

    public static Expr doc(Map<String, Object> document) {
        return new ValueExpr() {
            @Override
            public Object toQueryObject() {
                return document;
            }
        };
    }

    private static class MapOpExpr extends ValueExpr {
        private final String operation;
        private final Map<String, Expr> params;

        private MapOpExpr(String type, Map<String, Expr> par) {
            super();

            if (!type.startsWith("$")) {
                type = "$" + type;
            }

            operation = type;
            params = par;
        }

        @Override
        public Object toQueryObject() {
            Map m = new LinkedHashMap();
            Map ret = UtilsMap.of(operation, m);

            for (Map.Entry<String, Expr> e : params.entrySet()) {
                //noinspection unchecked
                if (e.getValue() != null) {
                    m.put(e.getKey(), e.getValue().toQueryObject());
                }
            }

            return ret;
        }

        /**
         * MapOpExpr inherits ValueExpr.evaluate(), which returns the operator's own JSON shape.
         * For $dateFromParts/$isoDateFromParts that is silently wrong - they must construct a Date
         * (#260). Every other map-form operator keeps the inherited behaviour.
         */
        @Override
        public Object evaluate(Map<String, Object> context) {
            if ("$dateFromParts".equals(operation)) {
                // MongoDB has no separate $isoDateFromParts operator - the ISO variant is the same
                // $dateFromParts distinguished by its isoWeekYear/isoWeek/isoDayOfWeek field names.
                return evaluateDateFromParts(context, params.containsKey("isoWeekYear"));
            }

            if ("$function".equals(operation) || "$accumulator".equals(operation)) {
                // Inheriting ValueExpr.evaluate() would silently return the operator's own JSON
                // shape; running the code would need a server-side JavaScript engine (#255).
                throw new UnsupportedOperationException(operation
                    + " is not supported by the in-memory expression engine: it requires a server-side JavaScript engine");
            }

            return super.evaluate(context);
        }

        private int intPart(Map<String, Object> context, String name, int fallback) {
            Expr e = params.get(name);

            if (e == null) {
                return fallback;
            }

            Object v = e.evaluate(context);
            return v instanceof Number ? ((Number) v).intValue() : fallback;
        }

        private Object evaluateDateFromParts(Map<String, Object> context, boolean iso) {
            // Defaults to UTC, honouring an explicit "timezone" part like real MongoDB.
            ZoneId zone = ZoneOffset.UTC;
            Expr tz = params.get("timezone");

            if (tz != null) {
                Object tzVal = tz.evaluate(context);

                if (tzVal != null) {
                    zone = ZoneId.of(tzVal.toString());
                }
            }

            int hour = intPart(context, "hour", 0);
            int minute = intPart(context, "minute", 0);
            int second = intPart(context, "second", 0);
            int millis = intPart(context, "millisecond", 0);

            if (iso) {
                // Jan 4th is always in ISO week 1 of its week-based year - anchor there, then move to
                // the requested ISO week and weekday.
                LocalDate day = LocalDate.of(intPart(context, "isoWeekYear", 1970), 1, 4)
                                .with(IsoFields.WEEK_OF_WEEK_BASED_YEAR, intPart(context, "isoWeek", 1))
                                .with(ChronoField.DAY_OF_WEEK, intPart(context, "isoDayOfWeek", 1));
                return Date.from(day.atStartOfDay()
                                 .plusHours(hour).plusMinutes(minute).plusSeconds(second)
                                 .plusNanos(millis * 1_000_000L)
                                 .atZone(zone).toInstant());
            }

            // Built additively from Jan 1st so out-of-range parts roll over, as MongoDB documents
            // (e.g. month 14 becomes February of the following year).
            return Date.from(LocalDateTime.of(intPart(context, "year", 1970), 1, 1, 0, 0, 0)
                             .plusMonths(intPart(context, "month", 1) - 1L)
                             .plusDays(intPart(context, "day", 1) - 1L)
                             .plusHours(hour).plusMinutes(minute).plusSeconds(second)
                             .plusNanos(millis * 1_000_000L)
                             .atZone(zone).toInstant());
        }
    }

    private static abstract class OpExpr extends Expr {
        private final String operation;
        private final List<Expr> params;

        private OpExpr(String type, List<Expr> par) {
            if (!type.startsWith("$")) {
                type = "$" + type;
            }

            operation = type;
            params = par;
        }

        @Override
        public Object toQueryObject() {
            List<Object> p = new ArrayList<>();

            for (Expr e : params) {
                if (e != null) {
                    p.add(e.toQueryObject());
                }
            }

            return UtilsMap.of(operation, p);
        }
    }

    private static abstract class OpExprNoList extends Expr {
        private final String operation;
        private final Expr params;

        private OpExprNoList(String type, Expr par) {
            if (!type.startsWith("$")) {
                type = "$" + type;
            }

            operation = type;
            params = par;
        }

        @Override
        public Object toQueryObject() {
            return UtilsMap.of(operation, params.toQueryObject());
        }
    }

    private static class FieldExpr extends Expr {

        private final String fieldRef;

        public FieldExpr(String fieldRef) {
            if (!fieldRef.startsWith("$")) {
                fieldRef = "$" + fieldRef;
            }

            this.fieldRef = fieldRef;
        }

        @Override
        public Object evaluate(Map<String, Object> context) {
            if (context == null) {
                return null;
            }

            String p = fieldRef.substring(1);
            String[] pth = p.split("\\.");

            if (context.containsKey(pth[0])) {
                Object val = context.get(pth[0]);

                for (int i = 1; i < pth.length; i++) {
                    if (val == null) {
                        return null;
                    }

                    if (i < pth.length) {
                        if (val instanceof Map) {
                            val = ((Map) val).get(pth[i]);
                        } else if (val instanceof List) {
                            try {
                                int idx = Integer.parseInt(pth[i]);
                                val = ((List) val).get(idx);
                            } catch (Exception e) {
                                //wrong index
                            }
                        }
                    }
                }

                return val;
            }

            AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(true);
            String n = an.convertCamelCase(fieldRef.substring(1));
            return context.get(n);
        }

        @Override
        public Object toQueryObject() {
            return fieldRef;
        }
    }

    private static class ArrayExpr extends ValueExpr {

        private final List<Expr> arr;

        public ArrayExpr(Expr... vals) {
            this.arr = Arrays.asList(vals);
        }

        @Override
        public Object toQueryObject() {
            return arr.stream().map(Expr::toQueryObject).collect(java.util.stream.Collectors.toList());
        }

        @Override
        public Object evaluate(Map<String, Object> context) {
            return arr.stream().map(e -> e.evaluate(context)).collect(java.util.stream.Collectors.toList());
        }
    }

    /**
     * $function operator for executing JavaScript code in aggregation pipeline
     *
     * Usage: function("function(arg1, arg2) { return arg1 + arg2; }", Arrays.asList(expr1, expr2))
     */
    public static Expr function(String jsFunction, List<Expr> args) {
        return new Expr() {
            @Override
            public Object toQueryObject() {
                Map<String, Object> functionMap = new HashMap<>();
                functionMap.put("body", jsFunction);
                functionMap.put("args", args.stream().map(Expr::toQueryObject).collect(java.util.stream.Collectors.toList()));
                functionMap.put("lang", "js");
                return UtilsMap.of("$function", functionMap);
            }

            @Override
            public Object evaluate(Map<String, Object> context) {
                try {
                    // Use same JavaScript engine pattern as QueryHelper
                    System.setProperty("polyglot.engine.WarnInterpreterOnly", "false");
                    javax.script.ScriptEngineManager mgr = new javax.script.ScriptEngineManager();
                    javax.script.ScriptEngine engine = mgr.getEngineByExtension("js");
                    if (engine == null) {
                        engine = mgr.getEngineByName("js");
                    }
                    if (engine == null) {
                        engine = mgr.getEngineByName("JavaScript");
                    }
                    if (engine == null) {
                        engine = mgr.getEngineByName("Graal.js");
                    }
                    if (engine == null) {
                        // Similar to QueryHelper's $where behavior - return null when JS not available
                        return null;
                    }

                    // Evaluate all arguments
                    List<Object> evaluatedArgs = new ArrayList<>();
                    for (Expr arg : args) {
                        evaluatedArgs.add(arg.evaluate(context));
                    }

                    // Create a wrapper function that calls the user function with evaluated args
                    StringBuilder funcCall = new StringBuilder();
                    funcCall.append("(").append(jsFunction).append(")(");

                    for (int i = 0; i < evaluatedArgs.size(); i++) {
                        if (i > 0) funcCall.append(", ");

                        Object argValue = evaluatedArgs.get(i);
                        if (argValue instanceof String) {
                            funcCall.append("\"").append(argValue.toString().replace("\"", "\\\"")).append("\"");
                        } else if (argValue == null) {
                            funcCall.append("null");
                        } else {
                            funcCall.append(argValue.toString());
                        }
                    }
                    funcCall.append(")");

                    // Execute the JavaScript
                    Object result = engine.eval(funcCall.toString());
                    return result;

                } catch (Exception e) {
                    throw new RuntimeException("$function execution failed: " + e.getMessage(), e);
                }
            }
        };
    }


    /**
     * Convenience method for $function with no arguments
     */
    public static Expr function(String jsFunction) {
        return function(jsFunction, Arrays.asList());
    }

    public static abstract class ValueExpr extends Expr {
        @Override
        public Object evaluate(Map<String, Object> context) {
            return toQueryObject();
        }
    }

}
