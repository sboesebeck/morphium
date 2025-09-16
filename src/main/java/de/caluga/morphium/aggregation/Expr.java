package de.caluga.morphium.aggregation;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.morphium.driver.MorphiumId;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

@SuppressWarnings("unchecked")
public abstract class Expr {

    public abstract Object toQueryObject();

    public abstract Object evaluate(Map<String, Object> context);

    private static Map parseMap(Map<?, ?> o) {
        Map<String, Expr> ret = new HashMap<>();

        for (Map.Entry<?, ?> e : o.entrySet()) {
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
                throw new IllegalArgumentException("no proper operation " + k);
            }

            k = k.replaceAll("\\$", "");

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
                                return (Expr) m.invoke(null, new Expr[] {(Expr) p});
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
                            log.error("Error during parsing of expr",e);
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
                    return ret;
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
                        System.out.println("Cannot evaluate");
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
                return Math.log1p(((Number) eval(e, context)).doubleValue());
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
                return Math.round(((Number) eval(e, context)).doubleValue());
            }
        };
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
                return null;
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
                return null; //does it make sense in this context?
            }
        };
    }

    public static Expr in(Expr elem, Expr array) {
        return new OpExpr("in", Arrays.asList(elem, array)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object v = eval(elem, context);
                boolean found = false;

                //noinspection unchecked
                for (Object o : (List<Object>) eval(array, context)) {
                    if (o instanceof Expr) {
                        o = eval(((Expr) o), context);
                    }

                    if (o.equals(v)) {
                        found = true;
                        break;
                    }
                }

                return found;
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
                return null;
            }
        };
    }

    public static Expr map(Expr inputArray, Expr as, Expr in) {
        return new OpExpr("map", Arrays.asList(inputArray, as, in)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                LoggerFactory.getLogger(Expr.class).error("map not implemented yet,sorry");
                return null;
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

                for (int i = startNum; i < endNum; i = i + stepNum) {
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
                List lst = (List) eval(array, context);
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
                LoggerFactory.getLogger(Expr.class).error("cond not implemented yet,sorry");
                return null;
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
                LoggerFactory.getLogger(Expr.class).error("binarySize not implemented yet,sorry");
                return null;
            }
        };
    }

    public static Expr bsonSize(Expr e) {
        return new OpExprNoList("bsonSize", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                LoggerFactory.getLogger(Expr.class).error("bsonSize not implemented yet,sorry");
                return null;
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
                GregorianCalendar cal = new GregorianCalendar();
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
                GregorianCalendar cal = new GregorianCalendar();
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
        return new MapOpExpr("dateFromParts", UtilsMap.of("isoWeekYear", isoWeekYear, "month", isoWeek));
    }

    public static Expr isoDateFromParts(Expr isoWeekYear, Expr isoWeek, Expr isoDayOfWeek) {
        return new MapOpExpr("dateFromParts", UtilsMap.of("isoWeekYear", isoWeekYear, "month", isoWeek, "day", isoDayOfWeek));
    }

    public static Expr isoDateFromParts(Expr isoWeekYear, Expr isoWeek, Expr isoDayOfWeek, Expr hour) {
        return new MapOpExpr("dateFromParts", UtilsMap.of("isoWeekYear", isoWeekYear, "month", isoWeek, "day", isoDayOfWeek, "hour", hour));
    }

    public static Expr isoDateFromParts(Expr isoWeekYear, Expr isoWeek, Expr isoDayOfWeek, Expr hour, Expr min) {
        return new MapOpExpr("dateFromParts", UtilsMap.of("isoWeekYear", isoWeekYear, "month", isoWeek, "day", isoDayOfWeek, "hour", hour, "minute", min));
    }

    public static Expr isoDateFromParts(Expr isoWeekYear, Expr isoWeek, Expr isoDayOfWeek, Expr hour, Expr min, Expr sec) {
        return new MapOpExpr("dateFromParts", UtilsMap.of("isoWeekYear", isoWeekYear, "month", isoWeek, "day", isoDayOfWeek, "hour", hour, "minute", min, "second", sec));
    }

    public static Expr isoDateFromParts(Expr isoWeekYear, Expr isoWeek, Expr isoDayOfWeek, Expr hour, Expr min, Expr sec, Expr ms) {
        return new MapOpExpr("dateFromParts", UtilsMap.of("isoWeekYear", isoWeekYear, "month", isoWeek, "day", isoDayOfWeek, "hour", hour, "minute", min, "second", sec, "millisecond", ms));
    }

    public static Expr isoDateFromParts(Expr isoWeekYear, Expr isoWeek, Expr isoDayOfWeek, Expr hour, Expr min, Expr sec, Expr ms, Expr timeZone) {
        return new MapOpExpr("dateFromParts",
                UtilsMap.of("isoWeekYear", isoWeekYear, "month", isoWeek, "day", isoDayOfWeek, "hour", hour, "minute", min, "second", sec, "millisecond", ms, "timezone", timeZone));
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
                GregorianCalendar cal = new GregorianCalendar();
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
                GregorianCalendar cal = new GregorianCalendar();
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
                GregorianCalendar cal = new GregorianCalendar();
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

    public static Expr isoWeek(Expr date) {
        return new OpExprNoList("isoWeek", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                GregorianCalendar cal = new GregorianCalendar();
                Object val = eval(date, context);

                if (val instanceof Date) {
                    cal.setTime((Date) val);
                } else if (val instanceof Number) {
                    cal.setTimeInMillis(((Number) val).longValue());
                } else {
                    throw new IllegalArgumentException("second expr got wrong type: " + val.getClass());
                }

                return cal.get(Calendar.WEEK_OF_MONTH);
            }
        };
    }

    public static Expr isoWeekYear(Expr date) {
        return new OpExprNoList("isoWeekYear", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                GregorianCalendar cal = new GregorianCalendar();
                Object val = eval(date, context);

                if (val instanceof Date) {
                    cal.setTime((Date) val);
                } else if (val instanceof Number) {
                    cal.setTimeInMillis(((Number) val).longValue());
                } else {
                    throw new IllegalArgumentException("second expr got wrong type: " + val.getClass());
                }

                return cal.get(Calendar.WEEK_OF_YEAR);
            }
        };
    }

    public static Expr millisecond(Expr date) {
        return new OpExprNoList("millisecond", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                GregorianCalendar cal = new GregorianCalendar();
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
                GregorianCalendar cal = new GregorianCalendar();
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
                GregorianCalendar cal = new GregorianCalendar();
                Object val = eval(date, context);

                if (val instanceof Date) {
                    cal.setTime((Date) val);
                } else if (val instanceof Number) {
                    cal.setTimeInMillis(((Number) val).longValue());
                } else {
                    throw new IllegalArgumentException("second expr got wrong type: " + val.getClass());
                }

                return cal.get(Calendar.MONTH);
            }
        };
    }

    public static Expr second(Expr date) {
        return new OpExprNoList("second", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                GregorianCalendar cal = new GregorianCalendar();
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
                LoggerFactory.getLogger(Expr.class).error("toDate not implemented yet,sorry");
                return null;
            }
        };
    }

    public static Expr week(Expr date) {
        return new OpExprNoList("week", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                GregorianCalendar cal = new GregorianCalendar();
                Object val = eval(date, context);

                if (val instanceof Date) {
                    cal.setTime((Date) val);
                } else if (val instanceof Number) {
                    cal.setTimeInMillis(((Number) val).longValue());
                } else {
                    throw new IllegalArgumentException("second expr got wrong type: " + val.getClass());
                }

                return cal.get(Calendar.WEEK_OF_YEAR);
            }
        };
    }

    public static Expr year(Expr date) {
        return new OpExprNoList("year", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                GregorianCalendar cal = new GregorianCalendar();
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
                    res.putAll((Map<? extends String, ?>) val);
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
                LoggerFactory.getLogger(Expr.class).error("not implemented yet,sorry");
                return null;
            }
        };
    }

    //Set Expression Operators

    public static Expr setEquals(Expr... e) {
        return new OpExpr("setEquals", Arrays.asList(e)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                LoggerFactory.getLogger(Expr.class).error("not implemented yet,sorry");
                return null;
            }
        };
    }

    public static Expr setIntersection(Expr... e) {
        return new OpExpr("setIntersection", Arrays.asList(e)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                LoggerFactory.getLogger(Expr.class).error("not implemented yet,sorry");
                return null;
            }
        };
    }

    public static Expr setIsSubset(Expr e1, Expr e2) {
        return new OpExpr("setIsSubset", Arrays.asList(e1, e2)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                LoggerFactory.getLogger(Expr.class).error("not implemented yet,sorry");
                return null;
            }
        };
    }

    public static Expr setUnion(Expr... e) {
        return new OpExpr("setUnion", Arrays.asList(e)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Set set = new HashSet();

                for (Expr el : e) {
                    //noinspection unchecked
                    set.add(eval(el, context));
                }

                return set;
            }
        };
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
                LoggerFactory.getLogger(Expr.class).error("not implemented yet,sorry");
                return null;
            }
        };
    }

    public static Expr indexOfCP(Expr str, Expr substr, Expr start, Expr end) {
        return new OpExpr("indexOfCP", Arrays.asList(str, substr, start, end)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                LoggerFactory.getLogger(Expr.class).error("not implemented yet,sorry");
                return null;
            }
        };
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
                LoggerFactory.getLogger(Expr.class).error("not implemented yet,sorry");
                return null;
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
                LoggerFactory.getLogger(Expr.class).error("not implemented yet,sorry");
                return null;
            }
        };
    }

    public static Expr strcasecmp(Expr e1, Expr e2) {
        return new OpExpr("strcasecmp", Arrays.asList(e1, e2)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                LoggerFactory.getLogger(Expr.class).error("not implemented yet,sorry");
                return null;
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
                LoggerFactory.getLogger(Expr.class).error("not implemented yet,sorry");
                return null;
            }
        };
    }

    public static Expr substrCP(Expr str, Expr cpIdx, Expr cpCount) {
        return new OpExpr("substrCP", Arrays.asList(str, cpIdx, cpCount)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                LoggerFactory.getLogger(Expr.class).error("not implemented yet,sorry");
                return null;
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

    public static Expr asinh(Expr e) {
        return new OpExprNoList("asinh", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.sinh(((Number) eval(e, context)).doubleValue());
            }
        };
    }

    public static Expr acosh(Expr e) {
        return new OpExprNoList("acosh", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return null;
            }
        };
    }

    public static Expr atanh(Expr e1, Expr e2) {
        return new OpExpr("atanh", Arrays.asList(e1, e2)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                LoggerFactory.getLogger(Expr.class).error("ATANH not implemented, sorry!");
                return 0;
            }
        };
    }

    private static Expr atanh(List lst) {
        //noinspection unchecked
        return new OpExpr("atanh", lst) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                LoggerFactory.getLogger(Expr.class).error("ATANH not implemented, sorry!");
                return 0;
            }
        };
    }

    public static Expr degreesToRadian(Expr e) {
        return new OpExprNoList("degreesToRadian", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.toRadians(((Number) eval(e, context)).doubleValue());
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
                LoggerFactory.getLogger(Expr.class).error("not implemented yet,sorry");
                return Expr.nullExpr();
            }
        };
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
                return null;
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
                return eval(e, context);
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
            public Object evaluate(Map<String, Object> context) {
                return eval(e, context);
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
            public Object evaluate(Map<String, Object> context) {
                return eval(e, context);
            }
        };
    }

    public static Expr push(Expr e) {
        return new OpExprNoList("push", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return null;
            }
        };
    }

    public static Expr stdDevPop(Expr e) {
        return new OpExprNoList("stdDevPop", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return null;
            }
        };
    }

    public static Expr stdDevPop(Expr... e) {
        return new OpExpr("stdDevPop", Arrays.asList(e)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return null;
            }
        };
    }

    public static Expr stdDevSamp(Expr e) {
        return new OpExprNoList("stdDevSamp", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return null;
            }
        };
    }

    public static Expr stdDevSamp(Expr... e) {
        return new OpExpr("stdDevSamp", Arrays.asList(e)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return null;
            }
        };
    }

    public static Expr sum(Expr e) {
        return new OpExprNoList("sum", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return null;
            }
        };
    }

    public static Expr sum(Expr... e) {
        return new OpExpr("sum", Arrays.asList(e)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                LoggerFactory.getLogger(Expr.class).error("not implemented yet,sorry");
                return null;
            }
        };
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
            return Arrays.asList(arr.stream().map(Expr::toQueryObject).toArray());
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
     * Convenience method for $function with single argument
     */
    public static Expr function(String jsFunction, Expr arg) {
        return function(jsFunction, Arrays.asList(arg));
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
