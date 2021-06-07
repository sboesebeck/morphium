package de.caluga.morphium.aggregation;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.MorphiumId;
import org.bson.types.ObjectId;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class Expr {
    public static Expr abs(Expr e) {
        return new OpExpr("abs", Arrays.asList(e)) {
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
        return field(m.getARHelper().getFieldName(type, name.name()));
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

    public static Expr add(Expr... fld) {
        return new OpExpr("add", Arrays.asList(fld)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Number sum = 0;
                for (Expr f : fld) {
                    sum = sum.doubleValue() + ((Number) f.evaluate(context)).doubleValue();
                }
                return null;
            }
        };
    }

    public static Expr doubleExpr(double d) {
        return new DoubleExpr(d);
    }

    public static Expr intExpr(int i) {
        return new IntExpr(i);
    }

    public static Expr bool(boolean v) {
        return new BoolExpr(v);
    }

    public static Expr arrayExpr(Expr... elem) {
        return new ArrayExpr(elem);
    }

    public static Expr string(String str) {
        return new StringExpr(str);
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
                return ((Number) divident.evaluate(context)).doubleValue() / ((Number) divisor.evaluate(context)).doubleValue();
            }
        };
    }

    public static Expr exp(Expr e) {
        return new OpExprNoList("exp", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.exp(((Number) e.evaluate(context)).doubleValue());
            }
        };
    }

    public static Expr floor(Expr e) {
        return new OpExprNoList("floor", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.floor(((Number) e.evaluate(context)).doubleValue());
            }
        };
    }

    public static Expr ln(Expr e) {
        return new OpExprNoList("ln", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.log1p(((Number) e.evaluate(context)).doubleValue());
            }
        };
    }

    public static Expr log(Expr num, Expr base) {
        return new OpExpr("log", Arrays.asList(num, base)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.log(((Number) num.evaluate(context)).doubleValue()) / Math.log(((Number) base.evaluate(context)).doubleValue());
            }
        };
    }

    public static Expr log10(Expr e) {
        return new OpExprNoList("floor", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.log10(((Number) e.evaluate(context)).doubleValue());
            }
        };
    }

    public static Expr mod(Expr divident, Expr divisor) {
        return new OpExpr("mod", Arrays.asList(divident, divisor)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return ((Number) divident.evaluate(context)).doubleValue() % ((Number) divisor.evaluate(context)).doubleValue();
            }
        };
    }

    public static Expr multiply(Expr e1, Expr e2) {
        return new OpExpr("multiply", Arrays.asList(e1, e2)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return ((Number) e1.evaluate(context)).doubleValue() * ((Number) e2.evaluate(context)).doubleValue();
            }
        };
    }

    public static Expr pow(Expr num, Expr exponent) {
        return new OpExpr("pow", Arrays.asList(num, exponent)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.pow(((Number) num.evaluate(context)).doubleValue(), ((Number) exponent.evaluate(context)).doubleValue());
            }
        };
    }

    public static Expr round(Expr e) {
        return new OpExprNoList("round", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.round(((Number) e.evaluate(context)).doubleValue());
            }
        };
    }

    public static Expr sqrt(Expr e) {
        return new OpExprNoList("sqrt", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.sqrt(((Number) e.evaluate(context)).doubleValue());
            }
        };
    }

    public static Expr subtract(Expr e1, Expr e2) {
        return new OpExpr("substract", Arrays.asList(e1, e2)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return ((Number) e1.evaluate(context)).doubleValue() - ((Number) e2.evaluate(context)).doubleValue();
            }
        };
    }

    public static Expr trunc(Expr num, Expr place) {
        return new OpExpr("trunc", Arrays.asList(num, place)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                double n = ((Number) num.evaluate(context)).doubleValue();
                double m = 1;
                if (place.evaluate(context) != null || !place.evaluate(context).equals(0)) {
                    m = Math.pow(10, ((Number) place.evaluate(context)).intValue());
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
                return ((List) array.evaluate(context)).get(((Number) index.evaluate(context)).intValue());
            }
        };
    }

    public static Expr arrayToObject(Expr array) {
        return new OpExpr("arrayToObject", Arrays.asList(array)) {
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
                    ret.addAll(((List) e.evaluate(context)));
                }
                return ret;
            }
        };
    }

    public static Expr filter(Expr inputArray, String as, Expr cond) {
        return new MapOpExpr("filter", Utils.getMap("input", inputArray).add("as", new StringExpr(as)).add("cond", cond)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                List<Object> ret = new ArrayList<>();
                for (Object el : (List) inputArray.evaluate(context)) {
                    if (el instanceof Expr) {
                        el = ((Expr) el).evaluate(context);
                    }
                    Map<String, Object> ctx = new ObjectMapperImpl().serialize(el);
                    String fld = as;
                    if (fld == null) {
                        fld = "this";
                    }
                    context.put(fld, el);
                    if ((cond.evaluate(context)).equals(Boolean.TRUE)) {
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
                Object v = elem.evaluate(context);

                boolean found = false;
                for (Object o : (List<Object>) array.evaluate(context)) {
                    if (o instanceof Expr) {
                        o = ((Expr) o).evaluate(context);
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
                int startNum = ((Number) start.evaluate(context)).intValue();
                int endNum = ((Number) end.evaluate(context)).intValue();
                List lst = (List) array.evaluate(context);
                Object o = search.evaluate(context);
                return lst.indexOf(o);
            }
        };
    }

    public static Expr isArray(Expr array) {
        return new OpExpr("isArray", Arrays.asList(array)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return array.evaluate(context) instanceof List;
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
                for (Map.Entry e : ((Map<String, Object>) obj.evaluate(context)).entrySet()) {
                    ret.add(e.getKey());
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
                int startNum = ((Number) start.evaluate(context)).intValue();
                int endNum = ((Number) end.evaluate(context)).intValue();
                int stepNum = ((Number) step.evaluate(context)).intValue();
                List<Number> lst = new ArrayList<>();
                if (endNum < startNum) {
                    if (stepNum > 0) stepNum = -stepNum;
                } else if (endNum > startNum) {
                    if (stepNum < 0) stepNum = -stepNum;
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
                List lst = (List) array.evaluate(context);
                Collections.reverse(lst);
                return lst;
            }
        };
    }

    public static Expr size(Expr array) {
        return new OpExprNoList("size", array) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return ((List) array.evaluate(context)).size();
            }
        };
    }

    public static Expr reduce(Expr inputArray, Expr initValue, Expr in) {
        return new MapOpExpr("reduce", Utils.getMap("input", inputArray)
                .add("initialValue", initValue)
                .add("in", in)

        );
    }

    public static Expr slice(Expr array, Expr pos, Expr n) {
        return new OpExpr("slice", Arrays.asList(array, pos, n)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                List lst = (List) array.evaluate(context);
                int posN = ((Number) pos.evaluate(context)).intValue();
                int len = ((Number) n.evaluate(context)).intValue();
                return lst.subList(posN, posN + len);
            }
        };
    }

    public static Expr slice(Expr array, Expr n) {
        return new OpExpr("slice", Arrays.asList(array, n)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                List lst = (List) array.evaluate(context);
                int len = ((Number) n.evaluate(context)).intValue();
                return lst.subList(0, len);
            }
        };
    }

    //Boolean Expression Operators
    public static Expr and(Expr... expressions) {
        return new OpExpr("and", Arrays.asList(expressions)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                boolean result = true;

                int idx = 0;
                while (result && idx < expressions.length) {
                    result = result && ((Boolean) expressions[idx].evaluate(context));
                    idx++;
                }
                return result;
            }
        };
    }

    public static Expr or(Expr... expressions) {
        return new OpExpr("or", Arrays.asList(expressions)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                boolean result = false;

                int idx = 0;
                while (!result && idx < expressions.length) {
                    result = result || ((Boolean) expressions[idx].evaluate(context));
                    idx++;
                }
                return result;
            }
        };
    }

    public static Expr zip(List<Expr> inputs, Expr useLongestLength, Expr defaults) {
        return new MapOpExpr("zip", Utils.getMap("inputs", (Expr) new ArrayExpr(inputs.toArray(new Expr[0]))).add("useLongestLength", useLongestLength).add("defaults", defaults));
    }

    public static Expr not(Expr expression) {
        return new OpExprNoList("not", expression) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return !((Boolean) expression.evaluate(context));
            }
        };
    }

    //Comparison Expression Operators
    public static Expr cmp(Expr e1, Expr e2) {
        return new OpExpr("cmp", Arrays.asList(e1, e2)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return ((Comparable) e1.evaluate(context)).compareTo(e1.evaluate(context));
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
                return e1.evaluate(context).equals(e2.evaluate(context));
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
                return !e1.evaluate(context).equals(e2.evaluate(context));
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
                return ((Comparable) e1.evaluate(context)).compareTo(e2.evaluate(context)) > 0;
            }
        };
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
                return ((Comparable) e1.evaluate(context)).compareTo(e2.evaluate(context)) < 0;
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
                return ((Comparable) e1.evaluate(context)).compareTo(e2.evaluate(context)) >= 0;
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
                return ((Comparable) e1.evaluate(context)).compareTo(e2.evaluate(context)) <= 0;
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
                Object result = toCheck.evaluate(context);
                if (result == null) {
                    return (resultIfNull.evaluate(context));
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
            branchList.add(Utils.getMap("case", ex.getKey().toQueryObject()).add("then", ex.getValue().toQueryObject()));
        }
        Utils.UtilsMap<String, Expr> branches1 = Utils.getMap("branches", new ValueExpr() {
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
        if (lang == null) lang = "js";
        return new MapOpExpr("function", Utils.getMap("body", string(code)).add("args", args).add("lang", string(lang)));
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
        if (lang == null) lang = "js";
        Utils.UtilsMap<String, Expr> map = Utils.getMap("init", string(initCode));
        map.add("initArgs", initArgs)
                .add("accumulate", string(accumulateCode))
                .add("accumulateArgs", accArgs)
                .add("merge", string(mergeCode))
                .add("finalize", string(finalizeCode))
                .add("lang", string(lang));


        return new MapOpExpr("accumulator", map);
    }

    public static Expr dayOfMonth(Expr date) {
        return new OpExprNoList("dayOfMonth", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                GregorianCalendar cal = new GregorianCalendar();
                cal.setTime((Date) date.evaluate(context));
                return cal.get(Calendar.DAY_OF_MONTH);
            }
        };
    }

    public static Expr dayOfWeek(Expr date) {
        return new OpExprNoList("dayOfWeek", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                GregorianCalendar cal = new GregorianCalendar();
                cal.setTime((Date) date.evaluate(context));
                return cal.get(Calendar.DAY_OF_WEEK);
            }
        };
    }


    //Date Expression Operators

    public static Expr dateFromParts(Expr year) {
        return new MapOpExpr("dateFromParts", Utils.getMap("year", year)
        );
    }

    public static Expr dateFromParts(Expr year, Expr month) {
        return new MapOpExpr("dateFromParts", Utils.getMap("year", year)
                .add("month", month)
        );
    }

    public static Expr dateFromParts(Expr year, Expr month, Expr day, Expr hour) {
        return new MapOpExpr("dateFromParts", Utils.getMap("year", year)
                .add("month", month)
                .add("day", day)
                .add("hour", hour)
        );
    }

    public static Expr dateFromParts(Expr year, Expr month, Expr day, Expr hour, Expr min, Expr sec) {
        return new MapOpExpr("dateFromParts", Utils.getMap("year", year)
                .add("month", month)
                .add("day", day)
                .add("hour", hour)
                .add("minute", min)
                .add("second", sec)

        );
    }

    public static Expr dateFromParts(Expr year, Expr month, Expr day, Expr hour, Expr min, Expr sec, Expr ms) {
        return new MapOpExpr("dateFromParts", Utils.getMap("year", year)
                .add("month", month)
                .add("day", day)
                .add("hour", hour)
                .add("minute", min)
                .add("second", sec)
                .add("millisecond", ms)
        );
    }

    public static Expr dateFromParts(Expr year, Expr month, Expr day, Expr hour, Expr min, Expr sec, Expr ms, Expr timeZone) {
        return new MapOpExpr("dateFromParts", Utils.getMap("year", year)
                .add("month", month)
                .add("day", day)
                .add("hour", hour)
                .add("minute", min)
                .add("second", sec)
                .add("millisecond", ms)
                .add("timezone", timeZone)
        );
    }


    public static Expr isoDateFromParts(Expr isoWeekYear) {
        return new MapOpExpr("dateFromParts", Utils.getMap("isoWeekYear", isoWeekYear)
        );
    }

    public static Expr isoDateFromParts(Expr isoWeekYear, Expr isoWeek) {
        return new MapOpExpr("dateFromParts", Utils.getMap("isoWeekYear", isoWeekYear)
                .add("month", isoWeek)
        );
    }

    public static Expr isoDateFromParts(Expr isoWeekYear, Expr isoWeek, Expr isoDayOfWeek) {
        return new MapOpExpr("dateFromParts", Utils.getMap("isoWeekYear", isoWeekYear)
                .add("month", isoWeek)
                .add("day", isoDayOfWeek)
        );
    }

    public static Expr isoDateFromParts(Expr isoWeekYear, Expr isoWeek, Expr isoDayOfWeek, Expr hour) {
        return new MapOpExpr("dateFromParts", Utils.getMap("isoWeekYear", isoWeekYear)
                .add("month", isoWeek)
                .add("day", isoDayOfWeek)
                .add("hour", hour)
        );
    }

    public static Expr isoDateFromParts(Expr isoWeekYear, Expr isoWeek, Expr isoDayOfWeek, Expr hour, Expr min) {
        return new MapOpExpr("dateFromParts", Utils.getMap("isoWeekYear", isoWeekYear)
                .add("month", isoWeek)
                .add("day", isoDayOfWeek)
                .add("hour", hour)
                .add("minute", min)
        );
    }

    public static Expr isoDateFromParts(Expr isoWeekYear, Expr isoWeek, Expr isoDayOfWeek, Expr hour, Expr min, Expr sec) {
        return new MapOpExpr("dateFromParts", Utils.getMap("isoWeekYear", isoWeekYear)
                .add("month", isoWeek)
                .add("day", isoDayOfWeek)
                .add("hour", hour)
                .add("minute", min)
                .add("second", sec)
        );
    }

    public static Expr isoDateFromParts(Expr isoWeekYear, Expr isoWeek, Expr isoDayOfWeek, Expr hour, Expr min, Expr sec, Expr ms) {
        return new MapOpExpr("dateFromParts", Utils.getMap("isoWeekYear", isoWeekYear)
                .add("month", isoWeek)
                .add("day", isoDayOfWeek)
                .add("hour", hour)
                .add("minute", min)
                .add("second", sec)
                .add("millisecond", ms)
        );
    }

    public static Expr isoDateFromParts(Expr isoWeekYear, Expr isoWeek, Expr isoDayOfWeek, Expr hour, Expr min, Expr sec, Expr ms, Expr timeZone) {
        return new MapOpExpr("dateFromParts", Utils.getMap("isoWeekYear", isoWeekYear)
                .add("month", isoWeek)
                .add("day", isoDayOfWeek)
                .add("hour", hour)
                .add("minute", min)
                .add("second", sec)
                .add("millisecond", ms)
                .add("timezone", timeZone)
        );
    }

    public static Expr dateFromString(Expr dateString, Expr format, Expr timezone, Expr onError, Expr onNull) {
        return new MapOpExpr("dateFromString", Utils.getMap("dateString", dateString)
                .add("format", format)
                .add("timezone", timezone)
                .add("onError", onError)
                .add("onNull", onNull)
        );
    }

    public static Expr dateToParts(Expr date, Expr timezone, boolean iso8601) {
        return new MapOpExpr("dateToParts", Utils.getMap("date", date)
                .add("timezone", timezone)
                .add("iso8601", bool(iso8601))
        );
    }

    public static Expr dateToString(Expr date, Expr format, Expr timezone, Expr onNull) {
        return new MapOpExpr("dateToString", Utils.getMap("dateString", date)
                .add("format", format)
                .add("timezone", timezone)
                .add("onNull", onNull)
        );
    }

    public static Expr dayOfYear(Expr date) {
        return new OpExprNoList("dayOfYear", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                GregorianCalendar cal = new GregorianCalendar();
                cal.setTime((Date) date.evaluate(context));
                return cal.get(Calendar.DAY_OF_YEAR);
            }
        };

    }

    public static Expr hour(Expr date) {
        return new OpExprNoList("hour", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                GregorianCalendar cal = new GregorianCalendar();
                cal.setTime((Date) date.evaluate(context));
                return cal.get(Calendar.HOUR_OF_DAY);
            }
        };
    }

    public static Expr isoDayOfWeek(Expr date) {
        return new OpExprNoList("isoDayOfWeek", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                GregorianCalendar cal = new GregorianCalendar();
                cal.setTime((Date) date.evaluate(context));
                return cal.get(Calendar.DAY_OF_WEEK);
            }
        };
    }

    public static Expr isoWeek(Expr date) {
        return new OpExprNoList("isoWeek", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                GregorianCalendar cal = new GregorianCalendar();
                cal.setTime((Date) date.evaluate(context));
                return cal.get(Calendar.WEEK_OF_MONTH);
            }
        };
    }

    public static Expr isoWeekYear(Expr date) {
        return new OpExprNoList("isoWeekYear", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                GregorianCalendar cal = new GregorianCalendar();
                cal.setTime((Date) date.evaluate(context));
                return cal.get(Calendar.WEEK_OF_YEAR);
            }
        };
    }

    public static Expr millisecond(Expr date) {
        return new OpExprNoList("millisecond", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                GregorianCalendar cal = new GregorianCalendar();
                cal.setTime((Date) date.evaluate(context));
                return cal.get(Calendar.MILLISECOND);
            }
        };
    }

    public static Expr minute(Expr date) {
        return new OpExprNoList("minute", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                GregorianCalendar cal = new GregorianCalendar();
                cal.setTime((Date) date.evaluate(context));
                return cal.get(Calendar.MINUTE);
            }
        };
    }

    public static Expr month(Expr date) {
        return new OpExprNoList("month", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                GregorianCalendar cal = new GregorianCalendar();
                cal.setTime((Date) date.evaluate(context));
                return cal.get(Calendar.MONTH);
            }
        };
    }

    public static Expr second(Expr date) {
        return new OpExprNoList("second", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                GregorianCalendar cal = new GregorianCalendar();
                cal.setTime((Date) date.evaluate(context));
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
                cal.setTime((Date) date.evaluate(context));
                return cal.get(Calendar.WEEK_OF_YEAR);
            }
        };
    }

    public static Expr year(Expr date) {
        return new OpExprNoList("year", date) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                GregorianCalendar cal = new GregorianCalendar();
                cal.setTime((Date) date.evaluate(context));
                return cal.get(Calendar.YEAR);
            }
        };
    }

    public static Expr literal(Expr e) {
        return new OpExprNoList("literal", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return e.evaluate(context);
            }
        };
    }

    public static Expr mergeObjects(Expr doc) {
        return new ValueExpr() {
            @Override
            public Object toQueryObject() {
                return Utils.getMap("$mergeObjects", doc.toQueryObject());
            }
        };
    }

    public static Expr mergeObjects(Expr... docs) {
        return new OpExpr("mergeObjects", Arrays.asList(docs)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                LoggerFactory.getLogger(Expr.class).error("mergeObjects not implemented yet,sorry");
                return null;
            }
        };
    }

    public static Expr allElementsTrue(Expr... e) {
        return new OpExpr("allElementsTru", Arrays.asList(e)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                boolean ret = true;
                for (Expr el : e) {
                    if (!Boolean.TRUE.equals(el.evaluate(context))) {
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
                    if (Boolean.TRUE.equals(el.evaluate(context))) {
                        ret = true;
                        break;
                    }
                }
                return ret;
            }
        };
    }


    //Set Expression Operators

    public static Expr setDifference(Expr e1, Expr e2) {
        return new OpExpr("setDifference", Arrays.asList(e1, e2)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                LoggerFactory.getLogger(Expr.class).error("not implemented yet,sorry");
                return null;
            }
        };
    }

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
                    set.add(el.evaluate(context));
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
                    Object evaluate = ex.evaluate(context);
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


    //concat

    public static Expr indexOfCP(Expr str, Expr substr, Expr start, Expr end) {
        return new OpExpr("indexOfCP", Arrays.asList(str, substr, start, end)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                LoggerFactory.getLogger(Expr.class).error("not implemented yet,sorry");
                return null;
            }
        };
    }

    public static Expr match(Expr expr) {
        return new OpExprNoList("$match", expr) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return expr.evaluate(context);
            }
        };
    }

    public static Expr expr(Expr expr) {
        return new OpExprNoList("expr", expr) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                LoggerFactory.getLogger(Expr.class).error("not implemented yet,sorry");
                return expr.evaluate(context);
            }
        };
    }

    public static Expr ltrim(Expr str, Expr charsToTrim) {
        return new MapOpExpr("ltrim", Utils.getMap("input", str).add("chars", charsToTrim));
    }

    public static Expr regexFind(Expr input, Expr regex, Expr options) {
        return new MapOpExpr("regexFind", Utils.getMap("input", input)
                .add("regex", regex)
                .add("options", options)
        );
    }

    public static Expr regexFindAll(Expr input, Expr regex, Expr options) {
        return new MapOpExpr("regexFindAll", Utils.getMap("input", input)
                .add("regex", regex)
                .add("options", options)
        );
    }

    public static Expr project(Map<String, Expr> expr) {
        return new MapOpExpr("$project", expr);
    }

    public static Expr split(Expr str, Expr delimiter) {
        return new OpExpr("split", Arrays.asList(str, delimiter)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                String s = str.evaluate(context).toString();
                return Arrays.asList(s.split(delimiter.evaluate(context).toString()));
            }
        };
    }

    public static Expr strLenBytes(Expr str) {
        return new OpExpr("strLenBytes", Arrays.asList(str)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                LoggerFactory.getLogger(Expr.class).error("not implemented yet,sorry");
                return null;
            }
        };
    }

    public static Expr regexMatch(Expr input, Expr regex, Expr options) {
        return new MapOpExpr("regexMatch", Utils.getMap("input", input)
                .add("regex", regex)
                .add("options", options)
        );
    }

    public static Expr replaceOne(Expr input, Expr find, Expr replacement) {
        return new MapOpExpr("replaceOne", Utils.getMap("input", input)
                .add("find", find)
                .add("replacement", replacement)
        );
    }

    public static Expr replaceAll(Expr input, Expr find, Expr replacement) {
        return new MapOpExpr("replaceAll", Utils.getMap("input", input)
                .add("find", find)
                .add("replacement", replacement));
    }

    public static Expr rtrim(Expr str, Expr charsToTrim) {
        return new MapOpExpr("rtrim", Utils.getMap("input", str).add("chars", charsToTrim));
    }

    public static Expr strLenCP(Expr str) {
        return new OpExpr("strLenCP", Arrays.asList(str)) {
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
                String s = (String) str.evaluate(context);
                int st = ((Number) start.evaluate(context)).intValue();
                int l = ((Number) len.evaluate(context)).intValue();
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
                if (e.evaluate(context) == null) return null;
                return ((String) e.evaluate(context)).toLowerCase(Locale.ROOT);
            }
        };
    }

    public static Expr toStr(Expr e) {
        return new OpExprNoList("toString", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return (e.evaluate(context) != null) ? e.evaluate(context).toString() : null;
            }
        };
    }

    public static Expr toUpper(Expr e) {
        return new OpExprNoList("toUpper", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                if (e.evaluate(context) == null) return null;
                return e.evaluate(context).toString().toUpperCase();
            }
        };
    }

    public static Expr meta(String metaDataKeyword) {
        return new ValueExpr() {
            @Override
            public Object toQueryObject() {
                return Utils.getMap("$meta", metaDataKeyword);
            }
        };
    }

    public static Expr trim(Expr str, Expr charsToTrim) {
        return new MapOpExpr("trim", Utils.getMap("input", str).add("chars", charsToTrim));
    }

    public static Expr sin(Expr e) {
        return new OpExprNoList("sin", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.sin(((Number) e.evaluate(context)).doubleValue());
            }
        };
    }

    public static Expr cos(Expr e) {
        return new OpExprNoList("cos", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.cos(((Number) e.evaluate(context)).doubleValue());
            }
        };
    }


    //Trigonometry Expression Operators

    public static Expr tan(Expr e) {
        return new OpExprNoList("tan", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.tan(((Number) e.evaluate(context)).doubleValue());
            }
        };
    }

    public static Expr asin(Expr e) {
        return new OpExprNoList("asin", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.asin(((Number) e.evaluate(context)).doubleValue());
            }
        };
    }

    public static Expr acos(Expr e) {
        return new OpExprNoList("acos", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.acos(((Number) e.evaluate(context)).doubleValue());
            }
        };
    }

    public static Expr atan(Expr e) {
        return new OpExprNoList("atan", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.atan(((Number) e.evaluate(context)).doubleValue());
            }
        };
    }

    public static Expr atan2(Expr e, Expr e2) {
        return new OpExpr("atan2", Arrays.asList(e, e2)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.atan2(((Number) e.evaluate(context)).doubleValue(), ((Number) e2.evaluate(context)).doubleValue());
            }
        };
    }

    public static Expr asinh(Expr e) {
        return new OpExprNoList("asinh", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.sinh(((Number) e.evaluate(context)).doubleValue());
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
                return null;
            }
        };
    }

    public static Expr degreesToRadian(Expr e) {
        return new OpExprNoList("degreesToRadian", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.toRadians(((Number) e.evaluate(context)).doubleValue());
            }
        };
    }

    public static Expr radiansToDegrees(Expr e) {
        return new OpExprNoList("radiansToDegrees", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Math.toDegrees(((Number) e.evaluate(context)).doubleValue());
            }
        };
    }

    public static Expr convert(Expr input, Expr to, Expr onError, Expr onNull) {
        return new MapOpExpr("convert", Utils.getMap("input", input)
                .add("to", to)
                .add("onError", onError)
                .add("onNull", onNull)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                Object in = input.evaluate(context);
                if (in == null) {
                    return onNull.evaluate(context);
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
                return e.evaluate(context) instanceof Number;
            }
        };
    }


    //Type Expression

    public static Expr convert(Expr input, Expr to) {
        return convert(input, to, null, null);
    }

    public static Expr convert(Expr input, Expr to, Expr onError) {
        return convert(input, to, onError, null);
    }

    public static Expr toBool(Expr e) {
        return new OpExprNoList("toBool", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return Boolean.valueOf(e.evaluate(context).toString());
            }
        };
    }

    public static Expr toDecimal(Expr e) {
        return new OpExprNoList("toDecimal", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return (Double.valueOf(e.evaluate(context).toString()));
            }
        };
    }

    public static Expr toDouble(Expr e) {
        return new OpExprNoList("toDouble", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                //LoggerFactory.getLogger(Expr.class).error("not implemented yet,sorry");
                Number n = (Number) e.evaluate(context);
                return n.doubleValue();
            }
        };
    }

    public static Expr toInt(Expr e) {
        return new OpExprNoList("toInt", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return ((Number) e.evaluate(context)).intValue();
            }
        };
    }

    public static Expr toLong(Expr e) {
        return new OpExprNoList("toLong", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return ((Number) e.evaluate(context)).longValue();
            }
        };
    }

    public static Expr toObjectId(Expr e) {
        return new OpExprNoList("toObjectId", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                //LoggerFactory.getLogger(Expr.class).error("not implemented yet,sorry");
                Object o = e.evaluate(context);
                if (o instanceof String) {
                    o = new MorphiumId((String) o);
                } else if (o instanceof ObjectId) {
                    o = new MorphiumId(((ObjectId) o).toHexString());
                }
                return e.evaluate(context);
            }
        };
    }

    public static Expr type(Expr e) {
        return new OpExprNoList("type", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                LoggerFactory.getLogger(Expr.class).error("not implemented yet,sorry");
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
                return null;
            }
        };
    }


    //Group stage

    public static Expr avg(Expr e) {
        return new OpExprNoList("avg", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return null;
            }
        };
    }

    public static Expr max(Expr... e) {
        return new OpExpr("max", Arrays.asList(e)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return null;
            }
        };
    }

    public static Expr max(Expr e) {
        return new OpExprNoList("max", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return null;
            }
        };
    }

    public static Expr min(Expr... e) {
        return new OpExpr("min", Arrays.asList(e)) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return null;
            }
        };
    }

    public static Expr min(Expr e) {
        return new OpExprNoList("min", e) {
            @Override
            public Object evaluate(Map<String, Object> context) {
                return null;
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
                return Utils.getMap("$let", Utils.getMap("vars", (Object) map).add("in", in.toQueryObject()));
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

    public abstract Object toQueryObject();

    public abstract Object evaluate(Map<String, Object> context);

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
            Map ret = Utils.getMap(operation, m);
            for (Map.Entry<String, Expr> e : params.entrySet()) {
                m.put(e.getKey(), e.getValue().toQueryObject());
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
                if (e != null)
                    p.add(e.toQueryObject());
            }
            return Utils.getMap(operation, p);
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
            return Utils.getMap(operation, params.toQueryObject());
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
            if (context.containsKey(fieldRef.substring(1))) {
                return context.get(fieldRef.substring(1));
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

    private static class StringExpr extends ValueExpr {

        private final String str;

        public StringExpr(String str) {
            this.str = str;
        }

        @Override
        public Object toQueryObject() {
            return str;
        }
    }

    private static class IntExpr extends ValueExpr {

        private final Integer number;

        public IntExpr(int str) {
            this.number = str;
        }

        @Override
        public Object toQueryObject() {
            return number;
        }
    }


    private static class DoubleExpr extends ValueExpr {

        private final Double number;

        public DoubleExpr(double str) {
            this.number = str;
        }

        @Override
        public Object toQueryObject() {
            return number;
        }
    }


    private static class BoolExpr extends ValueExpr {

        private final Boolean bool;

        public BoolExpr(boolean b) {
            this.bool = b;
        }

        @Override
        public Object toQueryObject() {
            return bool;
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

    private static abstract class ValueExpr extends Expr {
        @Override
        public Object evaluate(Map<String, Object> context) {
            return toQueryObject();
        }
    }
}
