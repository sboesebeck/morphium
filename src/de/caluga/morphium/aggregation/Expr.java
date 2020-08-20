package de.caluga.morphium.aggregation;

import de.caluga.morphium.Utils;

import java.util.*;

public abstract class Expr {

    public static Expr abs(Expr e) {
        return new OpExpr("abs", Arrays.asList(e));
    }

    public static Expr field(Enum field) {
        return field(field.name());
    }

    public static Expr field(String name) {
        return new FieldExpr(name);
    }

    public static Expr date(Date d) {
        return new Expr() {
            @Override
            public Object toQueryObject() {
                return d;
            }
        };
    }

    public static Expr now() {
        return new Expr() {
            @Override
            public Object toQueryObject() {
                return new Date();
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

    public static Expr mapExpr(Map<String, Expr> map) {
        return new Expr() {
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
        return new OpExpr("add", Arrays.asList(fld));
    }

    public static Expr ceil(Expr e) {
        return new OpExprNoList("ceil", e);
    }

    public static Expr divide(Expr divident, Expr divisor) {
        return new OpExpr("divide", Arrays.asList(divident, divisor));
    }

    public static Expr exp(Expr e) {
        return new OpExprNoList("exp", e);
    }

    public static Expr floor(Expr e) {
        return new OpExprNoList("floor", e);
    }

    public static Expr ln(Expr e) {
        return new OpExprNoList("ln", e);
    }

    public static Expr log(Expr num, Expr base) {
        return new OpExpr("log", Arrays.asList(num, base));
    }

    public static Expr log10(Expr e) {
        return new OpExprNoList("log10", e);
    }

    public static Expr mod(Expr divident, Expr divisor) {
        return new OpExpr("mod", Arrays.asList(divident, divisor));
    }

    public static Expr multiply(Expr e1, Expr e2) {
        return new OpExpr("multiply", Arrays.asList(e1, e2));
    }

    public static Expr pow(Expr num, Expr exponent) {
        return new OpExpr("pow", Arrays.asList(num, exponent));
    }

    public static Expr round(Expr e) {
        return new OpExprNoList("round", e);
    }

    public static Expr sqrt(Expr e) {
        return new OpExprNoList("sqrt", e);
    }

    public static Expr subtract(Expr e1, Expr e2) {
        return new OpExpr("substract", Arrays.asList(e1, e2));
    }

    public static Expr trunc(Expr num, Expr place) {
        return new OpExpr("trunc", Arrays.asList(num, place));
    }

    //Array Expression Operators
    public static Expr arrayElemAt(Expr array, Expr index) {
        return new OpExpr("arrayElemAt", Arrays.asList(array, index));
    }

    public static Expr arrayToObject(Expr array) {
        return new OpExpr("arrayToObject", Arrays.asList(array));
    }

    public static Expr concatArrays(Expr... arrays) {
        return new OpExpr("concatArrays", Arrays.asList(arrays));
    }

    public static Expr filter(Expr inputArray, String as, Expr cond) {
        return new MapOpExpr("filter", Utils.getMap("input", inputArray).add("as", new StringExpr(as)).add("cond", cond));
    }

    public static Expr first(Expr e) {
        return new OpExprNoList("first", e);
    }

    public static Expr in(Expr elem, Expr array) {
        return new OpExpr("in", Arrays.asList(elem, array));
    }

    public static Expr indexOfArray(Expr array, Expr search, Expr start, Expr end) {
        return new OpExpr("indexOfArray", Arrays.asList(array, search, start, end));
    }

    public static Expr isArray(Expr array) {
        return new OpExpr("isArray", Arrays.asList(array));
    }

    public static Expr last(Expr e) {
        return new OpExprNoList("last", e);
    }

    public static Expr map(Expr inputArray, Expr as, Expr in) {
        return new OpExpr("map", Arrays.asList(inputArray, as, in));
    }

    public static Expr objectToArray(Expr obj) {
        return new OpExprNoList("objectToArray", obj);
    }

    public static Expr range(Expr start, Expr end, Expr step) {
        return new OpExpr("range", Arrays.asList(start, end, step));
    }

    public static Expr reduce(Expr inputArray, Expr initValue, Expr in) {
        return new MapOpExpr("reduce", Utils.getMap("input", inputArray)
                .add("initialValue", initValue)
                .add("in", in)

        );
    }

    public static Expr reverseArray(Expr array) {
        return new OpExprNoList("reverseArray", array);
    }

    public static Expr size(Expr array) {
        return new OpExprNoList("size", array);
    }

    public static Expr slice(Expr array, Expr pos, Expr n) {
        return new OpExpr("slice", Arrays.asList(array, pos, n));
    }

    public static Expr slice(Expr array, Expr n) {
        return new OpExpr("slice", Arrays.asList(array, n));
    }

    public static Expr zip(List<Expr> inputs, Expr useLongestLength, Expr defaults) {
        return new MapOpExpr("zip", Utils.getMap("inputs", (Expr) new ArrayExpr(inputs.toArray(new Expr[inputs.size()]))).add("useLongestLength", useLongestLength).add("defaults", defaults));
    }


    //Boolean Expression Operators
    public static Expr and(Expr... expressions) {
        return new OpExpr("and", Arrays.asList(expressions));
    }

    public static Expr or(Expr... expressions) {
        return new OpExpr("or", Arrays.asList(expressions));
    }

    public static Expr not(Expr expression) {
        return new OpExprNoList("not", expression);
    }


    //Comparison Expression Operators
    public static Expr cmp(Expr e1, Expr e2) {
        return new OpExpr("cmp", Arrays.asList(e1, e2));
    }

    public static Expr eq(Expr e1, Expr e2) {
        return new OpExpr("eq", Arrays.asList(e1, e2));
    }

    public static Expr ne(Expr e1, Expr e2) {
        return new OpExpr("ne", Arrays.asList(e1, e2));
    }

    public static Expr gt(Expr e1, Expr e2) {
        return new OpExpr("gt", Arrays.asList(e1, e2));
    }

    public static Expr lt(Expr e1, Expr e2) {
        return new OpExpr("lt", Arrays.asList(e1, e2));
    }

    public static Expr gte(Expr e1, Expr e2) {
        return new OpExpr("gte", Arrays.asList(e1, e2));
    }

    public static Expr lte(Expr e1, Expr e2) {
        return new OpExpr("lte", Arrays.asList(e1, e2));
    }


    //Conditional Expression Operators
    public static Expr cond(Expr condition, Expr caseTrue, Expr caseFalse) {
        return new OpExpr("cond", Arrays.asList(condition, caseTrue, caseFalse));
    }

    public static Expr ifNull(Expr toCheck, Expr resultIfNull) {
        return new OpExpr("ifNull", Arrays.asList(toCheck, resultIfNull));
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
        Utils.UtilsMap<String, Expr> branches1 = Utils.getMap("branches", new Expr() {
            @Override
            public Object toQueryObject() {
                return branchList;
            }
        });
        branches1.put("default", defaultCase);
        return new MapOpExpr("switch", branches1);
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


    //Data Size Operators
    public static Expr binarySize(Expr e) {
        return new OpExprNoList("binarySize", e);
    }

    public static Expr bsonSize(Expr e) {
        return new OpExprNoList("bsonSize", e);
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

    public static Expr dayOfMonth(Expr date) {
        return new OpExprNoList("dayOfMonth", date);
    }

    public static Expr dayOfWeek(Expr date) {
        return new OpExprNoList("dayOfWeek", date);
    }

    public static Expr dayOfYear(Expr date) {
        return new OpExprNoList("dayOfYear", date);

    }

    public static Expr hour(Expr date) {
        return new OpExprNoList("hour", date);
    }

    public static Expr isoDayOfWeek(Expr date) {
        return new OpExprNoList("isoDayOfWeek", date);
    }

    public static Expr isoWeek(Expr date) {
        return new OpExprNoList("isoWeek", date);
    }

    public static Expr isoWeekYear(Expr date) {
        return new OpExprNoList("isoWeekYear", date);
    }

    public static Expr millisecond(Expr date) {
        return new OpExprNoList("millisecond", date);
    }

    public static Expr minute(Expr date) {
        return new OpExprNoList("minute", date);
    }

    public static Expr month(Expr date) {
        return new OpExprNoList("month", date);
    }

    public static Expr second(Expr date) {
        return new OpExprNoList("second", date);
    }

    public static Expr toDate(Expr e) {
        return new OpExprNoList("toDate", e);
    }

    public static Expr week(Expr date) {
        return new OpExprNoList("week", date);
    }

    public static Expr year(Expr date) {
        return new OpExprNoList("year", date);
    }


    public static Expr literal(Expr e) {
        return new OpExprNoList("literal", e);
    }


    public static Expr mergeObjects(Expr doc) {
        return new Expr() {
            @Override
            public Object toQueryObject() {
                return Utils.getMap("$mergeObjects", doc.toQueryObject());
            }
        };
    }

    public static Expr mergeObjects(Expr... docs) {
        return new OpExpr("mergeObjects", Arrays.asList(docs));
    }


    //Set Expression Operators

    public static Expr allElementsTrue(Expr... e) {
        return new OpExpr("allElementsTrue", Arrays.asList(e));
    }

    public static Expr anyElementTrue(Expr... e) {
        return new OpExpr("anyElementsTrue", Arrays.asList(e));
    }

    public static Expr setDifference(Expr e1, Expr e2) {
        return new OpExpr("setDifference", Arrays.asList(e1, e2));
    }

    public static Expr setEquals(Expr... e) {
        return new OpExpr("setEquals", Arrays.asList(e));
    }

    public static Expr setIntersection(Expr... e) {
        return new OpExpr("setIntersection", Arrays.asList(e));
    }

    public static Expr setIsSubset(Expr e1, Expr e2) {
        return new OpExpr("setIsSubset", Arrays.asList(e1, e2));
    }

    public static Expr setUnion(Expr... e) {
        return new OpExpr("setUnion", Arrays.asList(e));
    }


    //concat

    public static Expr concat(Expr... e) {
        return new OpExpr("concat", Arrays.asList(e));
    }

    public static Expr indexOfBytes(Expr str, Expr substr, Expr start, Expr end) {
        return new OpExpr("indexOfBytes", Arrays.asList(str, substr, start, end));
    }

    public static Expr indexOfCP(Expr str, Expr substr, Expr start, Expr end) {
        return new OpExpr("indexOfCP", Arrays.asList(str, substr, start, end));
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

    public static Expr split(Expr str, Expr delimiter) {
        return new OpExpr("split", Arrays.asList(str, delimiter));
    }

    public static Expr strLenBytes(Expr str) {
        return new OpExpr("strLenBytes", Arrays.asList(str));
    }

    public static Expr strLenCP(Expr str) {
        return new OpExpr("strLenCP", Arrays.asList(str));
    }

    public static Expr strcasecmp(Expr e1, Expr e2) {
        return new OpExpr("strcasecmp", Arrays.asList(e1, e2));
    }

    public static Expr substr(Expr str, Expr start, Expr len) {
        return new OpExpr("substr", Arrays.asList(str, start, len));
    }

    public static Expr substrBytes(Expr str, Expr index, Expr count) {
        return new OpExpr("substrBytes", Arrays.asList(str, index, count));
    }

    public static Expr substrCP(Expr str, Expr cpIdx, Expr cpCount) {
        return new OpExpr("substrCP", Arrays.asList(str, cpIdx, cpCount));
    }

    public static Expr toLower(Expr e) {
        return new OpExprNoList("toLower", e);
    }

    public static Expr toStr(Expr e) {
        return new OpExprNoList("toString", e);
    }

    public static Expr trim(Expr str, Expr charsToTrim) {
        return new MapOpExpr("trim", Utils.getMap("input", str).add("chars", charsToTrim));
    }

    public static Expr toUpper(Expr e) {
        return new OpExprNoList("toUpper", e);
    }


    public static Expr meta(String metaDataKeyword) {
        return new Expr() {
            @Override
            public Object toQueryObject() {
                return Utils.getMap("$meta", metaDataKeyword);
            }
        };
    }


    //Trigonometry Expression Operators

    public static Expr sin(Expr e) {
        return new OpExprNoList("sin", e);
    }

    public static Expr cos(Expr e) {
        return new OpExprNoList("cos", e);
    }

    public static Expr tan(Expr e) {
        return new OpExprNoList("tan", e);
    }

    public static Expr asin(Expr e) {
        return new OpExprNoList("asin", e);
    }

    public static Expr acos(Expr e) {
        return new OpExprNoList("acos", e);
    }

    public static Expr atan(Expr e) {
        return new OpExprNoList("atan", e);
    }

    public static Expr atan2(Expr e) {
        return new OpExprNoList("atan2", e);
    }

    public static Expr asinh(Expr e) {
        return new OpExprNoList("asinh", e);
    }

    public static Expr acosh(Expr e) {
        return new OpExprNoList("acosh", e);
    }

    public static Expr atanh(Expr e1, Expr e2) {
        return new OpExpr("atanh", Arrays.asList(e1, e2));
    }

    public static Expr degreesToRadian(Expr e) {
        return new OpExprNoList("degreesToRadian", e);
    }

    public static Expr radiansToDegrees(Expr e) {
        return new OpExprNoList("radiansToDegrees", e);
    }


    //Type Expression

    public static Expr convert(Expr input, Expr to) {
        return convert(input, to, null, null);
    }

    public static Expr convert(Expr input, Expr to, Expr onError) {
        return convert(input, to, onError, null);
    }

    public static Expr convert(Expr input, Expr to, Expr onError, Expr onNull) {
        return new MapOpExpr("convert", Utils.getMap("input", input)
                .add("to", to)
                .add("onError", onError)
                .add("onNull", onNull));
    }

    public static Expr isNumber(Expr e) {
        return new OpExprNoList("isNumber", e);
    }

    public static Expr toBool(Expr e) {
        return new OpExprNoList("toBool", e);
    }

    public static Expr toDecimal(Expr e) {
        return new OpExprNoList("toDecimal", e);
    }

    public static Expr toDouble(Expr e) {
        return new OpExprNoList("toDouble", e);
    }

    public static Expr toInt(Expr e) {
        return new OpExprNoList("toInt", e);
    }

    public static Expr toLong(Expr e) {
        return new OpExprNoList("toLong", e);
    }

    public static Expr toObjectId(Expr e) {
        return new OpExprNoList("toObjectId", e);
    }

    public static Expr type(Expr e) {
        return new OpExprNoList("type", e);
    }


    //Group stage

    public static Expr addToSet(Expr e) {
        return new OpExprNoList("addToSet", e);
    }

    public static Expr avg(Expr... e) {
        return new OpExpr("avg", Arrays.asList(e));
    }

    public static Expr avg(Expr e) {
        return new OpExprNoList("avg", e);
    }

    public static Expr max(Expr... e) {
        return new OpExpr("max", Arrays.asList(e));
    }

    public static Expr max(Expr e) {
        return new OpExprNoList("max", e);
    }


    public static Expr min(Expr... e) {
        return new OpExpr("min", Arrays.asList(e));
    }

    public static Expr min(Expr e) {
        return new OpExprNoList("min", e);
    }

    public static Expr push(Expr e) {
        return new OpExprNoList("push", e);
    }

    public static Expr stdDevPop(Expr e) {
        return new OpExprNoList("stdDevPop", e);
    }

    public static Expr stdDevPop(Expr... e) {
        return new OpExpr("stdDevPop", Arrays.asList(e));
    }


    public static Expr stdDevSamp(Expr e) {
        return new OpExprNoList("stdDevSamp", e);
    }

    public static Expr stdDevSamp(Expr... e) {
        return new OpExpr("stdDevSamp", Arrays.asList(e));
    }

    public static Expr sum(Expr e) {
        return new OpExprNoList("sum", e);
    }

    public static Expr sum(Expr... e) {
        return new OpExpr("sum", Arrays.asList(e));
    }


    public static Expr let(Map<String, Expr> vars, Expr in) {
        return new Expr() {
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
        return new Expr() {
            @Override
            public Object toQueryObject() {
                return document;
            }
        };
    }


    public abstract Object toQueryObject();


    private static class MapOpExpr extends Expr {
        private String operation;
        private Map<String, Expr> params;

        private MapOpExpr(String type, Map<String, Expr> par) {
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

    private static class OpExpr extends Expr {
        private String operation;
        private List<Expr> params;

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

    private static class OpExprNoList extends Expr {
        private String operation;
        private Expr params;

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

        private String fieldRef;

        public FieldExpr(String fieldRef) {
            if (!fieldRef.startsWith("$")) {
                fieldRef = "$" + fieldRef;
            }
            this.fieldRef = fieldRef;
        }

        @Override
        public Object toQueryObject() {
            return fieldRef;
        }
    }

    private static class StringExpr extends Expr {

        private String str;

        public StringExpr(String str) {
            this.str = str;
        }

        @Override
        public Object toQueryObject() {
            return str;
        }
    }

    private static class IntExpr extends Expr {

        private Integer number;

        public IntExpr(int str) {
            this.number = str;
        }

        @Override
        public Object toQueryObject() {
            return number;
        }
    }


    private static class DoubleExpr extends Expr {

        private Double number;

        public DoubleExpr(double str) {
            this.number = str;
        }

        @Override
        public Object toQueryObject() {
            return number;
        }
    }


    private static class BoolExpr extends Expr {

        private Boolean bool;

        public BoolExpr(boolean b) {
            this.bool = b;
        }

        @Override
        public Object toQueryObject() {
            return bool;
        }
    }

    private static class ArrayExpr extends Expr {

        private List<Expr> arr;

        public ArrayExpr(Expr... vals) {
            this.arr = Arrays.asList(vals);
        }

        @Override
        public Object toQueryObject() {
            return Arrays.asList(arr.stream().map((x) -> x.toQueryObject()).toArray());
        }
    }
}
