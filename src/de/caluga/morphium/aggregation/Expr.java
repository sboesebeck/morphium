package de.caluga.morphium.aggregation;

import de.caluga.morphium.Utils;
import sun.awt.image.ImageWatched;

import java.util.*;

public abstract class Expr {

    public static Expr abs(Expr e) {
        return new OpExpr("abs", Arrays.asList(e));
    }

    public static Expr field(String name) {
        return new FieldExpr(name);
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

    public static <T> Expr arrayExpr(Expr... elem) {
        return new ArrayExpr(elem);
    }

    public static Expr string(String str) {
        return new StringExpr(str);
    }

    public static Expr add(Expr... fld) {
        return new OpExpr("add", Arrays.asList(fld));
    }

    public static Expr ceil(Expr e) {
        return new OpExpr("ceil", Arrays.asList(e));
    }

    public static Expr divide(Expr divident, Expr divisor) {
        return new OpExpr("divide", Arrays.asList(divident, divisor));
    }

    public static Expr exp(Expr e) {
        return new OpExpr("exp", Arrays.asList(e));
    }

    public static Expr floor(Expr e) {
        return new OpExpr("floor", Arrays.asList(e));
    }

    public static Expr ln(Expr e) {
        return new OpExpr("ln", Arrays.asList(e));
    }

    public static Expr log(Expr num, Expr base) {
        return new OpExpr("log", Arrays.asList(num, base));
    }

    public static Expr log10(Expr e) {
        return new OpExpr("log10", Arrays.asList(e));
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
        return new OpExpr("round", Arrays.asList(e));
    }

    public static Expr sqrt(Expr e) {
        return new OpExpr("sqrt", Arrays.asList(e));
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
        return new OpExpr("log", Arrays.asList(arrays));
    }

    public static Expr filter(Expr inputArray, String as, Expr cond) {
        return new MapOpExpr("filter", Utils.getMap("input", inputArray).add("as", new StringExpr(as)).add("cond", cond));
    }

    public static Expr first(Expr e) {
        return new OpExpr("first", Arrays.asList(e));
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
        return new OpExpr("first", Arrays.asList(e));
    }

    public static Expr map(Expr inputArray, Expr as, Expr in) {
        return new OpExpr("map", Arrays.asList(inputArray, as, in));
    }

    public static Expr objectToArray(Expr obj) {
        return new OpExpr("objectToArray", Arrays.asList(obj));
    }

    public static Expr range(Expr start, Expr end, Expr step) {
        return new OpExpr("range", Arrays.asList(start, end, step));
    }

    public static Expr reduce(Expr inputArray, Expr initValue, Expr in) {
        return new OpExpr("reduce", Arrays.asList(inputArray, initValue, in));
    }

    public static Expr reverseArray(Expr array) {
        return new OpExpr("reverseArray", Arrays.asList(array));
    }

    public static Expr size(Expr array) {
        return new OpExpr("size", Arrays.asList(array));
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
    public static Expr and() {
        return null;
    }

    public static Expr or() {
        return null;
    }

    public static Expr not() {
        return null;
    }


    //Comparison Expression Operators
    public static Expr cmp() {
        return null;
    }

    public static Expr eq() {
        return null;
    }

    public static Expr ne() {
        return null;
    }

    public static Expr gt() {
        return null;
    }

    public static Expr lt() {
        return null;
    }

    public static Expr gte() {
        return null;
    }

    public static Expr lte() {
        return null;
    }


    //Conditional Expression Operators
    public static Expr cond() {
        return null;
    }

    public static Expr ifNull() {
        return null;
    }

    public static Expr switchExpr() {
        return null;
    }


    //Custom Aggregation
    public static Expr function() {
        return null;
    }

    public static Expr accumulator() {
        return null;
    }


    //Data Size Operators
    public static Expr binarySize() {
        return null;
    }

    public static Expr bsonSize() {
        return null;
    }


    //Date Expression Operators

    public static Expr dateFromParts() {
        return null;
    }

    public static Expr dateFromString() {
        return null;
    }

    public static Expr dateToParts() {
        return null;
    }

    public static Expr dateToString() {
        return null;
    }

    public static Expr dayOfMonth() {
        return null;
    }

    public static Expr dayOfWeek() {
        return null;
    }

    public static Expr dayOfYear() {
        return null;
    }

    public static Expr hour() {
        return null;
    }

    public static Expr isoDayOfWeek() {
        return null;
    }

    public static Expr isoWeek() {
        return null;
    }

    public static Expr isoWeekYear() {
        return null;
    }

    public static Expr millisecond() {
        return null;
    }

    public static Expr minute() {
        return null;
    }

    public static Expr month() {
        return null;
    }

    public static Expr second() {
        return null;
    }

    public static Expr toDate() {
        return null;
    }

    public static Expr week() {
        return null;
    }

    public static Expr year() {
        return null;
    }


    public static Expr literal() {
        return null;
    }


    public static Expr mergeObjects() {
        return null;
    }


    //Set Expression Operators

    public static Expr allElementsTrue() {
        return null;
    }

    public static Expr anyElementTrue() {
        return null;
    }

    public static Expr setDifference() {
        return null;
    }

    public static Expr setEquals() {
        return null;
    }

    public static Expr setIntersection() {
        return null;
    }

    public static Expr setIsSubset() {
        return null;
    }

    public static Expr setUnion() {
        return null;
    }


    //concat

    public static Expr concat() {
        return null;
    }

    public static Expr dindexOfBytes() {
        return null;
    }

    public static Expr indexOfCP() {
        return null;
    }

    public static Expr ltrim() {
        return null;
    }

    public static Expr regexFind() {
        return null;
    }

    public static Expr regexFindAll() {
        return null;
    }

    public static Expr regexMatch() {
        return null;
    }

    public static Expr replaceOne() {
        return null;
    }

    public static Expr replaceAll() {
        return null;
    }

    public static Expr rtrim() {
        return null;
    }

    public static Expr split() {
        return null;
    }

    public static Expr strLenBytes() {
        return null;
    }

    public static Expr strLenCP() {
        return null;
    }

    public static Expr strcasecmp() {
        return null;
    }

    public static Expr substr() {
        return null;
    }

    public static Expr substrBytes() {
        return null;
    }

    public static Expr substrCP() {
        return null;
    }

    public static Expr toLower() {
        return null;
    }

    public static Expr toStr() {
        return null;
    }

    public static Expr trim() {
        return null;
    }

    public static Expr toUpper() {
        return null;
    }


    public static Expr meta() {
        return null;
    }


    //Trigonometry Expression Operators

    public static Expr sin() {
        return null;
    }

    public static Expr cos() {
        return null;
    }

    public static Expr tan() {
        return null;
    }

    public static Expr asin() {
        return null;
    }

    public static Expr acos() {
        return null;
    }

    public static Expr atan() {
        return null;
    }

    public static Expr atan2() {
        return null;
    }

    public static Expr asinh() {
        return null;
    }

    public static Expr acosh() {
        return null;
    }

    public static Expr atanh() {
        return null;
    }

    public static Expr degreesToRadian() {
        return null;
    }

    public static Expr radiansToDegrees() {
        return null;
    }


    //Type Expression

    public static Expr convert() {
        return null;
    }

    public static Expr isNumber() {
        return null;
    }

    public static Expr toBool() {
        return null;
    }

    public static Expr toDecimal() {
        return null;
    }

    public static Expr toDouble() {
        return null;
    }

    public static Expr toInt() {
        return null;
    }

    public static Expr toLong() {
        return null;
    }

    public static Expr toObjectId() {
        return null;
    }

    public static Expr type() {
        return null;
    }


    //Group stage

    public static Expr addToSet() {
        return null;
    }

    public static Expr avg() {
        return null;
    }

    public static Expr max() {
        return null;
    }

    public static Expr min() {
        return null;
    }

    public static Expr push() {
        return null;
    }

    public static Expr stdDevPop() {
        return null;
    }

    public static Expr stdDevSamp() {
        return null;
    }

    public static Expr sum() {
        return null;
    }


    public static Expr let() {
        return null;
    }


    public abstract Object toQueryObject();

    public static class MapOpExpr extends Expr {
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

    public static class OpExpr extends Expr {
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
                p.add(e.toQueryObject());
            }
            return Utils.getMap(operation, p);
        }
    }


    public static class FieldExpr extends Expr {

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

    public static class StringExpr extends Expr {

        private String str;

        public StringExpr(String str) {
            this.str = str;
        }

        @Override
        public Object toQueryObject() {
            return str;
        }
    }

    public static class IntExpr extends Expr {

        private Integer number;

        public IntExpr(int str) {
            this.number = str;
        }

        @Override
        public Object toQueryObject() {
            return number;
        }
    }


    public static class DoubleExpr extends Expr {

        private Double number;

        public DoubleExpr(double str) {
            this.number = str;
        }

        @Override
        public Object toQueryObject() {
            return number;
        }
    }


    public static class BoolExpr extends Expr {

        private Boolean bool;

        public BoolExpr(boolean b) {
            this.bool = b;
        }

        @Override
        public Object toQueryObject() {
            return bool;
        }
    }

    public static class ArrayExpr extends Expr {

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
