package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Utils;
import de.caluga.morphium.aggregation.Expr;
import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static de.caluga.morphium.aggregation.Expr.*;

public class AggregationExprTest extends TestCase {
    Logger log = LoggerFactory.getLogger(AggregationExprTest.class);

    public void testAbs() {
        Object o = abs(intExpr(-12)).toQueryObject();
        String s = Utils.toJsonString(o);
        log.info("Json: " + s);
        assert (s.equals("{ \"$abs\" : -12 } "));
    }

    public void testRegexMatch() {
        Expr e = regexMatch(string("input"), string("a*"), string("options"));
        String j = Utils.toJsonString(e.toQueryObject());
    }

    public void testRegexFind() {
        Expr e = regexFind(string("input"), string("a*"), string("options"));
        String j = Utils.toJsonString(e.toQueryObject());
    }

    public void testRegexFindAll() {
        Expr e = regexFindAll(string("input"), string("a*"), string("options"));
        String j = Utils.toJsonString(e.toQueryObject());
    }

    public void testReplaceOne() {
        Expr e = replaceOne(string("input"), string("a*"), string("replace"));
        String j = Utils.toJsonString(e.toQueryObject());
    }

    public void testReplaceAll() {
        Expr e = replaceAll(string("input"), string("a*"), string("replace"));
        String j = Utils.toJsonString(e.toQueryObject());
    }


    public void testField() {
        Expr fld = field("test");
        assert (fld.toQueryObject().equals("$test"));
    }

    public void dateTest() {
        Expr dt = date(new Date());
        assert (dt.toQueryObject() instanceof Date);
    }


    public void testDoubleExpr() {
        Expr e = doubleExpr(123.4);
        assert (e.toQueryObject().equals(123.4));
    }

    public void testIntExpr() {
        Expr e = intExpr(123);
        assert (e.toQueryObject().equals(123));
    }

    public void testBool() {
        Expr e = bool(true);
        assert (e.toQueryObject().equals(true));
    }

    public void testArrayExpr() {
        Expr e = arrayExpr(intExpr(1), string("test"));
        assert (e.toQueryObject() instanceof List);
        assert (((List) e.toQueryObject()).get(0).equals(1));
    }

    public void testString() {
        Expr e = string("test");
        assert (e.toQueryObject().equals("test"));
    }

    public void testAdd() {
        Expr e = add(field("tst"), intExpr(42));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$add\" :  [ \"$tst\", 42] } "));
    }

    public void testCeil() {
        Expr e = ceil(doubleExpr(42.42));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$ceil\" : 42.42 } "));
    }

    public void testDivide() {
        Expr e = divide(doubleExpr(42), doubleExpr(12));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$divide\" :  [ 42.0, 12.0] } "));
    }

    public void testExp() {
        Expr e = exp(doubleExpr(42));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$exp\" : 42.0 } "));
    }

    public void testFloor() {
        Expr e = floor(doubleExpr(42));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$floor\" : 42.0 } "));
    }

    public void testLn() {
        Expr e = ln(doubleExpr(42));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$ln\" : 42.0 } "));
    }

    public void testLog() {
        Expr e = log(doubleExpr(42), intExpr(10));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$log\" :  [ 42.0, 10] } "));
    }

    public void testLog10() {
        Expr e = log10(doubleExpr(42));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$log10\" : 42.0 } "));
    }

    public void testMod() {
        Expr e = mod(doubleExpr(42), doubleExpr(12));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$mod\" :  [ 42.0, 12.0] } "));
    }

    public void testMultiply() {
        Expr e = multiply(doubleExpr(42), doubleExpr(12));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$multiply\" :  [ 42.0, 12.0] } "));
    }

    public void testPow() {
        Expr e = pow(doubleExpr(42), doubleExpr(12));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$pow\" :  [ 42.0, 12.0] } "));
    }

    public void testRound() {
        Expr e = round(doubleExpr(42));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$round\" : 42.0 } "));
    }

    public void testSqrt() {
        Expr e = sqrt(doubleExpr(42));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$sqrt\" : 42.0 } "));
    }

    public void testSubtract() {
        Expr e = subtract(doubleExpr(42), doubleExpr(12));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$substract\" :  [ 42.0, 12.0] } "));
    }

    public void testTrunc() {
        Expr e = trunc(doubleExpr(42.23), doubleExpr(1));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$trunc\" :  [ 42.23, 1.0] } "));
    }

    public void testArrayElemAt() {
        Expr e = arrayElemAt(arrayExpr(intExpr(1), intExpr(41)), intExpr(1));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$arrayElemAt\" :  [  [ 1, 41], 1] } "));
    }

    public void testArrayToObject() {
        Expr e = arrayToObject(arrayExpr(string("value"), intExpr(42)));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$arrayToObject\" :  [  [ \"value\", 42]] } "));
    }

    public void testConcatArrays() {
        Expr e = concatArrays(arrayExpr(string("value"), intExpr(42)), arrayExpr(intExpr(1234)));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$concatArrays\" :  [  [ \"value\", 42],  [ 1234]] } "));
    }

    public void testFilter() {
        Expr e = filter(arrayExpr(string("value"), intExpr(42)), "name", gt(field("tst"), intExpr(40)));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$filter\" : { \"input\" :  [ \"value\", 42], \"as\" : \"name\", \"cond\" : { \"$gt\" :  [ \"$tst\", 40] }  }  } "));
    }

    public void testFirst() {
        Expr e = first(arrayExpr(string("value"), intExpr(42)));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$first\" :  [ \"value\", 42] } "));
    }

    public void testIn() {
        Expr e = in(field("test"), arrayExpr(string("value"), intExpr(42)));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$in\" :  [ \"$test\",  [ \"value\", 42]] } "));
    }

    public void testIndexOfArray() {
        Expr e = indexOfArray(arrayExpr(string("value"), intExpr(42)), string("value"), intExpr(0), null);
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$indexOfArray\" :  [  [ \"value\", 42], \"value\", 0] } "));
    }

    public void testIsArray() {
        Expr e = isArray(arrayExpr(string("value"), intExpr(42)));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$isArray\" :  [  [ \"value\", 42]] } "));
    }

    public void testLast() {
        Expr e = last(arrayExpr(string("value"), intExpr(42)));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$last\" :  [ \"value\", 42] } "));
    }

    public void testMap() {
        Expr e = map(arrayExpr(string("value"), intExpr(42)), string("name"), gt(field("name"), intExpr(42)));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$map\" :  [  [ \"value\", 42], \"name\", { \"$gt\" :  [ \"$name\", 42] } ] } "));
    }

    public void testObjectToArray() {
        Expr e = objectToArray(doc(Map.of("_id", (Object) 12, "test", "value")));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$objectToArray\" : { \"_id\" : 12, \"test\" : \"value\" }  } "));
    }

    public void testRange() {
        Expr e = range(intExpr(12), intExpr(42), intExpr(2));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$range\" :  [ 12, 42, 2] } "));
    }

    public void testReduce() {
        Expr e = reduce(arrayExpr(intExpr(1), intExpr(2), intExpr(3),
                intExpr(4)),
                string(""),
                mapExpr(
                        Map.of("sum", add(string("$$value.sum"), string("$$this")),
                                "product", multiply(string("$$value.product"), string("$$this")))
                )

        );
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$reduce\" : { \"input\" :  [ 1, 2, 3, 4], \"initialValue\" : \"\", \"in\" : { \"sum\" : { \"$add\" :  [ \"$$value.sum\", \"$$this\"] } , \"product\" : { \"$multiply\" :  [ \"$$value.product\", \"$$this\"] }  }  }  } "));
    }

    public void testReverseArray() {
        Expr e = reverseArray(arrayExpr(intExpr(42), intExpr(2)));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$reverseArray\" :  [ 42, 2] } "));
    }

    public void testSize() {
        Expr e = size(arrayExpr(intExpr(42), intExpr(2)));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$size\" :  [ 42, 2] } "));
    }

    public void testSlice() {
        Expr e = slice(arrayExpr(intExpr(42), intExpr(4), intExpr(12), intExpr(2)), intExpr(1), intExpr(2));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$slice\" :  [  [ 42, 4, 12, 2], 1, 2] } "));
    }

    public void testZip() {
        List<Expr> inputs = new ArrayList<>();
        inputs.add(arrayExpr(intExpr(42), intExpr(4), intExpr(12), intExpr(2)));
        inputs.add(arrayExpr(intExpr(122), intExpr(3), intExpr(17), intExpr(9)));
        inputs.add(arrayExpr(intExpr(782), intExpr(1234), intExpr(-5), intExpr(6)));

        Expr e = zip(inputs, bool(false), arrayExpr(intExpr(122), intExpr(3), intExpr(17), intExpr(9)));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$zip\" : { \"inputs\" :  [  [ 42, 4, 12, 2],  [ 122, 3, 17, 9],  [ 782, 1234, -5, 6]], \"useLongestLength\" : false, \"defaults\" :  [ 122, 3, 17, 9] }  } "));
    }

    public void testAnd() {
        Expr e = and(gte(intExpr(12), field("test")),
                lt(field("count"), doubleExpr(12.2)),
                anyElementTrue(bool(false), bool(true), field("checker"))
        );
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$and\" :  [ { \"$gte\" :  [ 12, \"$test\"] } , { \"$lt\" :  [ \"$count\", 12.2] } , { \"$anyElementsTrue\" :  [ false, true, \"$checker\"] } ] } "));
    }

    public void testOr() {
        Expr e = or(gte(intExpr(12), field("test")),
                lt(field("count"), doubleExpr(12.2)),
                anyElementTrue(bool(false), bool(true), field("checker"))
        );
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$or\" :  [ { \"$gte\" :  [ 12, \"$test\"] } , { \"$lt\" :  [ \"$count\", 12.2] } , { \"$anyElementsTrue\" :  [ false, true, \"$checker\"] } ] } "));

    }

    public void testNot() {
        Expr e = not(lte(field("count"), doubleExpr(12.3)));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$not\" : { \"$lte\" :  [ \"$count\", 12.3] }  } "));

    }

    public void testCmp() {
        Expr e = cmp(intExpr(12), doubleExpr(21.2));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$cmp\" :  [ 12, 21.2] } "));
    }

    public void testEq() {
        Expr e = eq(intExpr(12), doubleExpr(21.2));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$eq\" :  [ 12, 21.2] } "));
    }

    public void testNe() {
        Expr e = ne(intExpr(12), doubleExpr(21.2));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$ne\" :  [ 12, 21.2] } "));
    }

    public void testGt() {
        Expr e = gt(intExpr(12), doubleExpr(21.2));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$gt\" :  [ 12, 21.2] } "));
    }

    public void testLt() {
        Expr e = lt(intExpr(12), doubleExpr(21.2));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$lt\" :  [ 12, 21.2] } "));
    }

    public void testGte() {
        Expr e = gte(intExpr(12), doubleExpr(21.2));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$gte\" :  [ 12, 21.2] } "));
    }

    public void testLte() {
        Expr e = lte(intExpr(12), doubleExpr(21.2));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$lte\" :  [ 12, 21.2] } "));
    }

    public void testCond() {
        Expr e = cond(lt(field("created"), string("now")), intExpr(12), doubleExpr(21.2));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$cond\" :  [ { \"$lt\" :  [ \"$created\", \"now\"] } , 12, 21.2] } "));
    }

    public void testIfNull() {
        Expr e = ifNull(field("testField"), field("otherField"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$ifNull\" :  [ \"$testField\", \"$otherField\"] } "));
    }

    public void testSwitchExpr() {
        Expr e = switchExpr(Map.of(Expr.gt(field("test"), intExpr(12)), string("teststring")), intExpr(12));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$switch\" : { \"branches\" :  [ { \"case\" : { \"$gt\" :  [ \"$test\", 12] } , \"then\" : \"teststring\" } ], \"default\" : 12 }  } "));
    }

    public void testFunction() {
        Expr e = function("code", Expr.field("fieldArg"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$function\" : { \"body\" : \"code\", \"args\" : \"$fieldArg\", \"lang\" : \"js\" }  } "));
    }

    public void testAccumulator() {
        Expr e = accumulator("init code here", Expr.field("InitArgs"), "Accumulating code", Expr.string("accArgs"), "Merged code", "finalizeCode");
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$accumulator\" : { \"init\" : \"init code here\", \"initArgs\" : \"$InitArgs\", \"accumulate\" : \"Accumulating code\", \"accumulateArgs\" : \"accArgs\", \"merge\" : \"Merged code\", \"finalize\" : \"finalizeCode\", \"lang\" : \"js\" }  } "));
    }

    public void testBinarySize() {
        Expr e = binarySize(field("fld"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$binarySize\" : \"$fld\" } "));
    }

    public void testBsonSize() {
        Expr e = bsonSize(field("fld"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$bsonSize\" : \"$fld\" } "));
    }

    public void testDateFromParts() {
        Expr e = dateFromParts(intExpr(2020), intExpr(8), intExpr(12), intExpr(22), intExpr(34), intExpr(29), intExpr(123), string("CET"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$dateFromParts\" : { \"year\" : 2020, \"month\" : 8, \"day\" : 12, \"hour\" : 22, \"minute\" : 34, \"second\" : 29, \"millisecond\" : 123, \"timezone\" : \"CET\" }  } "));
    }

    public void testDateFromString() {
        Expr e = dateFromString(field("fld"), null, null, null, null);
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$dateFromString\" : { \"dateString\" : \"$fld\" }  } "));
    }

    public void testDateToParts() {
        Expr e = dateToParts(field("fld"), null, false);
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$dateToParts\" : { \"date\" : \"$fld\", \"iso8601\" : false }  } "));
    }

    public void testDateToString() {
        Expr e = dateToString(field("fld"), null, null, Expr.string("no date"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$dateToString\" : { \"dateString\" : \"$fld\", \"onNull\" : \"no date\" }  } "));
    }

    public void testDayOfMonth() {
        Expr e = dayOfMonth(field("fld"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$dayOfMonth\" : \"$fld\" } "));
    }

    public void testDayOfWeek() {
        Expr e = dayOfWeek(field("fld"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$dayOfWeek\" : \"$fld\" } "));
    }

    public void testDayOfYear() {
        Expr e = dayOfYear(field("fld"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$dayOfYear\" : \"$fld\" } "));
    }

    public void testHour() {
        Expr e = hour(field("fld"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$hour\" : \"$fld\" } "));
    }

    public void testIsoDayOfWeek() {
        Expr e = isoDayOfWeek(field("fld"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$isoDayOfWeek\" : \"$fld\" } "));
    }

    public void testIsoWeek() {
        Expr e = isoWeek(field("fld"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$isoWeek\" : \"$fld\" } "));
    }

    public void testIsoWeekYear() {
        Expr e = isoWeekYear(field("fld"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$isoWeekYear\" : \"$fld\" } "));
    }

    public void testMillisecond() {
        Expr e = millisecond(field("fld"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$millisecond\" : \"$fld\" } "));
    }

    public void testMinute() {
        Expr e = minute(field("fld"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$minute\" : \"$fld\" } "));
    }

    public void testMonth() {
        Expr e = month(field("fld"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$month\" : \"$fld\" } "));
    }

    public void testSecond() {
        Expr e = second(field("fld"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$second\" : \"$fld\" } "));
    }

    public void testToDate() {
        Expr e = toDate(field("fld"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$toDate\" : \"$fld\" } "));
    }

    public void testWeek() {
        Expr e = week(field("fld"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$week\" : \"$fld\" } "));
    }

    public void testYear() {
        Expr e = year(field("fld"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$year\" : \"$fld\" } "));
    }

    public void testLiteral() {
        Expr e = literal(string("$$fieldname"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$literal\" : \"$$fieldname\" } "));
    }

    public void testMergeObjects() {
        Expr e = mergeObjects(field("fld"), field("doc2"), mapExpr(Map.of("test", intExpr(123), "value", string("val"))));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$mergeObjects\" :  [ \"$fld\", \"$doc2\", { \"test\" : 123, \"value\" : \"val\" } ] } "));
    }

    public void testTestMergeObjects() {
        Expr e = mergeObjects(field("fld"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$mergeObjects\" : \"$fld\" } "));
    }

    public void testAllElementsTrue() {
        Expr e = allElementsTrue(field("fld"), bool(true), field("other"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$allElementsTrue\" :  [ \"$fld\", true, \"$other\"] } "));
    }

    public void testAnyElementTrue() {
        Expr e = anyElementTrue(field("fld"), bool(true), field("other"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$anyElementsTrue\" :  [ \"$fld\", true, \"$other\"] } "));
    }

    public void testSetDifference() {
        Expr e = setDifference(field("fld"), field("other"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$setDifference\" :  [ \"$fld\", \"$other\"] } "));
    }

    public void testSetEquals() {
        Expr e = setEquals(field("fld"), field("other"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$setEquals\" :  [ \"$fld\", \"$other\"] } "));
    }

    public void testSetIntersection() {
        Expr e = setIntersection(field("fld"), field("other"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$setIntersection\" :  [ \"$fld\", \"$other\"] } "));
    }

    public void testSetIsSubset() {
        Expr e = setIsSubset(field("fld"), field("other"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$setIsSubset\" :  [ \"$fld\", \"$other\"] } "));
    }

    public void testSetUnion() {
        Expr e = setUnion(field("fld"), field("other"), arrayExpr(intExpr(12), intExpr(22), intExpr(10)));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$setUnion\" :  [ \"$fld\", \"$other\",  [ 12, 22, 10]] } "));
    }

    public void testConcat() {
        Expr e = concat(field("fld"), field("other"), arrayExpr(intExpr(12), intExpr(22), intExpr(10)));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$concat\" :  [ \"$fld\", \"$other\",  [ 12, 22, 10]] } "));
    }

    public void testIndexOfBytes() {
        Expr e = indexOfBytes(string("String to search in for substring"), string("substring"), intExpr(0), null);
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$indexOfBytes\" :  [ \"String to search in for substring\", \"substring\", 0] } "));
    }

    public void testIndexOfCP() {
        Expr e = indexOfCP(string("String to search in for substring"), string("substring"), intExpr(0), null);
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$indexOfCP\" :  [ \"String to search in for substring\", \"substring\", 0] } "));
    }

    public void testLtrim() {
        Expr e = ltrim(string("string to trim"), string(" "));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$ltrim\" : { \"input\" : \"string to trim\", \"chars\" : \" \" }  } "));
    }

    public void testRtrim() {
        Expr e = rtrim(string("string to trim"), string(" "));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$rtrim\" : { \"input\" : \"string to trim\", \"chars\" : \" \" }  } "));
    }

    public void testToLower() {
        Expr e = toLower(string("text to lower"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$toLower\" : \"text to lower\" } "));
    }

    public void testToStr() {
        Expr e = toStr(field("testfield"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$toString\" : \"$testfield\" } "));
    }

    public void testTrim() {
        Expr e = trim(string("string to trim"), string(" "));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$trim\" : { \"input\" : \"string to trim\", \"chars\" : \" \" }  } "));
    }

    public void testToUpper() {
        Expr e = toUpper(string("text to upper"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$toUpper\" : \"text to upper\" } "));
    }

    public void testMeta() {
    }

    public void testSin() {
        Expr e = sin(field("testField"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$sin\" : \"$testField\" } "));
    }

    public void testCos() {
        Expr e = cos(intExpr(23));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$cos\" : 23 } "));
    }

    public void testTan() {
        Expr e = tan(intExpr(23));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$tan\" : 23 } "));
    }

    public void testAsin() {
        Expr e = asin(intExpr(23));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$asin\" : 23 } "));
    }

    public void testAcos() {
        Expr e = acos(intExpr(23));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$acos\" : 23 } "));
    }

    public void testAtan() {
        Expr e = atan(intExpr(23));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$atan\" : 23 } "));
    }

    public void testAtan2() {
        Expr e = atan2(intExpr(23), intExpr(2));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$atan2\" :  [ 23, 2] } "));
    }

    public void testAsinh() {
        Expr e = asinh(intExpr(23));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$asinh\" : 23 } "));
    }

    public void testAcosh() {
        Expr e = acosh(intExpr(23));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$acosh\" : 23 } "));
    }

    public void testAtanh() {
        Expr e = atanh(intExpr(23), intExpr(1));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$atanh\" :  [ 23, 1] } "));
    }

    public void testDegreesToRadian() {
        Expr e = degreesToRadian(intExpr(230));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$degreesToRadian\" : 230 } "));

    }

    public void testRadiansToDegrees() {
        Expr e = radiansToDegrees(doubleExpr(1.28));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$radiansToDegrees\" : 1.28 } "));
    }

    public void testConvert() {
        Expr e = convert(intExpr(230), intExpr(2), string("error"), string("null"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$convert\" : { \"input\" : 230, \"to\" : 2, \"onError\" : \"error\", \"onNull\" : \"null\" }  } "));
    }

    public void testConvert2() {
        Expr e = convert(intExpr(230), intExpr(2));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$convert\" : { \"input\" : 230, \"to\" : 2 }  } "));
    }


    public void testConvert3() {
        Expr e = convert(intExpr(230), intExpr(2), string("error"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$convert\" : { \"input\" : 230, \"to\" : 2, \"onError\" : \"error\" }  } "));
    }

    public void testDateFromParts2() {
        Expr e = dateFromParts(intExpr(2020));
        e = dateFromParts(intExpr(2020), intExpr(12), intExpr(12), intExpr(12));
        e = dateFromParts(intExpr(2020), intExpr(12), intExpr(12), intExpr(12), intExpr(12), intExpr(12));
        e = dateFromParts(intExpr(2020), intExpr(12), intExpr(12), intExpr(12), intExpr(12), intExpr(12), intExpr(12));
    }


    public void testIsNumber() {
        Expr e = isNumber(doubleExpr(1.28));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$isNumber\" : 1.28 } "));
    }

    public void testToBool() {
        Expr e = toBool(intExpr(1));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$toBool\" : 1 } "));
    }

    public void testToDecimal() {
        Expr e = toDecimal(intExpr(1));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$toDecimal\" : 1 } "));
    }

    public void testToDouble() {
        Expr e = toDouble(intExpr(1));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$toDouble\" : 1 } "));
    }

    public void testToInt() {
        Expr e = toInt(intExpr(1));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$toInt\" : 1 } "));
    }

    public void testToLong() {
        Expr e = toLong(intExpr(1));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$toLong\" : 1 } "));
    }

    public void testToObjectId() {
        Expr e = toObjectId(intExpr(1));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$toObjectId\" : 1 } "));
    }

    public void testType() {
        Expr e = type(intExpr(1));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$type\" : 1 } "));
    }

    public void testAddToSet() {
        Expr e = addToSet(field("destinationField"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$addToSet\" : \"$destinationField\" } "));
    }

    public void testAvg() {
        Expr e = avg(field("fld"), intExpr(12), doubleExpr(12.2));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$avg\" :  [ \"$fld\", 12, 12.2] } "));
    }

    public void testTestAvg() {
        Expr e = avg(field("field"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$avg\" : \"$field\" } "));
    }

    public void testMax() {
        Expr e = max(field("fld"), intExpr(12), doubleExpr(12.2));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$max\" :  [ \"$fld\", 12, 12.2] } "));
    }

    public void testTestMax() {
        Expr e = max(field("field"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$max\" : \"$field\" } "));
    }

    public void testMin() {
        Expr e = min(field("fld"), intExpr(12), doubleExpr(12.2));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$min\" :  [ \"$fld\", 12, 12.2] } "));
    }

    public void testTestMin() {
        Expr e = min(field("field"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$min\" : \"$field\" } "));
    }

    public void testPush() {
        Expr e = push(field("field"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$push\" : \"$field\" } "));
    }

    public void testStdDevPop() {
        Expr e = stdDevPop(field("field"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$stdDevPop\" : \"$field\" } "));
    }

    public void testTestStdDevPop() {
        Expr e = stdDevPop(field("fld"), intExpr(12), doubleExpr(12.2));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$stdDevPop\" :  [ \"$fld\", 12, 12.2] } "));
    }

    public void testStdDevSamp() {
        Expr e = stdDevSamp(field("field"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$stdDevSamp\" : \"$field\" } "));
    }

    public void testTestStdDevSamp() {
        Expr e = stdDevSamp(field("fld"), intExpr(12), doubleExpr(12.2));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$stdDevSamp\" :  [ \"$fld\", 12, 12.2] } "));
    }

    public void testSum() {
        Expr e = sum(field("field"));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$sum\" : \"$field\" } "));
    }

    public void testTestSum() {
        Expr e = sum(field("fld"), intExpr(12), doubleExpr(12.2));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$sum\" :  [ \"$fld\", 12, 12.2] } "));
    }

    public void testLet() {
        Expr e = let(Map.of("var1", Expr.field("testField")), first(field("var1")));
        log.info(Utils.toJsonString(e.toQueryObject()));
        assert (Utils.toJsonString(e.toQueryObject()).equals("{ \"$let\" : { \"vars\" : { \"var1\" : \"$testField\" } , \"in\" : { \"$first\" : \"$var1\" }  }  } "));

    }

    public void testLetEvaluation() {
        Expr e = Expr.let(Map.of("var1", Expr.field("testField")), Expr.abs(Expr.field("var1")));
        Object result = e.evaluate(Map.of("testField", 100));
        assert (result != null);
        assert (result.equals(100.0));
    }

    public void testIsoDateFromParts() {
        Expr e = isoDateFromParts(intExpr(2020));
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(2020);
        e = isoDateFromParts(intExpr(2020), intExpr(2));
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(2020);
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(2);

        e = isoDateFromParts(intExpr(2020), intExpr(2), intExpr(48));
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(2020);
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(2);
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(48);

        e = isoDateFromParts(intExpr(2020), intExpr(2), intExpr(48), intExpr(23));
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(2020);
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(2);
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(48);
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(23);

        e = isoDateFromParts(intExpr(2020), intExpr(2), intExpr(48), intExpr(23), intExpr(59));
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(2020);
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(2);
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(48);
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(23);
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(59);

        e = isoDateFromParts(intExpr(2020), intExpr(2), intExpr(48), intExpr(23), intExpr(59), intExpr(38));
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(2020);
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(2);
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(48);
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(23);
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(59);
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(38);

        e = isoDateFromParts(intExpr(2020), intExpr(2), intExpr(48), intExpr(23), intExpr(59), intExpr(38), intExpr(999));
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(2020);
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(2);
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(48);
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(23);
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(59);
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(38);
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(999);

        e = isoDateFromParts(intExpr(2020), intExpr(2), intExpr(48), intExpr(23), intExpr(59), intExpr(38), intExpr(999), string("UTC"));
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(2020);
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(2);
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(48);
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(23);
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(59);
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(38);
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue(999);
        assert ((Map<String, Object>) (((Map<String, Object>) e.toQueryObject()).get("$dateFromParts"))).containsValue("UTC");

    }

}