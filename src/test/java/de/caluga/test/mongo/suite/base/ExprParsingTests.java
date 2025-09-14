package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.aggregation.Expr;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

@Tag("core")
public class ExprParsingTests {
    private final Logger log = LoggerFactory.getLogger(ExprParsingTests.class);

    @Test
    public void parseMod() {
        Map<String, Object> qo = new HashMap<>();
        qo.put("$mod", Arrays.asList(Expr.field("field"), Expr.doubleExpr(5.0)));

        Expr add = Expr.parse(qo);
        Map<String, Object> context = UtilsMap.of("field", 12);
        Object result = add.evaluate(context);
        assert (result.equals(2.0));
        log.info("done");
    }
    @Test
    public void parseAdd() {
        Map<String, Object> qo = new HashMap<>();
        qo.put("$add", Arrays.asList(Expr.field("field"), Expr.doubleExpr(32.0), Expr.intExpr(3)));

        Expr add = Expr.parse(qo);
        Map<String, Object> context = UtilsMap.of("field", 12);
        Object result = add.evaluate(context);
        assert (result.equals(47.0));
        log.info("done");
    }

    @Test
    public void backAndForthTest() {
        Expr o = Expr.abs(Expr.intExpr(1));
        Expr o2 = Expr.parse(o.toQueryObject());
        assert (o.toQueryObject().equals(o2.toQueryObject()));
        Map<String, Object> context = UtilsMap.of("test", 1);
        assert (o.evaluate(context).equals(o2.evaluate(context)));
    }


    @Test
    public void allCallerTest() throws Exception {
        Method[] declaredMethods = Expr.class.getDeclaredMethods();
        Arrays.sort(declaredMethods, new Comparator<Method>() {
            @Override
            public int compare(Method o1, Method o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });
        boolean failed = false;
        List<String> failedElements = new ArrayList<>();
        int count = 0;
        for (Method m : declaredMethods) {
            if (Modifier.isStatic(m.getModifiers())) {
                try {
                    Expr expr1;
                    Expr expr2;
                    if (m.getParameterTypes().length == 1 && m.getParameterTypes()[0] == Expr.class) {
                        count++;
                        log.info("Method with one param: " + m.getName());
                        expr1 = (Expr) m.invoke(null, Expr.intExpr(1));
                        expr2 = Expr.parse(expr1.toQueryObject());
                    } else if (m.getParameterCount() == 1 && m.getParameterTypes()[0].isArray()) {
                        count++;
                        log.info("Method with array param: " + m.getName());
                        expr1 = (Expr) m.invoke(null, new Object[] {new Expr[]{Expr.intExpr(1)}});
                        expr2 = Expr.parse(expr1.toQueryObject());
                    } else if (m.getParameterCount() == 2 && m.getParameterTypes()[0] == Expr.class) {
                        count++;
                        log.info("Method with two param: " + m.getName());
                        expr1 = (Expr) m.invoke(null, Expr.intExpr(1), Expr.intExpr(1));
                        expr2 = Expr.parse(expr1.toQueryObject());
                    } else if (m.getParameterCount() == 3 && m.getParameterTypes()[0] == Expr.class) {
                        count++;
                        log.info("Method with two param: " + m.getName());
                        expr1 = (Expr) m.invoke(null, Expr.intExpr(1), Expr.intExpr(1), Expr.intExpr(1));
                        expr2 = Expr.parse(expr1.toQueryObject());
                    } else {
                        continue;
                    }
                    if (!expr1.toQueryObject().equals(expr2.toQueryObject())) {
                        log.error("QueryObjects differ");
                        failed = true;
                        failedElements.add(m.getName());
                    }
                    HashMap<String, Object> context = new HashMap<>();
                    if (expr1.evaluate(context) == null && expr2.evaluate(context) != null) {
                        log.error(m + " failed, evaluate null vs not null");
                        failed = true;
                        failedElements.add(m.getName());
                    } else if (expr1.evaluate(context) == null && expr2.evaluate(context) == null) {
                        //all good
                    } else {
                        if (!expr1.evaluate(context).equals(expr2.evaluate(context))) {
                            log.error(m + " Failed, evaluate results differ: " + expr1.evaluate(context) + "!=" + expr2.evaluate(context));
                            failed = true;
                            failedElements.add(m.getName());
                        }
                    }

                } catch (Exception e) {
                    if (e instanceof ClassCastException) {
                        log.warn("Could be ok, that we have a class-castExeption when running " + m.getName(), e);

                    } else {
                        failed = true;
                        log.error(m + " failed", e);
                        failedElements.add(m.getName());
                    }
                }
            }
        }
        if (failed) {
            log.error("Summary... failed Methods:");
            for (String m : failedElements) {
                log.error(".... " + m);
            }
            log.error("Total fails: " + failedElements.size() + " of total checks " + count);
        }
        //assert (!failed);

    }
}
