package de.caluga.test.mongo.suite;

import de.caluga.morphium.Utils;
import de.caluga.morphium.aggregation.Expr;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ExprParsingTests {
    private final Logger log = LoggerFactory.getLogger(ExprParsingTests.class);

    @Test
    public void simpleParsing() {
        Map<String, Object> qo = new HashMap<>();
        qo.put("$add", Arrays.asList("$field", 32.0d, 3));

        Expr add = Expr.parse(qo);
        Map<String, Object> context = Utils.getMap("field", 12);
        Object result = add.evaluate(context);
        log.info("done");

        qo = new HashMap<>();
        qo.put("$add", Arrays.asList(1, 2, 3));

    }
}
