package de.caluga.morphium.aggregation;

import java.util.List;
import java.util.Map;

public class ExpressionImpl implements Expression {

    private String operation;
    private List<Map<String, Object>> params;


    public static final Expression getExpressionFor(Expressions expr) {
        ExpressionImpl ret = new ExpressionImpl();
        ret.operation = "$" + expr.name();
        return ret;
    }
}
