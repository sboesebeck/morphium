package de.caluga.morphium.aggregation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 31.08.12
 * Time: 08:48
 * <p/>
 * Aggregator-Group
 */
@SuppressWarnings("UnusedDeclaration")
public class Group<T, R> {
    private Aggregator<T, R> aggregator;
    private Map<String, Object> id;

    private List<Map<String, Object>> operators = new ArrayList<>();

    public Group(Aggregator<T, R> ag, String id) {
        aggregator = ag;
        this.id = getMap("_id", id);
    }

    public Group(Aggregator<T, R> ag, Map<String, Object> id) {
        aggregator = ag;
        this.id = getMap("_id", id);
    }

    private Map<String, Object> getMap(String key, Object value) {
        Map<String, Object> ret = new HashMap<>();
        ret.put(key, value);
        return ret;
    }

    public Group<T, R> addToSet(Map<String, Object> param) {
        Map<String, Object> o = getMap("$addToSet", param);
        operators.add(o);
        return this;
    } //don't know what this actually should do???

    public Group<T, R> first(String name, Object p) {
        Map<String, Object> o = getMap(name, getMap("$first", p));
        operators.add(o);
        return this;
    }

    public Group<T, R> last(String name, Object p) {
        Map<String, Object> o = getMap(name, getMap("$last", p));
        operators.add(o);
        return this;
    }

    public Group<T, R> max(String name, Object p) {
        Map<String, Object> o = getMap(name, getMap("$max", p));
        operators.add(o);
        return this;
    }

    public Group<T, R> min(String name, Object p) {
        Map<String, Object> o = getMap(name, getMap("$min", p));
        operators.add(o);
        return this;
    }

    public Group<T, R> avg(String name, Object p) {
        Map<String, Object> o = getMap(name, getMap("$avg", p));
        operators.add(o);
        return this;
    }

    public Group<T, R> push(String name, Object p) {
        Map<String, Object> o = getMap(name, getMap("$push", p));
        operators.add(o);
        return this;
    }


    public Group<T, R> sum(String name, int p) {
        return sum(name, Integer.valueOf(p));
    }

    public Group<T, R> sum(String name, long p) {
        return sum(name, Long.valueOf(p));
    }

    public Group<T, R> sum(String name, Object p) {
        Map<String, Object> o = getMap(name, getMap("$sum", p));
        operators.add(o);
        return this;
    }

    public Group<T, R> sum(String name, String p) {
        return sum(name, (Object) p);
    }

    public Aggregator<T, R> end() {
        Map<String, Object> params = new HashMap<>();
        params.putAll(id);
        operators.forEach(params::putAll);
        Map<String, Object> obj = getMap("$group", params);
        aggregator.addOperator(obj);
        return aggregator;
    }

    public List<Map<String, Object>> getOperators() {
        return operators;
    }
}
