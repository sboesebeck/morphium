package de.caluga.morphium.aggregation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 31.08.12
 * Time: 08:48
 * <p>
 * Aggregator-Group
 * </p>
 *
 * @param <T> search type
 * @param <R> result type
 */
@SuppressWarnings("UnusedDeclaration")
public class Group<T, R> {
    @SuppressWarnings({"CanBeFinal", "FieldMayBeFinal"})
    private Logger log = LoggerFactory.getLogger(Group.class);
    @SuppressWarnings({"CanBeFinal", "FieldMayBeFinal"})
    private Aggregator<T, R> aggregator;
    @SuppressWarnings({"CanBeFinal", "FieldMayBeFinal"})
    private Map<String, Object> id;
    private boolean ended = false;

    @SuppressWarnings({"CanBeFinal", "FieldMayBeFinal"})
    private List<Map<String, Object>> operators = new ArrayList<>();

    public Group(Aggregator<T, R> ag, String id) {
        aggregator = ag;
        this.id = getMap("_id", id);
    }

    public Group(Aggregator<T, R> ag, Map<String, Object> id) {
        aggregator = ag;
        this.id = getMap("_id", id);
    }

    public Group(Aggregator<T, R> ag, Expr id) {
        aggregator = ag;
        this.id = getMap("_id", id.toQueryObject());
    }

    private Map<String, Object> getMap(String key, Object value) {
        Map<String, Object> ret = new HashMap<>();
        ret.put(key, value);
        return ret;
    }

    private String tf(String field) {
        if (aggregator.getMorphium() == null) return field;
        if (aggregator.getMorphium().getARHelper().getField(aggregator.getSearchType(), field) == null) return field;
        return aggregator.getMorphium().getARHelper().getMongoFieldName(aggregator.getSearchType(), field);
    }

    private String ref(Enum<?> field) {
        return "$" + tf(field.name());
    }

    public Group<T, R> addToSet(String name, Object p) {
        Map<String, Object> o = getMap(name, getMap("$addToSet", p));
        operators.add(o);
        return this;

    }

    public Group<T, R> addToSet(String name, Enum<?> fieldRef) {
        return addToSet(name, (Object) ref(fieldRef));
    }

    public Group<T, R> addToSet(Enum<?> name, Enum<?> fieldRef) {
        return addToSet(tf(name.name()), (Object) ref(fieldRef));
    }

    public Group<T, R> addToSet(Enum<?> name, Object p) {
        return addToSet(tf(name.name()), p);
    }

    public Group<T, R> addToSet(String name, Map<String, Object> param) {
        Map<String, Object> o = getMap(name, getMap("$addToSet", param));
        operators.add(o);
        return this;
    } //don't know what this actually should do???
    public Group<T, R> addToSet(String name, String vn, String value) {
        Map<String, Object> o = getMap(name, getMap("$addToSet", getMap(vn, value)));
        operators.add(o);
        return this;
    }

    public Group<T, R> first(String name, Object p) {
        Map<String, Object> o = getMap(name, getMap("$first", p));
        operators.add(o);
        return this;
    }

    public Group<T, R> first(String name, Enum<?> fieldRef) {
        return first(name, (Object) ref(fieldRef));
    }

    public Group<T, R> first(Enum<?> name, Enum<?> fieldRef) {
        return first(tf(name.name()), (Object) ref(fieldRef));
    }

    public Group<T, R> first(Enum<?> name, Object p) {
        return first(tf(name.name()), p);
    }

    public Group<T, R> last(String name, Object p) {
        Map<String, Object> o = getMap(name, getMap("$last", p));
        operators.add(o);
        return this;
    }

    public Group<T, R> last(String name, Enum<?> fieldRef) {
        return last(name, (Object) ref(fieldRef));
    }

    public Group<T, R> last(Enum<?> name, Enum<?> fieldRef) {
        return last(tf(name.name()), (Object) ref(fieldRef));
    }

    public Group<T, R> last(Enum<?> name, Object p) {
        return last(tf(name.name()), p);
    }

    public Group<T, R> max(String name, Object p) {
        Map<String, Object> o = getMap(name, getMap("$max", p));
        operators.add(o);
        return this;
    }

    public Group<T, R> max(String name, Enum<?> fieldRef) {
        return max(name, (Object) ref(fieldRef));
    }

    public Group<T, R> max(Enum<?> name, Enum<?> fieldRef) {
        return max(tf(name.name()), (Object) ref(fieldRef));
    }

    public Group<T, R> max(Enum<?> name, Object p) {
        return max(tf(name.name()), p);
    }

    public Group<T, R> min(String name, Object p) {
        Map<String, Object> o = getMap(name, getMap("$min", p));
        operators.add(o);
        return this;
    }

    public Group<T, R> min(String name, Enum<?> fieldRef) {
        return min(name, (Object) ref(fieldRef));
    }

    public Group<T, R> min(Enum<?> name, Enum<?> fieldRef) {
        return min(tf(name.name()), (Object) ref(fieldRef));
    }

    public Group<T, R> min(Enum<?> name, Object p) {
        return min(tf(name.name()), p);
    }

    public Group<T, R> avg(String name, Object p) {
        Map<String, Object> o = getMap(name, getMap("$avg", p));
        operators.add(o);
        return this;
    }

    public Group<T, R> avg(String name, Enum<?> fieldRef) {
        return avg(name, (Object) ref(fieldRef));
    }

    public Group<T, R> avg(Enum<?> name, Enum<?> fieldRef) {
        return avg(tf(name.name()), (Object) ref(fieldRef));
    }

    public Group<T, R> avg(Enum<?> name, Object p) {
        return avg(tf(name.name()), p);
    }

    public Group<T, R> push(String name, Object o) {
        Map<String, Object> jp = getMap(name, getMap("$push", o));
        operators.add(jp);
        return this;
    }

    public Group<T, R> push(String name, Enum<?> fieldRef) {
        return push(name, (Object) ref(fieldRef));
    }

    public Group<T, R> push(Enum<?> name, Enum<?> fieldRef) {
        return push(tf(name.name()), (Object) ref(fieldRef));
    }

    public Group<T, R> push(Enum<?> name, Object p) {
        return push(tf(name.name()), p);
    }
    public Group<T, R> push(String name, String vn, String value) {
        Map<String, Object> o = getMap(name, getMap("$push", getMap(vn, value)));
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

    public Group<T, R> sum(String name, Enum<?> fieldRef) {
        return sum(name, (Object) ref(fieldRef));
    }

    public Group<T, R> sum(Enum<?> name, Enum<?> fieldRef) {
        return sum(tf(name.name()), (Object) ref(fieldRef));
    }

    public Group<T, R> sum(Enum<?> name, Object p) {
        return sum(tf(name.name()), p);
    }

    public Group<T, R> stdDevPop(String name, Object value) {
        operators.add(getMap(name, getMap("$stdDevPop", value)));
        return this;
    }

    public Group<T, R> stdDevPop(String name, Enum<?> fieldRef) {
        return stdDevPop(name, (Object) ref(fieldRef));
    }

    public Group<T, R> stdDevPop(Enum<?> name, Enum<?> fieldRef) {
        return stdDevPop(tf(name.name()), (Object) ref(fieldRef));
    }

    public Group<T, R> stdDevPop(Enum<?> name, Object p) {
        return stdDevPop(tf(name.name()), p);
    }

    public Group<T, R> stdDevSamp(String name, Object value) {
        operators.add(getMap(name, getMap("$stdDevSamp", value)));
        return this;
    }

    public Group<T, R> stdDevSamp(String name, Enum<?> fieldRef) {
        return stdDevSamp(name, (Object) ref(fieldRef));
    }

    public Group<T, R> stdDevSamp(Enum<?> name, Enum<?> fieldRef) {
        return stdDevSamp(tf(name.name()), (Object) ref(fieldRef));
    }

    public Group<T, R> stdDevSamp(Enum<?> name, Object p) {
        return stdDevSamp(tf(name.name()), p);
    }

    public Group<T, R> expr(String fld, Expr e) {
        operators.add(getMap(fld, e.toQueryObject()));
        return this;
    }


    public Aggregator<T, R> end() {
        if (ended) {
//            log.debug("Group.end() already called!");
            return aggregator;
        }
        Map<String, Object> params = new HashMap<>(id);
        operators.forEach(params::putAll);
        if (aggregator.isTranslateAggregationFieldNames()) {
            //noinspection unchecked
            params = (Map<String, Object>) translateRefs(params);
        }
        Map<String, Object> obj = getMap("$group", params);
        aggregator.addOperator(obj);
        ended = true;
        return aggregator;
    }

    /**
     * translates $-field references (not $$-variables) from Java property names to Mongo
     * field names, recursing into maps and lists. Only active when
     * translateAggregationFieldNames is enabled.
     */
    @SuppressWarnings("unchecked")
    private Object translateRefs(Object value) {
        if (value instanceof String) {
            String s = (String) value;
            if (s.startsWith("$") && !s.startsWith("$$")) {
                return "$" + tf(s.substring(1));
            }
            return s;
        }
        if (value instanceof Map) {
            Map<String, Object> ret = new HashMap<>();
            for (Map.Entry<String, Object> e : ((Map<String, Object>) value).entrySet()) {
                ret.put(e.getKey(), translateRefs(e.getValue()));
            }
            return ret;
        }
        if (value instanceof List) {
            List<Object> ret = new ArrayList<>();
            for (Object o : (List<Object>) value) {
                ret.add(translateRefs(o));
            }
            return ret;
        }
        return value;
    }

    public List<Map<String, Object>> getOperators() {
        return operators;
    }
}
