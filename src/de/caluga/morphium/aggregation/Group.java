package de.caluga.morphium.aggregation;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 31.08.12
 * Time: 08:48
 * <p/>
 * TODO: Add documentation here
 */
public class Group<T, R> {
    private Aggregator<T, R> aggregator;
    private BasicDBObject id;

    private List<BasicDBObject> operators = new ArrayList<BasicDBObject>();

    public Group(Aggregator<T, R> ag, Map<String, String> idSubObject) {
        aggregator = ag;
        id = new BasicDBObject("_id", new BasicDBObject(idSubObject));
    }

    public Group(Aggregator<T, R> ag, String id) {
        aggregator = ag;
        this.id = new BasicDBObject("_id", id);
    }

    public Group<T, R> addToSet(BasicDBObject param) {
        BasicDBObject o = new BasicDBObject("$addToSet", param);
        operators.add(o);
        return this;
    } //don't know what this actually should do???

    public Group<T, R> first(String name, Object p) {
        BasicDBObject o = new BasicDBObject(name, new BasicDBObject("$first", p));
        operators.add(o);
        return this;
    }

    public Group<T, R> last(String name, Object p) {
        BasicDBObject o = new BasicDBObject(name, new BasicDBObject("$last", p));
        operators.add(o);
        return this;
    }

    public Group<T, R> max(String name, Object p) {
        BasicDBObject o = new BasicDBObject(name, new BasicDBObject("$max", p));
        operators.add(o);
        return this;
    }

    public Group<T, R> min(String name, Object p) {
        BasicDBObject o = new BasicDBObject(name, new BasicDBObject("$min", p));
        operators.add(o);
        return this;
    }

    public Group<T, R> avg(String name, Object p) {
        BasicDBObject o = new BasicDBObject(name, new BasicDBObject("$avg", p));
        operators.add(o);
        return this;
    }

    public Group<T, R> push(String name, Object p) {
        BasicDBObject o = new BasicDBObject(name, new BasicDBObject("$push", p));
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
        BasicDBObject o = new BasicDBObject(name, new BasicDBObject("$sum", p));
        operators.add(o);
        return this;
    }

    public Group<T, R> sum(String name, String p) {
        return sum(name, (Object) p);
    }

    public Aggregator<T, R> end() {
        BasicDBObject params = new BasicDBObject();
        params.putAll((DBObject) id);
        for (BasicDBObject o : operators) {
            params.putAll((DBObject) o);
        }
        DBObject obj = new BasicDBObject("$group", params);
        aggregator.addOperator(obj);
        return aggregator;
    }

    public List<BasicDBObject> getOperators() {
        return operators;
    }
}
