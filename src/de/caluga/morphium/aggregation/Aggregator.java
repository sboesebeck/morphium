package de.caluga.morphium.aggregation;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.query.Query;

import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 30.08.12
 * Time: 16:23
 * <p/>
 * Aggregator Framework:
 * represents the aggregator of Mongo 2.2.x.
 */
public interface Aggregator<T, R> {

    public void setMorphium(Morphium m);

    public Morphium getMorphium();

    public void setSearchType(Class<? extends T> type);

    public Class<? extends T> getSearchType();

    public void setResultType(Class<? extends R> type);

    public Class<? extends R> getResultType();

    public Aggregator<T, R> project(Map<String, Object> m);  //field -> other field, field -> 0,1

    public Aggregator<T, R> project(String... m);    //field:1

    public Aggregator<T, R> project(BasicDBObject m);    //custom

    public Aggregator<T, R> match(Query<T> q);

    public Aggregator<T, R> limit(int num);

    public Aggregator<T, R> skip(int num);

    public Aggregator<T, R> unwind(String listField);

    public Aggregator<T, R> sort(String... prefixed);

    public Aggregator<T, R> sort(Map<String, Integer> sort);

    public Group<T, R> group(BasicDBObject id);

    public Group<T, R> group(Map<String, String> idSubObject);

    public Group<T, R> group(String id);

    public List<DBObject> toAggregationList();

    public void addOperator(DBObject o);

    public List<R> aggregate();


}
