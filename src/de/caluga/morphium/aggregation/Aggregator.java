package de.caluga.morphium.aggregation;

import com.mongodb.DBObject;
import de.caluga.morphium.query.Query;

import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 30.08.12
 * Time: 16:23
 * <p/>
 * TODO: Add documentation here
 */
public interface Aggregator<T> {

    public Aggregator project(Map<String, String> m);  //field -> other field, field -> 0,1

    public Aggregator project(String... m);    //field:1

    public Aggregator project(DBObject m);    //custom

    public Aggregator match(Query<T> q);

    public Aggregator limit(int num);

    public Aggregator skip(int num);

    public Aggregator unwind(String listField);
    //Grouping is complex...


    public Aggregator<T> sort(String... prefixed);

    public Aggregator<T> sort(Map<String, Integer> sort);


}
