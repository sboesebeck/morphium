package de.caluga.morphium.query;

import java.util.Map;

public interface MorphiumQueryIterator<T> extends MorphiumIterator<T> {

    Query<T> getQuery();

    void setQuery(Query<T> q);


    Map<String, Object> nextMap();
}
