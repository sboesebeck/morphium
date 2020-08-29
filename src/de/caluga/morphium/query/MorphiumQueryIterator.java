package de.caluga.morphium.query;

public interface MorphiumQueryIterator<T> extends MorphiumIterator<T> {

    Query<T> getQuery();

    void setQuery(Query<T> q);


}
