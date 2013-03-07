package de.caluga.morphium;

import de.caluga.morphium.query.Query;

import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 07.03.13
 * Time: 10:07
 * <p/>
 * TODO: Add documentation here
 */
public interface MorphiumReadListener<T> {
    public void preRead(Query<T> q) throws MorphiumAccessVetoException;

    public void postRead(Query<T> q, List<T> result, boolean fromCache) throws MorphiumAccessVetoException;
}
