package de.caluga.morphium;

import de.caluga.morphium.query.Query;

/**
 * User: Stephan Bösebeck
 * Date: 01.08.12
 * Time: 09:46
 * <p/>
 * Listener for profiling
 */
public interface ProfilingListener {
    void readAccess(Query query, long time, ReadAccessType t);

    void writeAccess(Class type, Object o, long time, boolean isNew, WriteAccessType t);
}
