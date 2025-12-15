package de.caluga.morphium;

/**
 * User: Stephan Bösebeck
 * Date: 01.08.12
 * Time: 10:43
 * <p>
 * profiling write access types
 */
public enum WriteAccessType {
    SINGLE_DELETE, BULK_DELETE, SINGLE_INSERT, SINGLE_UPDATE, BULK_UPDATE, BULK_INSERT, DROP, ENSURE_INDEX
}
