/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium;

import de.caluga.morphium.query.Query;

import java.util.List;
import java.util.Map;

/**
 * @author stephan
 *         These listeners will be informed about Storing _any_ object in morphium!
 */
public interface MorphiumStorageListener<T> {
    void preStore(Morphium m, T r, boolean isNew) throws MorphiumAccessVetoException;

    void preStore(Morphium m, Map<T, Boolean> isNew) throws MorphiumAccessVetoException;

    @SuppressWarnings("UnusedParameters")
    void postStore(Morphium m, T r, boolean isNew);

    @SuppressWarnings("UnusedParameters")
    void postStore(Morphium m, Map<T, Boolean> isNew);

    @SuppressWarnings("UnusedParameters")
    void preRemove(Morphium m, Query<T> q) throws MorphiumAccessVetoException;

    @SuppressWarnings({"EmptyMethod", "UnusedParameters"})
    void preRemove(Morphium m, T r) throws MorphiumAccessVetoException;

    @SuppressWarnings("UnusedParameters")
    void postRemove(Morphium m, T r);

    @SuppressWarnings("UnusedParameters")
    void postRemove(Morphium m, List<T> lst);

    @SuppressWarnings("UnusedParameters")
    void postDrop(Morphium m, Class<? extends T> cls);

    @SuppressWarnings("UnusedParameters")
    void preDrop(Morphium m, Class<? extends T> cls) throws MorphiumAccessVetoException;

    @SuppressWarnings("UnusedParameters")
    void postRemove(Morphium m, Query<T> q);

    @SuppressWarnings({"EmptyMethod", "UnusedParameters"})
    void postLoad(Morphium m, T o);

    @SuppressWarnings({"EmptyMethod", "UnusedParameters"})
    void postLoad(Morphium m, List<T> o);

    @SuppressWarnings("UnusedParameters")
    void preUpdate(Morphium m, Class<? extends T> cls, Enum updateType) throws MorphiumAccessVetoException;

    @SuppressWarnings("UnusedParameters")
    void postUpdate(Morphium m, Class<? extends T> cls, Enum updateType);

    enum UpdateTypes {
        SET, UNSET, PUSH, PULL, INC, @SuppressWarnings("unused")DEC, MUL, MIN, MAX, RENAME, POP, CURRENTDATE, CUSTOM,
    }

}
