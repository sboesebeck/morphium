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
    enum UpdateTypes {
        SET, UNSET, PUSH, PULL, INC, DEC, MUL, MIN, MAX, RENAME, POP, CURRENTDATE, CUSTOM,
    }

    void preStore(Morphium m, T r, boolean isNew) throws MorphiumAccessVetoException;

    void preStore(Morphium m, Map<T, Boolean> isNew) throws MorphiumAccessVetoException;


    void postStore(Morphium m, T r, boolean isNew);

    void postStore(Morphium m, Map<T, Boolean> isNew);

    void preRemove(Morphium m, Query<T> q) throws MorphiumAccessVetoException;

    void preRemove(Morphium m, T r) throws MorphiumAccessVetoException;

    void postRemove(Morphium m, T r);

    void postRemove(Morphium m, List<T> lst);

    void postDrop(Morphium m, Class<? extends T> cls);

    void preDrop(Morphium m, Class<? extends T> cls) throws MorphiumAccessVetoException;

    void postRemove(Morphium m, Query<T> q);

    void postLoad(Morphium m, T o);

    void postLoad(Morphium m, List<T> o);

    void preUpdate(Morphium m, Class<? extends T> cls, Enum updateType) throws MorphiumAccessVetoException;

    void postUpdate(Morphium m, Class<? extends T> cls, Enum updateType);

}
