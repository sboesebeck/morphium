/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium;

import de.caluga.morphium.query.Query;

/**
 * @author stephan
 *         These listeners will be informed about Storing _any_ object in morphium!
 */
public interface MorphiumStorageListener<T> {
    public enum UpdateTypes {
        SET, UNSET, PUSH, PULL, INC, DEC;
    }

    public void preStore(Morphium m, T r, boolean isNew);

    public void postStore(Morphium m, T r, boolean isNew);

    public void postRemove(Morphium m, T r);

    public void preDelete(Morphium m, T r);

    public void postDrop(Morphium m, Class<? extends T> cls);

    public void preDrop(Morphium m, Class<? extends T> cls);

    public void preRemove(Morphium m, Query<T> q);

    public void postRemove(Morphium m, Query<T> q);

    public void postLoad(Morphium m, T o);

    public void preUpdate(Morphium m, Class<? extends T> cls, Enum updateType);

    public void postUpdate(Morphium m, Class<? extends T> cls, Enum updateType);

}
