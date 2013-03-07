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
        SET, UNSET, PUSH, PULL, INC, DEC,
    }

    public void preStore(Morphium m, T r, boolean isNew) throws MorphiumAccessVetoException;

    public void postStore(Morphium m, T r, boolean isNew) throws MorphiumAccessVetoException;

    public void postRemove(Morphium m, T r) throws MorphiumAccessVetoException;

    public void preDelete(Morphium m, T r) throws MorphiumAccessVetoException;

    public void postDrop(Morphium m, Class<? extends T> cls) throws MorphiumAccessVetoException;

    public void preDrop(Morphium m, Class<? extends T> cls) throws MorphiumAccessVetoException;

    public void preRemove(Morphium m, Query<T> q) throws MorphiumAccessVetoException;

    public void postRemove(Morphium m, Query<T> q) throws MorphiumAccessVetoException;

    public void postLoad(Morphium m, T o) throws MorphiumAccessVetoException;

    public void preUpdate(Morphium m, Class<? extends T> cls, Enum updateType) throws MorphiumAccessVetoException;

    public void postUpdate(Morphium m, Class<? extends T> cls, Enum updateType) throws MorphiumAccessVetoException;

}
