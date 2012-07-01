/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium;

/**
 * @author stephan
 *         These listeners will be informed about Storing _any_ object in morphium!
 */
public interface MorphiumStorageListener<T> {
    public enum UpdateTypes {
        SET, UNSET, PUSH, PULL, INC, DEC;
    }

    public void preStore(T r, boolean isNew);

    public void postStore(T r, boolean isNew);

    public void postRemove(T r);

    public void preDelete(T r);

    public void postDrop(Class<T> cls);

    public void preDrop(Class<T> cls);

    public void preRemove(Query<T> q);

    public void postRemove(Query<T> q);

    public void postLoad(T o);

    public void preUpdate(Class<T> cls, Enum updateType);

    public void postUpdate(Class<T> cls, Enum updateType);

}
