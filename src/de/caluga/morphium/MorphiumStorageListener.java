/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium;

import java.util.List;

/**
 * @author stephan
 *         These listeners will be informed about Storing _any_ object in morphium!
 */
public interface MorphiumStorageListener<T> {

    public void preStore(T r);

    public void postStore(T r);

    public void postRemove(T r);

    public void preDelete(T r);

    public void postDrop(Class<T> cls);

    public void preDrop(Class<T> cls);

    public void postListStore(List<T> lst);

    public void preListStore(List<T> lst);

    public void preRemove(Query<T> q);

    public void postRemove(Query<T> q);

    public void postLoad(T o);

}
