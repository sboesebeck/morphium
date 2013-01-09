/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium;

import de.caluga.morphium.query.Query;

/**
 * @author stephan
 */
@SuppressWarnings("UnusedDeclaration")
public class StorageAdapter<T> implements MorphiumStorageListener<T> {

    @Override
    public void preStore(T r, boolean isNew) {
    }

    @Override
    public void postStore(T r, boolean isNew) {
    }

    @Override
    public void postRemove(T r) {
    }

    @Override
    public void preDelete(T r) {
    }

    @Override
    public void postDrop(Class<? extends T> cls) {
    }

    @Override
    public void preDrop(Class<? extends T> cls) {
    }

//    @Override
//    public void postListStore(List<T> lst,Map<Object,Boolean> isNew) {
//
//    }
//
//    @Override
//    public void preListStore(List<T> lst, Map<Object,Boolean> isNew) {
//    }

    @Override
    public void preRemove(Query<T> q) {
    }

    @Override
    public void postRemove(Query<T> q) {
    }

    @Override
    public void postLoad(T o) {
    }

    @Override
    public void preUpdate(Class<? extends T> cls, Enum updateType) {
    }

    @Override
    public void postUpdate(Class<? extends T> cls, Enum updateType) {
    }


}
