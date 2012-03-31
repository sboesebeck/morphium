/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium;

import java.util.List;

/**
 * @author stephan
 */
public class StorageAdapter<T> implements MorphiumStorageListener<T> {

    @Override
    public void preStore(T r) {
    }

    @Override
    public void postStore(T r) {
    }

    @Override
    public void postRemove(T r) {
    }

    @Override
    public void preDelete(T r) {
    }

    @Override
    public void postDrop(Class<T> cls) {
    }

    @Override
    public void preDrop(Class<T> cls) {
    }

    @Override
    public void postListStore(List<T> lst) {

    }

    @Override
    public void preListStore(List<T> lst) {
    }

    @Override
    public void preRemove(Query<T> q) {
    }

    @Override
    public void postRemove(Query<T> q) {
    }

    @Override
    public void postLoad(T o) {
    }


}
