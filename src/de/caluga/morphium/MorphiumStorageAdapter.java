package de.caluga.morphium;

import de.caluga.morphium.query.Query;

/**
 * User: Stephan Bösebeck
 * Date: 06.03.13
 * Time: 08:13
 * <p/>
 * TODO: Add documentation here
 */
public abstract class MorphiumStorageAdapter<T> implements MorphiumStorageListener<T> {
    @Override
    public void preStore(Morphium m, T r, boolean isNew) {
    }

    @Override
    public void postStore(Morphium m, T r, boolean isNew) {
    }

    @Override
    public void postRemove(Morphium m, T r) {
    }

    @Override
    public void preDelete(Morphium m, T r) {
    }

    @Override
    public void postDrop(Morphium m, Class<? extends T> cls) {
    }

    @Override
    public void preDrop(Morphium m, Class<? extends T> cls) {
    }

    @Override
    public void preRemove(Morphium m, Query<T> q) {
    }

    @Override
    public void postRemove(Morphium m, Query<T> q) {
    }

    @Override
    public void postLoad(Morphium m, T o) {
    }

    @Override
    public void preUpdate(Morphium m, Class<? extends T> cls, Enum updateType) {
    }

    @Override
    public void postUpdate(Morphium m, Class<? extends T> cls, Enum updateType) {
    }
}
