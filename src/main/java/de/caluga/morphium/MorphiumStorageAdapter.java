package de.caluga.morphium;

import de.caluga.morphium.query.Query;

import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 06.03.13
 * Time: 08:13
 * <p>
 * the adapter for the storage listener
 */
public abstract class MorphiumStorageAdapter<T> implements MorphiumStorageListener<T> {

    @Override
    public void preStore(Morphium m, Map<T, Boolean> isNew) throws MorphiumAccessVetoException {
    }

    @Override
    public void preStore(Morphium m, T r, boolean isNew) throws MorphiumAccessVetoException {
    }

    @Override
    public void preDrop(Morphium m, Class<? extends T> cls) throws MorphiumAccessVetoException {
    }

    @Override
    public void preRemove(Morphium m, Query<T> q) throws MorphiumAccessVetoException {
    }

    @Override
    public void preUpdate(Morphium m, Class<? extends T> cls, Enum updateType) throws MorphiumAccessVetoException {
    }

    @Override
    public void postStore(Morphium m, Map<T, Boolean> isNew) {
    }

    @Override
    public void postLoad(Morphium m, List<T> o) {
    }

    @Override
    public void postRemove(Morphium m, List<T> toRemove) {
    }

    @Override
    public void postStore(Morphium m, T r, boolean isNew) {
    }

    @Override
    public void postRemove(Morphium m, T r) {
    }

    @Override
    public void preRemove(Morphium m, T r) {
    }

    @Override
    public void postDrop(Morphium m, Class<? extends T> cls) {
    }

    @Override
    public void postRemove(Morphium m, Query<T> q) {
    }

    @Override
    public void postLoad(Morphium m, T o) {
    }

    @Override
    public void postUpdate(Morphium m, Class<? extends T> cls, Enum updateType) {
    }
}
