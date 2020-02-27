package de.caluga.morphium.query;

import java.util.Iterator;
import java.util.List;

public class EmptyIterator<T> implements MorphiumIterator<T> {
    private Query<T> q;

    @Override
    public int getWindowSize() {
        return 0;
    }

    @Override
    public void setWindowSize(int sz) {

    }

    @Override
    public Query<T> getQuery() {
        return q;
    }

    @Override
    public void setQuery(Query<T> q) {
        this.q = q;
    }

    @Override
    public int getCurrentBufferSize() {
        return 0;
    }

    @Override
    public List<T> getCurrentBuffer() {
        return null;
    }

    @Override
    public long getCount() {
        return 0;
    }

    @Override
    public int getCursor() {
        return 0;
    }

    @Override
    public void ahead(int jump) {

    }

    @Override
    public void back(int jump) {

    }

    @Override
    public void setNumberOfPrefetchWindows(int n) {

    }

    @Override
    public int getNumberOfAvailableThreads() {
        return 0;
    }

    @Override
    public int getNumberOfThreads() {
        return 0;
    }

    @Override
    public boolean isMultithreaddedAccess() {
        return false;
    }

    @Override
    public void setMultithreaddedAccess(boolean mu) {

    }

    @Override
    public Iterator<T> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public T next() {
        return null;
    }
}
