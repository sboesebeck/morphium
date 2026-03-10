package de.caluga.morphium.driver;

import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SingleBatchCursor extends MorphiumCursor {
    private Iterator<Map<String, Object>> iterator;
    private int idx = 0;

    public SingleBatchCursor(List<Map<String, Object>> batch) {
        super.setBatch(batch);
        iterator = batch.iterator();
    }

    @Override
    public synchronized boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public synchronized Map<String, Object> next() {
        idx++;

        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    @Override
    public void close() {
    }

    @Override
    public int available() {
        return getBatch().size() - getCursor();
    }

    @Override
    public List<Map<String, Object>> getAll() throws MorphiumDriverException {
        return getBatch();
    }

    @Override
    public synchronized void ahead(int skip) throws MorphiumDriverException {
        if (getBatch() == null) {
            throw new IllegalArgumentException("cannot jump that far");
        }

        if (skip + idx > getBatch().size()) {
            throw new IllegalArgumentException("cannot jump that far");
        }

        for (int i = 0; i < skip; i++) {
            next();
        }
    }

    @Override
    public synchronized void back(int jump) throws MorphiumDriverException {
        throw new IllegalArgumentException("cannot jump back");
    }

    @Override
    public int getCursor() {
        return idx;
    }

    @Override
    public MongoConnection getConnection() {
        return null;
    }

    @Override
    public Iterator<Map<String, Object>> iterator() {
        return this;
    }
}
