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
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Map<String, Object> next() {
        idx++;
        return iterator.next();
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
    public void ahead(int skip) throws MorphiumDriverException {
        if (getBatch() == null) throw new IllegalArgumentException("cannot jump that far");
        if (skip + idx > getBatch().size()) {
            throw new IllegalArgumentException("cannot jump that far");
        }
        for (int i = 0; i < skip; i++) next();
    }

    @Override
    public void back(int jump) throws MorphiumDriverException {
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
