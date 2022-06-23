package de.caluga.morphium.driver;

import java.util.List;
import java.util.Map;

public abstract class MorphiumCursorAdapter extends MorphiumCursor {
    @Override
    public boolean hasNext() throws MorphiumDriverException {
        return false;
    }

    @Override
    public Map<String, Object> next() throws MorphiumDriverException {
        return null;
    }

    @Override
    public void close() throws MorphiumDriverException {

    }

    @Override
    public int available() {
        return 0;
    }

    @Override
    public List<Map<String, Object>> getAll() throws MorphiumDriverException {
        return null;
    }

    @Override
    public void ahead(int skip) throws MorphiumDriverException {

    }

    @Override
    public void back(int jump) throws MorphiumDriverException {

    }

    @Override
    public int getCursor() {
        return 0;
    }
}
