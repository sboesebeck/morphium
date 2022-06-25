package de.caluga.morphium.driver;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SingleElementCursor extends MorphiumCursor {
    private Map<String, Object> element;
    private int idx = 0;

    public SingleElementCursor(Map<String, Object> element) {
        this.element = element;
    }

    @Override
    public List<Map<String, Object>> getBatch() {
        return Arrays.asList(element);
    }

    @Override
    public boolean hasNext() throws MorphiumDriverException {
        return idx == 0;
    }

    @Override
    public Map<String, Object> next() throws MorphiumDriverException {
        idx++;
        return element;
    }

    @Override
    public void close() throws MorphiumDriverException {

    }

    @Override
    public int available() {
        return 1 - idx;
    }

    @Override
    public List<Map<String, Object>> getAll() throws MorphiumDriverException {
        return Arrays.asList(element);
    }

    @Override
    public void ahead(int skip) throws MorphiumDriverException {

    }

    @Override
    public void back(int jump) throws MorphiumDriverException {

    }

    @Override
    public int getCursor() {
        return idx;
    }
}
