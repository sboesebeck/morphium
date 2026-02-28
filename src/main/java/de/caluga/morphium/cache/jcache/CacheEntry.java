package de.caluga.morphium.cache.jcache;

/**
 * Created with IntelliJ IDEA.
 * User: stephan
 * Date: 15.04.14
 * Time: 10:06
 * To change this template use File | Settings | File Templates.
 */
public class CacheEntry<T> {
    private T result;
    private Object key;
    private Class<? extends T> type;
    private final long created;
    private long lru;


    @SuppressWarnings("unchecked")
    public CacheEntry(T result, Object key) {
        this.result = result;
        this.key = key;
        type = (Class<? extends T>) result.getClass();
        created = System.currentTimeMillis();
    }

    @SuppressWarnings("unused")
    public Class<? extends T> getType() {
        return type;
    }

    public void setType(Class<? extends T> type) {
        this.type = type;
    }

    @SuppressWarnings("unused")
    public T getResult() {
        lru = System.currentTimeMillis();
        return result;
    }

    public void setResult(T result) {
        this.result = result;
    }

    @SuppressWarnings("unused")
    public Object getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getCreated() {
        return created;
    }

    public long getLru() {
        return lru;
    }

    public void setLru(long lru) {
        this.lru = lru;
    }
}
