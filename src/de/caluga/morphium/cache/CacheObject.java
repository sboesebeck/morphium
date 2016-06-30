package de.caluga.morphium.cache;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: stephan
 * Date: 15.04.14
 * Time: 10:06
 * To change this template use File | Settings | File Templates.
 */
public class CacheObject<T> {
    private List<T> result;
    private String key;
    private Class<? extends T> type;

    @SuppressWarnings("unused")
    public Class<? extends T> getType() {
        return type;
    }

    public void setType(Class<? extends T> type) {
        this.type = type;
    }

    @SuppressWarnings("unused")
    public List<T> getResult() {
        return result;
    }

    public void setResult(List<T> result) {
        this.result = result;
    }

    @SuppressWarnings("unused")
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}
