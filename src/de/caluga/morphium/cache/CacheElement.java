/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium.cache;

import java.util.List;

/**
 * @author stephan
 */
public class CacheElement<T> {

    private long created;
    private List<T> found;
    private long lru;

    public CacheElement(List<T> found) {
        this.found = found;
        created = System.currentTimeMillis();
    }

    public List<T> getFound() {
        lru = System.currentTimeMillis();

        return found;


    }

    public void setFound(List<T> found) {
        this.found = found;

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
