/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.test.mongo.suite;

import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.annotations.caching.WriteBuffer;
import org.bson.types.ObjectId;

/**
 * @author stephan
 */
@Cache(clearOnWrite = true, maxEntries = 20000, readCache = true, strategy = Cache.ClearStrategy.LRU, syncCache = Cache.SyncCacheStrategy.CLEAR_TYPE_CACHE, timeout = 5000)
@WriteBuffer(value = true, timeout = 10000, size = 100)
@Entity
@WriteSafety(level = SafetyLevel.WAIT_FOR_ALL_SLAVES, timeout = 0, waitForJournalCommit = true)
public class CachedObject {
    @Index
    private String value;
    @Index
    private int counter;

    @Id
    private ObjectId id;

    public int getCounter() {
        return counter;
    }

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public void setCounter(int counter) {
        this.counter = counter;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }


    public String toString() {
        return "Counter: " + counter + " Value: " + value + " MongoId: " + id;
    }


}
