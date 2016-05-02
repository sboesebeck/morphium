/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.test.mongo.suite.data;

import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.annotations.caching.WriteBuffer;
import de.caluga.morphium.driver.bson.MorphiumId;


/**
 * @author stephan
 */
@Entity
@Cache(maxEntries = 20000, strategy = Cache.ClearStrategy.LRU, syncCache = Cache.SyncCacheStrategy.CLEAR_TYPE_CACHE, timeout = 15000)
@WriteBuffer(timeout = 500, size = 100)
@WriteSafety(level = SafetyLevel.WAIT_FOR_ALL_SLAVES, timeout = 3000, waitForJournalCommit = true)
public class CachedObject {
    @Index
    private String value;
    @Index
    private int counter;

    @Id
    private MorphiumId id;

    public int getCounter() {
        return counter;
    }

    public MorphiumId getId() {
        return id;
    }

    public void setId(MorphiumId id) {
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
