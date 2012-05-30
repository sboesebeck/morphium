/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium.annotations.caching;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;


/**
 * Define the caching configuration of a given MongoDbObject. The options are as follows:
 * timeout: int timout in ms when a cache entry becomes invalid
 * maxEntries: default -1 means unlimited (Only limited by Memory, uses SoftReferences!)
 * clearOnWrite: clear cache if one element is written
 * strategy: when using fixed number of entries, define how to remove additional entries
 * readCache: use cache for reading
 * writeCache: use cache for writing (schedule write as background job), default false
 *
 * @author stephan
 */
@Target({TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface Cache {
    enum ClearStrategy {LRU, FIFO, RANDOM}

    int timeout() default 60000;

    boolean overridable() default true;

    boolean clearOnWrite() default true;

    int maxEntries() default -1;

    ClearStrategy strategy() default ClearStrategy.FIFO;

    boolean writeCache() default false;

    boolean readCache() default true;

    /**
     * if cache synchronizer is used, tell it to synchronize the cache for this type
     * @return
     */
    boolean autoSyncCache() default false;
}