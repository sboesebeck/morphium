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
    int timeout() default 60000;

    boolean clearOnWrite() default true;

    int maxEntries() default -1;

    //    boolean overridable() default false;

    ClearStrategy strategy() default ClearStrategy.FIFO;

    boolean readCache() default true;

    SyncCacheStrategy syncCache() default SyncCacheStrategy.NONE;

    enum ClearStrategy {LRU, FIFO, RANDOM}

    enum SyncCacheStrategy {NONE, CLEAR_TYPE_CACHE, REMOVE_ENTRY_FROM_TYPE_CACHE, UPDATE_ENTRY}

}