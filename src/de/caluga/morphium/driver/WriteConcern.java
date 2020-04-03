package de.caluga.morphium.driver;/**
 * Created by stephan on 05.11.15.
 */

import java.util.concurrent.TimeUnit;

/**
 * define how secure the write should be. most important the w value which states the number of nodes written to:
 * 0: no error handling
 * 1: master only
 * >1: number of nodes
 * -1: all available replicase nodes
 * -2: majority
 **/
public class WriteConcern {
    //number of nodes data is written to
    //0: no error handling
    //1: master only
    //>1: number of nodes
    //-1: all available Replicaset Nodes
    //-2: Majority
    private final int w;

    //journaled
    private final boolean j;
    private final boolean fsync;

    /**
     * write timeout
     */
    private final int wtimeout;

    private WriteConcern(int w, boolean fsync, boolean j, int wtimeout) {
        this.w = w;
        this.j = j;
        this.wtimeout = wtimeout;
        this.fsync = fsync;
    }

    public static WriteConcern getWc(int w, boolean fsync, boolean j, int wtimeout) {
        return new WriteConcern(w, fsync, j, wtimeout);
    }


    public com.mongodb.WriteConcern toMongoWriteConcern() {
        com.mongodb.WriteConcern wc = getW() > 0 ? com.mongodb.WriteConcern.ACKNOWLEDGED : com.mongodb.WriteConcern.UNACKNOWLEDGED;
        if (getW() > 0) {
            if (isJ()) {
                wc = wc.withJournal(isJ());
            } else if (isFsync()) {
                // do not set if journal is already set - otherwise AWS DocumentDB will throw an error. fsync must be null not only false
                wc = wc.withFsync(isFsync());
            }
            if (getWtimeout() > 0) {
                wc.withWTimeout(getWtimeout(), TimeUnit.MILLISECONDS);
            }
        }
        return wc;
    }

    public int getW() {
        return w;
    }

    public boolean isJ() {
        return j;
    }

    public boolean isFsync() {
        return fsync;
    }

    public int getWtimeout() {
        return wtimeout;
    }

}
