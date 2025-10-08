package de.caluga.morphium.driver;/**
 * Created by stephan on 05.11.15.
 */

import java.util.Map;
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
    private final boolean j;

    /**
     * write timeout
     */
    private final int wtimeout;

    private WriteConcern(int w, boolean j, int wtimeout) {
        this.w = w;
        this.j = j;
        this.wtimeout = wtimeout;
    }

    public static WriteConcern getWc(int w, boolean j, int wtimeout) {
        return new WriteConcern(w, j, wtimeout);
    }



    public int getW() {
        return w;
    }

    public int getWtimeout() {
        return wtimeout;
    }

    public Map<String, Object> asMap() {
        Doc wc = Doc.of("j", j, "wtimeout", wtimeout);

        if (w < 0) {
            wc.put("w", "majority");
        } else if (w > 0) {
            wc.put("w", w);
        }

        return wc;
    }

}
