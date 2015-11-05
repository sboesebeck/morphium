package de.caluga.morphium.driver;/**
 * Created by stephan on 05.11.15.
 */

import java.util.Map;

/**
 * TODO: Add Documentation here
 **/
public class WriteConcern {
    //number of nodes data is written to
    //0: no error handling
    //1: master only
    //>1: number of nodes
    //-1: all available Replicaset Nodes
    //-2: Majority
    private int w;

    //journaled
    private boolean j;

    //
    private Map<String, String> tagSet;

    /**
     * write timeout
     */
    private int wtimeout;

}
