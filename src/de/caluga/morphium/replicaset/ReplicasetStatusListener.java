package de.caluga.morphium.replicaset;

import de.caluga.morphium.Morphium;

import java.util.List;

public interface ReplicasetStatusListener {

    void gotNewStatus(Morphium morphium, ReplicaSetStatus status);

    /**
     * infoms, if replicaset status could not be optained.
     *
     * @param numErrors - how many errors getting the status in a row we already havei
     */
    void onGetStatusFailure(Morphium morphium, int numErrors);

    /**
     * called, if the ReplicasetMonitor aborts due to too many errors
     *
     * @param numErrors - number of errors occured
     */
    void onMonitorAbort(Morphium morphium, int numErrors);

    /**
     * @param hostsDown       - list of hostnamed not up
     * @param currentHostSeed - list of currently available replicaset members
     */
    void onHostDown(Morphium morphium, List<String> hostsDown, List<String> currentHostSeed);
}
