package de.caluga.morphium.replicaset;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.commands.FindCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Used in a Thread or executor.
 * Checks for the replicaset status periodically.
 * Used in order to get number of currently active nodes and their state
 */
@SuppressWarnings({"WeakerAccess", "BusyWait"})
public class RSMonitor {
    private static final Logger logger = LoggerFactory.getLogger(RSMonitor.class);
    private ScheduledThreadPoolExecutor executorService;
    private Morphium morphium;
    private ReplicaSetStatus currentStatus;
    private int nullcounter = 0;
    private final List<ReplicasetStatusListener> listeners = new Vector<>();

    public RSMonitor(Morphium morphium) {
        this.morphium = morphium;
        executorService = new ScheduledThreadPoolExecutor(1, Thread.ofVirtual().name("rsMonitor-", 0).factory());
    }

    public void start() {
        executorService.scheduleWithFixedDelay(this::execute, 1000, morphium.getConfig().getReplicaSetMonitoringTimeout(), TimeUnit.MILLISECONDS);
        execute();
    }


    public void addListener(ReplicasetStatusListener lst) {
        listeners.add(lst);
    }

    public void removeListener(ReplicasetStatusListener lst) {
        listeners.remove(lst);
    }

    public void terminate() {
        executorService.shutdownNow();
        while (!executorService.isShutdown()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                //ignored
            }
        }
    }

    public void execute() {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Getting RS-Status...");
            }
            currentStatus = getReplicaSetStatus(true);
            if (currentStatus == null) {
                nullcounter++;
                if (logger.isDebugEnabled()) {
                    logger.debug("RS status is null! Counter " + nullcounter);
                }
                for (ReplicasetStatusListener l : listeners) l.onGetStatusFailure(morphium, nullcounter);
            } else {
                nullcounter = 0;
            }
            if (nullcounter > 10) {
                logger.error("Getting ReplicasetStatus failed 10 times... will gracefully exit thread");
                executorService.shutdownNow();
                for (ReplicasetStatusListener l : listeners) l.onMonitorAbort(morphium, nullcounter);

            }
            if (currentStatus != null) {
                for (ReplicasetStatusListener l : listeners) {
                    l.gotNewStatus(morphium, currentStatus);
                }

                for (ReplicaSetNode n : currentStatus.getMembers()) {
                    if (morphium.getConfig().getHostSeed().contains(n.getName())) {
                        logger.debug("Found host in config " + n.getName());
                    } else {
                        morphium.getConfig().getHostSeed().add(n.getName());
                    }
                }
                List<String> hostsNotFound = new ArrayList<>();
                for (String host : morphium.getConfig().getHostSeed()) {
                    boolean found = false;
                    for (ReplicaSetNode n : currentStatus.getMembers()) {
                        if (n.getName().equals(host)) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        hostsNotFound.add(host);
                    }
                }
                if (!hostsNotFound.isEmpty()) {
                    morphium.getConfig().getHostSeed().removeAll(hostsNotFound);
                    for (ReplicasetStatusListener l : listeners)
                        l.onHostDown(morphium, hostsNotFound, morphium.getConfig().getHostSeed());
                }
            }
        } catch (Exception ignored) {
            // ignored
        }
    }


    /**
     * get the current replicaset status - issues the replSetGetStatus command to mongo
     * if full==true, also the configuration is read. This method is called with full==false for every write in
     * case a Replicaset is configured to find out the current number of active nodes
     *
     * @param full - if true- return full status
     * @return status
     */
    @SuppressWarnings({"unchecked", "CommentedOutCode"})
    public de.caluga.morphium.replicaset.ReplicaSetStatus getReplicaSetStatus(boolean full) {
//
//         if (morphium.isReplicaSet()) {
//             try {
//                 Map<String, Object> res = morphium.getDriver().getReplsetStatus();
//                 de.caluga.morphium.replicaset.ReplicaSetStatus status = morphium.getMapper().deserialize(de.caluga.morphium.replicaset.ReplicaSetStatus.class, res);
//                 if (status == null) return null;
//                 if (full) {
//
//                     FindCommand settings = new FindCommand(morphium.getDriver().getPrimaryConnection(null))
//                             .setDb("local").setColl("system.replset").setBatchSize(10).setLimit(10);
//                     List<Map<String, Object>> stats = settings.execute();
//                     settings.releaseConnection();
//                     if (stats == null || stats.isEmpty()) {
//                         logger.debug("could not get replicaset status");
//                     } else {
//                         Map<String, Object> stat = stats.get(0);
//                         //                    DBCursor rpl = morphium.getDriver().getDB("local").getCollection("system.replset").find();
//                         //                    DBObject stat = rpl.next(); //should only be one, i think
//                         //                    rpl.close();
//                         ReplicaSetConf cfg = morphium.getMapper().deserialize(ReplicaSetConf.class, stat);
// //                        List<Object> mem = cfg.getMemberList();
// //                        List<ConfNode> cmembers = new ArrayList<>();
// //
// //                        for (Object o : mem) {
// //                            //                        DBObject dbo = (DBObject) o;
// //                            ConfNode cn = (ConfNode) o;// objectMapper.deserialize(ConfNode.class, dbo);
// //                            cmembers.add(cn);
// //                        }
// //                        cfg.setMembers(cmembers);
//                         status.setConfig(cfg);
//                     }
//                 }
//                 //de-referencing list
//                 List lst = status.getMembers();
//                 List<ReplicaSetNode> members = new ArrayList<>();
//                 if (lst != null) {
//                     for (Object l : lst) {
//                         //                    DBObject o = (DBObject) l;
//                         ReplicaSetNode n = (ReplicaSetNode) l;//objectMapper.deserialize(ReplicaSetNode.class, o);
//                         members.add(n);
//                     }
//                 }
//                 status.setMembers(members);
//
//
//                 //getting max limits
//                 //	"maxBsonObjectSize" : 16777216,
//                 //                "maxMessageSizeBytes" : 48000000,
//                 //                        "maxWriteBatchSize" : 1000,
//                 return status;
//             } catch (Exception e) {
//                 if (e.getMessage() == null) {
//                     throw new RuntimeException(e);
//                 }
//                 if (e.getMessage().contains(" 'not running with --replSet'")) {
//                     logger.warn("Mongo not configured for replicaset! Disabling monitoring for now");
//                     morphium.getConfig().setReplicasetMonitoring(false);
//                     terminate();
//                 } else if (e.getMessage().contains("user is not allowed to do action")) {
//                     logger.warn("permission denied for replicaset status! Disabling monitoring for now");
//                     morphium.getConfig().setReplicasetMonitoring(false);
//                     terminate();
//                 } else if (e.getMessage().contains("replSetGetStatus is not supported")) {
//                     logger.warn("Replicaset check not possible - not supported!");
//                     morphium.getConfig().setReplicasetMonitoring(false);
//                     terminate();
//                 } else {
//                     logger.warn("Could not get Replicaset status: " + e.getMessage(), e);
//                     logger.warn("Tried connection to: ");
//                     for (String adr : morphium.getConfig().getHostSeed()) {
//                         logger.warn("   " + adr);
//                     }
//                 }
//             }
//         }
        return null;
    }


    public ReplicaSetStatus getCurrentStatus() {
        return currentStatus;
    }
}
