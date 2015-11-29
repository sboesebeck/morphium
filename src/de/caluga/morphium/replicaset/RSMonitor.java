package de.caluga.morphium.replicaset;

import com.mongodb.ServerAddress;
import de.caluga.morphium.Logger;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Used in a Thread or executor.
 * Checks for the replicaset status periodically.
 * Used in order to get number of currently active nodes and their state
 */
public class RSMonitor {
    private static Logger logger = new Logger(RSMonitor.class);
    private Morphium morphium;
    private final ScheduledThreadPoolExecutor executorService;
    private ReplicaSetStatus currentStatus;
    private int nullcounter = 0;

    public RSMonitor(Morphium morphium) {
        this.morphium = morphium;
        executorService = new ScheduledThreadPoolExecutor(1);
    }

    public void start() {
        execute();
        executorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                execute();
            }
        }, 1000, morphium.getConfig().getReplicaSetMonitoringTimeout(), TimeUnit.MILLISECONDS);
    }

    public void terminate() {
        executorService.shutdownNow();
    }

    public void execute() {
        try {
            if (logger.isDebugEnabled()) logger.debug("Getting RS-Status...");
            currentStatus = getReplicaSetStatus(true);
            if (currentStatus == null) {
                nullcounter++;
                if (logger.isDebugEnabled()) logger.debug("RS status is null! Counter " + nullcounter);
            } else {
                nullcounter = 0;
            }
            if (nullcounter > 10) {
                logger.error("Getting ReplicasetStatus failed 10 times... will gracefully exit thread");
                executorService.shutdownNow();
            }
        } catch (Exception ignored) {

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
    @SuppressWarnings("unchecked")
    public de.caluga.morphium.replicaset.ReplicaSetStatus getReplicaSetStatus(boolean full) {
        if (morphium.isReplicaSet()) {
            try {
//                DB adminDB = morphium.getDriver().("admin");
                MorphiumConfig config = morphium.getConfig();
//                if (config.getMongoAdminUser() != null) {
//                    if (!adminDB.authenticate(config.getMongoAdminUser(), config.getMongoAdminPwd().toCharArray())) {
//                        logger.error("Authentication as admin failed!");
//                        return null;
//                    }
//                }


                Map<String, Object> res = morphium.getDriver().getReplsetStatus();
                de.caluga.morphium.replicaset.ReplicaSetStatus status = morphium.getMapper().unmarshall(de.caluga.morphium.replicaset.ReplicaSetStatus.class, res);
                if (full) {
                    Map<String, Object> findMetaData = new HashMap<>();
                    List<Map<String, Object>> stats = morphium.getDriver().find("local", "system.replset", new HashMap<String, Object>(), null, null, 0, 10, 10, null, findMetaData);
                    if (stats == null || stats.size() == 0) {
                        logger.info("could not get replicaset status");
                    } else {
                        Map<String, Object> stat = stats.get(0);
//                    DBCursor rpl = morphium.getDriver().getDB("local").getCollection("system.replset").find();
//                    DBObject stat = rpl.next(); //should only be one, i think
//                    rpl.close();
                        ReplicaSetConf cfg = morphium.getMapper().unmarshall(ReplicaSetConf.class, stat);
                        List<Object> mem = cfg.getMemberList();
                        List<ConfNode> cmembers = new ArrayList<>();

                        for (Object o : mem) {
//                        DBObject dbo = (DBObject) o;
                            ConfNode cn = (ConfNode) o;// objectMapper.unmarshall(ConfNode.class, dbo);
                            cmembers.add(cn);
                        }
                        cfg.setMembers(cmembers);
                        status.setConfig(cfg);
                    }
                }
                //de-referencing list
                List lst = status.getMembers();
                List<ReplicaSetNode> members = new ArrayList<>();
                if (lst != null)
                for (Object l : lst) {
//                    DBObject o = (DBObject) l;
                    ReplicaSetNode n = (ReplicaSetNode) l;//objectMapper.unmarshall(ReplicaSetNode.class, o);
                    members.add(n);
                }
                status.setMembers(members);


                //getting max limits
                //	"maxBsonObjectSize" : 16777216,
//                "maxMessageSizeBytes" : 48000000,
//                        "maxWriteBatchSize" : 1000,
                return status;
            } catch (Exception e) {
                logger.warn("Could not get Replicaset status: " + e.getMessage(), e);
                logger.warn("Tried connection to: ");
                for (ServerAddress adr : morphium.getConfig().getAdr()) {
                    logger.warn("   " + adr.getHost() + ":" + adr.getPort());
                }
            }
        }
        return null;
    }


    public ReplicaSetStatus getCurrentStatus() {
        return currentStatus;
    }
}
