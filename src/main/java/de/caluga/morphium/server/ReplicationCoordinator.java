package de.caluga.morphium.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Coordinates replication between primary and secondary MorphiumServer nodes.
 * Tracks write sequence numbers and waits for replication acknowledgment.
 */
public class ReplicationCoordinator {

    private static final Logger log = LoggerFactory.getLogger(ReplicationCoordinator.class);

    // Current write sequence number (incremented on each write)
    private final AtomicLong writeSequence = new AtomicLong(0);

    // Map of secondary address -> last acknowledged sequence number
    private final Map<String, Long> secondaryProgress = new ConcurrentHashMap<>();

    // Map of secondary address -> last heartbeat time
    private final Map<String, Long> secondaryHeartbeats = new ConcurrentHashMap<>();

    // Lock and condition for waiting on replication
    private final ReentrantLock replicationLock = new ReentrantLock();
    private final Condition replicationCondition = replicationLock.newCondition();

    // Configuration
    private final int replicaSetSize;
    private final long defaultWriteConcernTimeoutMs;

    public ReplicationCoordinator(int replicaSetSize) {
        this(replicaSetSize, 10000); // Default 10 second timeout
    }

    public ReplicationCoordinator(int replicaSetSize, long defaultWriteConcernTimeoutMs) {
        this.replicaSetSize = replicaSetSize;
        this.defaultWriteConcernTimeoutMs = defaultWriteConcernTimeoutMs;
    }

    /**
     * Called after each write operation on the primary.
     * Returns the sequence number for this write.
     */
    public long recordWrite() {
        return writeSequence.incrementAndGet();
    }

    /**
     * Get the current write sequence number.
     */
    public long getCurrentSequence() {
        return writeSequence.get();
    }

    /**
     * Called by secondaries to report their replication progress.
     */
    public void reportProgress(String secondaryAddress, long sequenceNumber) {
        Long previous = secondaryProgress.put(secondaryAddress, sequenceNumber);
        secondaryHeartbeats.put(secondaryAddress, System.currentTimeMillis());

        if (previous == null || sequenceNumber > previous) {
            log.debug("Secondary {} reported progress: seq={}", secondaryAddress, sequenceNumber);

            // Signal any waiting threads
            replicationLock.lock();
            try {
                replicationCondition.signalAll();
            } finally {
                replicationLock.unlock();
            }
        }
    }

    /**
     * Wait for a write to be replicated to the specified number of nodes.
     *
     * @param sequenceNumber The sequence number of the write to wait for
     * @param w              Number of nodes that must acknowledge (including primary)
     * @param timeoutMs      Maximum time to wait in milliseconds (0 = use default)
     * @return true if replication was acknowledged, false if timeout
     */
    public boolean waitForReplication(long sequenceNumber, int w, long timeoutMs) {
        if (w <= 1) {
            // w=1 means primary only, no need to wait
            return true;
        }

        if (timeoutMs <= 0) {
            timeoutMs = defaultWriteConcernTimeoutMs;
        }

        int requiredSecondaries = w - 1; // Subtract 1 for primary
        long deadline = System.currentTimeMillis() + timeoutMs;

        replicationLock.lock();
        try {
            while (true) {
                int acknowledgedCount = countAcknowledgedSecondaries(sequenceNumber);

                if (acknowledgedCount >= requiredSecondaries) {
                    log.debug("Write seq={} acknowledged by {} secondaries", sequenceNumber, acknowledgedCount);
                    return true;
                }

                long remainingMs = deadline - System.currentTimeMillis();
                if (remainingMs <= 0) {
                    log.warn("Write concern timeout: seq={}, w={}, acknowledged={}/{}",
                            sequenceNumber, w, acknowledgedCount, requiredSecondaries);
                    return false;
                }

                try {
                    // Wait for progress reports or timeout - very short interval for fastest acknowledgment
                    replicationCondition.await(Math.min(remainingMs, 2), TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        } finally {
            replicationLock.unlock();
        }
    }

    /**
     * Wait for "majority" write concern.
     */
    public boolean waitForMajority(long sequenceNumber, long timeoutMs) {
        int majority = (replicaSetSize / 2) + 1;
        return waitForReplication(sequenceNumber, majority, timeoutMs);
    }

    /**
     * Count how many secondaries have acknowledged up to the given sequence.
     */
    private int countAcknowledgedSecondaries(long sequenceNumber) {
        int count = 0;
        long now = System.currentTimeMillis();
        long staleThreshold = 30000; // Consider secondary stale after 30s without heartbeat

        for (Map.Entry<String, Long> entry : secondaryProgress.entrySet()) {
            String secondary = entry.getKey();
            Long lastSeq = entry.getValue();
            Long lastHeartbeat = secondaryHeartbeats.get(secondary);

            // Check if secondary is still alive
            if (lastHeartbeat != null && (now - lastHeartbeat) < staleThreshold) {
                if (lastSeq != null && lastSeq >= sequenceNumber) {
                    count++;
                }
            }
        }

        return count;
    }

    /**
     * Get the number of active secondaries.
     */
    public int getActiveSecondaryCount() {
        long now = System.currentTimeMillis();
        long staleThreshold = 30000;
        int count = 0;

        for (Map.Entry<String, Long> entry : secondaryHeartbeats.entrySet()) {
            if ((now - entry.getValue()) < staleThreshold) {
                count++;
            }
        }

        return count;
    }

    /**
     * Remove a secondary from tracking (e.g., when it disconnects).
     */
    public void removeSecondary(String secondaryAddress) {
        secondaryProgress.remove(secondaryAddress);
        secondaryHeartbeats.remove(secondaryAddress);
        log.info("Removed secondary from tracking: {}", secondaryAddress);
    }

    /**
     * Get replication statistics.
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new ConcurrentHashMap<>();
        stats.put("currentSequence", writeSequence.get());
        stats.put("activeSecondaries", getActiveSecondaryCount());
        stats.put("replicaSetSize", replicaSetSize);

        Map<String, Object> secondaryStats = new ConcurrentHashMap<>();
        for (Map.Entry<String, Long> entry : secondaryProgress.entrySet()) {
            String secondary = entry.getKey();
            Long seq = entry.getValue();
            Long heartbeat = secondaryHeartbeats.get(secondary);
            long lag = writeSequence.get() - (seq != null ? seq : 0);

            Map<String, Object> secInfo = new ConcurrentHashMap<>();
            secInfo.put("lastSequence", seq);
            secInfo.put("lastHeartbeat", heartbeat);
            secInfo.put("lagSequences", lag);
            secondaryStats.put(secondary, secInfo);
        }
        stats.put("secondaries", secondaryStats);

        return stats;
    }
}
