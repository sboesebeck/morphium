package de.caluga.morphium.driver.wire;

import de.caluga.morphium.driver.wire.PooledDriver.ConnectionContainer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Host {
    private static final Logger log = LoggerFactory.getLogger(Host.class);
    private final String hostName;
    private final int port;
    private final BlockingQueue<ConnectionContainer> connectionPool = new LinkedBlockingQueue<>();
    private final AtomicInteger waitCounter = new AtomicInteger(0);
    private final AtomicInteger failures = new AtomicInteger(0);
    private final AtomicInteger borrowedConnections = new AtomicInteger(0);
    public static final int MAX_FAILURES = 5;
    private PooledDriver.PingStats pingStats = new PooledDriver.PingStats(0, 0, 0, 0, 0, 0);

    public Host(String host, int port) {
        this.hostName = host;
        this.port = port;
    }

    public int getBorrowedConnections() {
        return borrowedConnections.get();
    }

    public void incrementBorrowedConnections() {
        borrowedConnections.incrementAndGet();
    }

    public void decrementBorrowedConnections() {
        borrowedConnections.decrementAndGet();
    }


    public int getFailures() {
        return failures.get();
    }

    public void incrementFailures() {
        failures.incrementAndGet();
    }

    public void resetFailures() {
        failures.set(0);
    }

    public String getHostName() {
        return hostName;
    }

    public int getPort() {
        return port;
    }

    public BlockingQueue<ConnectionContainer> getConnectionPool() {
        return connectionPool;
    }

    public int getWaitCounter() {
        return waitCounter.get();
    }

    public void incrementWaitCounter() {
        waitCounter.incrementAndGet();
    }

    public void decrementWaitCounter() {
        waitCounter.decrementAndGet();
    }

    public PooledDriver.PingStats getPingStats() {
        return pingStats;
    }

    public void setPingStats(PooledDriver.PingStats pingStats) {
        this.pingStats = pingStats;
    }
}
