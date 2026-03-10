package de.caluga.test.morphium.driver.pool;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.morphium.driver.wire.SingleMongoConnection;

/**
 * Test for the host key mismatch bug fix.
 * 
 * The bug: When hosts are stored in the map without port numbers (e.g., "server.example.com"),
 * but connections return "server.example.com:27017" from getConnectedTo(), the releaseConnection()
 * lookup would fail, causing the borrowed counter to never be decremented.
 * 
 * This test verifies that the fallback lookup (trying hostname without port) works correctly.
 */
public class HostKeyMismatchTest {
    private static final Logger log = LoggerFactory.getLogger(HostKeyMismatchTest.class);
    
    /**
     * Mock connection that simulates the host:port format mismatch
     */
    static class MockConnection extends SingleMongoConnection {
        private final String hostname;
        private final int port;
        private final int sourcePort;
        private boolean connected = true;
        
        public MockConnection(String hostname, int port, int sourcePort) {
            this.hostname = hostname;
            this.port = port;
            this.sourcePort = sourcePort;
        }
        
        @Override
        public String getConnectedTo() {
            return hostname + ":" + port;  // Always returns host:port
        }
        
        @Override
        public String getConnectedToHost() {
            return hostname;
        }
        
        @Override
        public int getConnectedToPort() {
            return port;
        }
        
        @Override
        public int getSourcePort() {
            return sourcePort;
        }
        
        @Override
        public boolean isConnected() {
            return connected;
        }
        
        @Override
        public void close() {
            connected = false;
        }
    }
    
    @Test
    @DisplayName("Counter should be decremented even when host stored without port")
    public void testBorrowedCounterDecrementWithHostKeyMismatch() throws Exception {
        // This test uses reflection to simulate the scenario without needing MongoDB
        
        PooledDriver driver = new PooledDriver();
        
        // Access private fields via reflection
        Field hostsField = PooledDriver.class.getDeclaredField("hosts");
        hostsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Object> hosts = (Map<String, Object>) hostsField.get(driver);
        
        Field borrowedField = PooledDriver.class.getDeclaredField("borrowedConnections");
        borrowedField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<Integer, Object> borrowedConnections = (Map<Integer, Object>) borrowedField.get(driver);
        
        Field runningField = PooledDriver.class.getDeclaredField("running");
        runningField.setAccessible(true);
        runningField.set(driver, true);
        
        // Create a Host object with hostname-only key (simulating the bug scenario)
        String hostnameOnly = "testserver.example.com";
        String hostnameWithPort = hostnameOnly + ":27017";
        
        // Use reflection to create Host instance
        Class<?> hostClass = Class.forName("de.caluga.morphium.driver.wire.Host");
        Object host = hostClass.getConstructor(String.class, int.class)
            .newInstance(hostnameOnly, 27017);
        
        // Add host with hostname-only key (this is the problematic scenario)
        hosts.put(hostnameOnly, host);
        
        // Get borrowed counter accessor
        Method getBorrowedConnections = hostClass.getMethod("getBorrowedConnections");
        Method incrementBorrowed = hostClass.getMethod("incrementBorrowedConnections");
        Method getConnectionPool = hostClass.getMethod("getConnectionPool");
        
        // Simulate borrowing a connection
        incrementBorrowed.invoke(host);
        int borrowedBefore = (int) getBorrowedConnections.invoke(host);
        assertEquals(1, borrowedBefore, "Borrowed count should be 1 after increment");
        
        // Create a mock connection that returns hostname:port (like real connections do)
        MockConnection mockCon = new MockConnection(hostnameOnly, 27017, 12345);
        
        // Add to borrowed connections map (simulating what borrowConnection does)
        Class<?> containerClass = Class.forName("de.caluga.morphium.driver.wire.PooledDriver$ConnectionContainer");
        Object container = containerClass.getConstructor(SingleMongoConnection.class).newInstance(mockCon);
        borrowedConnections.put(12345, container);
        
        // Add a connection to the pool so offer() has somewhere to return it
        @SuppressWarnings("unchecked")
        BlockingQueue<Object> pool = (BlockingQueue<Object>) getConnectionPool.invoke(host);
        
        // Now release the connection - this should find the host via fallback
        driver.releaseConnection(mockCon);
        
        // Verify the counter was decremented
        int borrowedAfter = (int) getBorrowedConnections.invoke(host);
        assertEquals(0, borrowedAfter, 
            "Borrowed count should be 0 after release - fallback lookup should have found the host");
        
        log.info("Test passed: borrowed counter correctly decremented from {} to {}", 
            borrowedBefore, borrowedAfter);
        
        driver.close();
    }
    
    @Test
    @DisplayName("Counter should work normally when host stored with port")
    public void testBorrowedCounterWithMatchingHostKey() throws Exception {
        // Control test: verify normal case still works
        
        PooledDriver driver = new PooledDriver();
        
        // Access private fields via reflection
        Field hostsField = PooledDriver.class.getDeclaredField("hosts");
        hostsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Object> hosts = (Map<String, Object>) hostsField.get(driver);
        
        Field borrowedField = PooledDriver.class.getDeclaredField("borrowedConnections");
        borrowedField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<Integer, Object> borrowedConnections = (Map<Integer, Object>) borrowedField.get(driver);
        
        Field runningField = PooledDriver.class.getDeclaredField("running");
        runningField.setAccessible(true);
        runningField.set(driver, true);
        
        String hostname = "testserver.example.com";
        String hostnameWithPort = hostname + ":27017";
        
        // Create Host with host:port key (normal case)
        Class<?> hostClass = Class.forName("de.caluga.morphium.driver.wire.Host");
        Object host = hostClass.getConstructor(String.class, int.class)
            .newInstance(hostname, 27017);
        
        // Add host with hostname:port key (matching what connection returns)
        hosts.put(hostnameWithPort, host);
        
        Method getBorrowedConnections = hostClass.getMethod("getBorrowedConnections");
        Method incrementBorrowed = hostClass.getMethod("incrementBorrowedConnections");
        Method getConnectionPool = hostClass.getMethod("getConnectionPool");
        
        // Simulate borrowing
        incrementBorrowed.invoke(host);
        assertEquals(1, (int) getBorrowedConnections.invoke(host));
        
        // Create mock connection
        MockConnection mockCon = new MockConnection(hostname, 27017, 54321);
        
        // Add to borrowed map
        Class<?> containerClass = Class.forName("de.caluga.morphium.driver.wire.PooledDriver$ConnectionContainer");
        Object container = containerClass.getConstructor(SingleMongoConnection.class).newInstance(mockCon);
        borrowedConnections.put(54321, container);
        
        // Release
        driver.releaseConnection(mockCon);
        
        // Verify
        int borrowedAfter = (int) getBorrowedConnections.invoke(host);
        assertEquals(0, borrowedAfter, 
            "Borrowed count should be 0 after release with matching host key");
        
        log.info("Control test passed: normal case works correctly");
        
        driver.close();
    }
    
    @Test
    @DisplayName("Without fix: demonstrate the counter drift bug")
    public void demonstrateCounterDriftBug() throws Exception {
        // This test demonstrates what WOULD happen without the fix
        // by checking what happens when neither hostname nor hostname:port matches
        
        PooledDriver driver = new PooledDriver();
        
        Field hostsField = PooledDriver.class.getDeclaredField("hosts");
        hostsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Object> hosts = (Map<String, Object>) hostsField.get(driver);
        
        Field borrowedField = PooledDriver.class.getDeclaredField("borrowedConnections");
        borrowedField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<Integer, Object> borrowedConnections = (Map<Integer, Object>) borrowedField.get(driver);
        
        Field runningField = PooledDriver.class.getDeclaredField("running");
        runningField.setAccessible(true);
        runningField.set(driver, true);
        
        // Simulate a completely mismatched scenario (neither key matches)
        String storedHostname = "different-server.example.com";
        String connectionHostname = "actual-server.example.com";
        
        Class<?> hostClass = Class.forName("de.caluga.morphium.driver.wire.Host");
        Object host = hostClass.getConstructor(String.class, int.class)
            .newInstance(storedHostname, 27017);
        
        hosts.put(storedHostname, host);
        
        Method getBorrowedConnections = hostClass.getMethod("getBorrowedConnections");
        Method incrementBorrowed = hostClass.getMethod("incrementBorrowedConnections");
        
        // Simulate borrowing on stored host
        incrementBorrowed.invoke(host);
        assertEquals(1, (int) getBorrowedConnections.invoke(host));
        
        // Create connection pointing to different host
        MockConnection mockCon = new MockConnection(connectionHostname, 27017, 99999);
        
        Class<?> containerClass = Class.forName("de.caluga.morphium.driver.wire.PooledDriver$ConnectionContainer");
        Object container = containerClass.getConstructor(SingleMongoConnection.class).newInstance(mockCon);
        borrowedConnections.put(99999, container);
        
        // Release - this should NOT find a matching host
        driver.releaseConnection(mockCon);
        
        // The counter on the stored host should still be 1 (not decremented)
        // because neither hostname nor hostname:port matched
        int borrowedAfter = (int) getBorrowedConnections.invoke(host);
        assertEquals(1, borrowedAfter, 
            "Counter should remain 1 when no host matches (demonstrating the need for correct keys)");
        
        log.info("Bug demonstration: counter remains at {} when host doesn't match", borrowedAfter);
        
        driver.close();
    }
}
