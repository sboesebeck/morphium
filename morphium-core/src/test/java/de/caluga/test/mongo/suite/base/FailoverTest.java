package de.caluga.test.mongo.suite.base;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;

/**
 * Failover tests removed - PoppyDB now uses simplified static primary election.
 * The complex failover/replication functionality was removed when consolidating
 * to the async Netty-based PoppyDB implementation.
 */
@Disabled("Failover functionality removed - PoppyDB now uses simplified primary election")
@Tag("failover")
@Tag("poppydb")
public class FailoverTest {
    // Tests removed - failover functionality no longer exists
}
