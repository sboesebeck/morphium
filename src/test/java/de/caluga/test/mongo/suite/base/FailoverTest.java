package de.caluga.test.mongo.suite.base;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;

/**
 * Failover tests removed - MorphiumServer now uses simplified static primary election.
 * The complex failover/replication functionality was removed when consolidating
 * to the async Netty-based MorphiumServer implementation.
 */
@Disabled("Failover functionality removed - MorphiumServer now uses simplified primary election")
@Tag("failover")
@Tag("morphiumserver")
public class FailoverTest {
    // Tests removed - failover functionality no longer exists
}
