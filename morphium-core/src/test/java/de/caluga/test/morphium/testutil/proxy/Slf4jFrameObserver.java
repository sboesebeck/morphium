package de.caluga.test.morphium.testutil.proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;

/** Trivial working example of a {@link FrameObserver} - the "logging is provided for" seam
 * the design spec describes. Not used by the failover test itself (which relies only on the
 * escape-guard assertion, not a wire log), but proves the hook is usable by a real consumer. */
public class Slf4jFrameObserver implements FrameObserver {
    private static final Logger log = LoggerFactory.getLogger(Slf4jFrameObserver.class);

    @Override
    public void onFrame(Direction dir, WireProtocolMessage msg, ConnectionCtx ctx) {
        log.debug("{} frame on {}: {}", dir, ctx, msg);
    }
}
