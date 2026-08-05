package de.caluga.test.morphium.testutil.proxy;

import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;

/** Rewrites a backend→client frame before it is forwarded. The address rewriter (Task 2) is
 * the first implementation; kept as a separate interface from {@link FrameObserver} on purpose
 * (fault = state, rewrite = strategy, observe = listener - no single omnipotent interceptor). */
public interface ResponseRewriter {
    WireProtocolMessage rewrite(WireProtocolMessage reply);
}
