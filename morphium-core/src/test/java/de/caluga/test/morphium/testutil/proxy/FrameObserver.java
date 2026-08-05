package de.caluga.test.morphium.testutil.proxy;

import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;

/** Read-only observability hook - MUST NOT mutate {@code msg} (rewriting is
 * {@link ResponseRewriter}'s separate job). Client→backend frames are forwarded raw/unparsed
 * for pass-through fidelity (see {@link WireProxy}), so only {@code BACKEND_TO_CLIENT} fires
 * today; {@code CLIENT_TO_BACKEND} is reserved for a future consumer that specifically needs
 * it (YAGNI - not implemented until then). */
public interface FrameObserver {
    enum Direction { CLIENT_TO_BACKEND, BACKEND_TO_CLIENT }

    void onFrame(Direction dir, WireProtocolMessage msg, ConnectionCtx ctx);
}
