package de.caluga.test.morphium.testutil.proxy;

/** Minimal per-connection context handed to a {@link FrameObserver}. Kept deliberately small -
 * expand only when a real consumer needs more (see design spec's open question on this type's
 * shape). */
public record ConnectionCtx(String peerAddress, int listenPort) {
}
