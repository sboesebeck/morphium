package de.caluga.morphium;

/**
 * User: Stephan Bösebeck
 * Date: 03.05.12
 * Time: 6:23
 * <p>
 * Will be called, whenever Morphium-instance is shut down. Call morphium.addShutdownListener to be informed...
 */
public interface ShutdownListener {
    @SuppressWarnings("UnusedParameters")
    void onShutdown(Morphium m);
}
