package de.caluga.morphium.messaging;

/**
 * User: Stephan Bösebeck
 * Date: 26.05.12
 * Time: 17:19
 * <p>
 */
public interface MessageListener<T extends Msg> {


    /**
     * process message, send answer
     * if null is returned, no answer is sent
     */
    @SuppressWarnings("UnusedParameters")
    T onMessage(MorphiumMessaging msg, T m) ;

    default boolean markAsProcessedBeforeExec() {
        return false;
    }
}
