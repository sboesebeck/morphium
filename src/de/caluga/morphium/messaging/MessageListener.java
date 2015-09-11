package de.caluga.morphium.messaging;

/**
 * User: Stephan Bösebeck
 * Date: 26.05.12
 * Time: 17:19
 * <p/>
 */
public interface MessageListener {


    /**
     * process message, send answer
     * if null is returned, no answer is sent
     */
    Msg onMessage(Messaging msg, Msg m);
}
