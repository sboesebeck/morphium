package de.caluga.morphium.messaging.jms;

import de.caluga.morphium.Morphium;

import javax.jms.*;

public class JMSConnection implements Connection {
    private final Morphium morphium;

    public JMSConnection(Morphium m) {
        morphium = m;
    }

    @Override
    public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public Session createSession(int sessionMode) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public Session createSession() throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public String getClientID() throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public void setClientID(String clientID) throws JMSException {

    }

    @Override
    public ConnectionMetaData getMetaData() throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public ExceptionListener getExceptionListener() throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public void setExceptionListener(ExceptionListener listener) throws JMSException {

    }

    @Override
    public void start() throws JMSException {

    }

    @Override
    public void stop() throws JMSException {

    }

    @Override
    public void close() throws JMSException {

    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public ConnectionConsumer createSharedConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public ConnectionConsumer createSharedDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }
}
