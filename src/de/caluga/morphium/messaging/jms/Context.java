package de.caluga.morphium.messaging.jms;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.messaging.Messaging;

import javax.jms.*;
import java.io.Serializable;

public class Context implements javax.jms.JMSContext {

    private final int sessionMode;
    private final Morphium morphium;
    private final Messaging messaging;
    private boolean startOnCosumerCreate = true;

    public Context(Morphium morphium, String name, int sessionMode) {
        this.morphium = morphium;
        this.sessionMode = sessionMode;
        this.messaging = new Messaging(morphium, 100, true, true, 10);
        if (name != null) this.messaging.setQueueName(name);
    }

    public Context(Morphium morphium) {
        this(morphium, null, JMSContext.CLIENT_ACKNOWLEDGE);
    }

    @Override
    public javax.jms.JMSContext createContext(int sessionMode) {
        return new Context(morphium, null, sessionMode);
    }

    @Override
    public JMSProducer createProducer() {
        return null;
    }

    @Override
    public String getClientID() {
        return null;
    }

    @Override
    public void setClientID(String clientID) {
        messaging.setSenderId(clientID);
    }

    @Override
    public ConnectionMetaData getMetaData() {
        return null;
    }

    @Override
    public ExceptionListener getExceptionListener() {
        return null;
    }

    @Override
    public void setExceptionListener(ExceptionListener listener) {

    }

    @Override
    public void start() {
        messaging.start();
    }

    @Override
    public void stop() {
        messaging.terminate();
    }

    @Override
    public boolean getAutoStart() {
        return startOnCosumerCreate;
    }

    @Override
    public void setAutoStart(boolean autoStart) {
        startOnCosumerCreate = autoStart;
    }

    @Override
    public void close() {
        stop();
    }

    @Override
    public BytesMessage createBytesMessage() {
        return null;
    }

    @Override
    public MapMessage createMapMessage() {
        return null;
    }

    @Override
    public Message createMessage() {
        return null;
    }

    @Override
    public ObjectMessage createObjectMessage() {
        return null;
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable object) {
        return null;
    }

    @Override
    public StreamMessage createStreamMessage() {
        return null;
    }

    @Override
    public TextMessage createTextMessage() {
        return null;
    }

    @Override
    public TextMessage createTextMessage(String text) {
        return null;
    }

    @Override
    public boolean getTransacted() {
        return false;
    }

    @Override
    public int getSessionMode() {
        return 0;
    }

    @Override
    public void commit() {

    }

    @Override
    public void rollback() {

    }

    @Override
    public void recover() {

    }

    @Override
    public JMSConsumer createConsumer(Destination destination) {
        if (startOnCosumerCreate) start();
        return null;
    }

    @Override
    public JMSConsumer createConsumer(Destination destination, String messageSelector) {
        return null;
    }

    @Override
    public JMSConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) {
        return null;
    }

    @Override
    public Queue createQueue(String queueName) {
        return null;
    }

    @Override
    public Topic createTopic(String topicName) {
        return null;
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name) {
        return null;
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name, String messageSelector, boolean noLocal) {
        return null;
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name) {
        return null;
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name, String messageSelector) {
        return null;
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName) {
        return null;
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName, String messageSelector) {
        return null;
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) {
        return null;
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) {
        return null;
    }

    @Override
    public TemporaryQueue createTemporaryQueue() {
        return null;
    }

    @Override
    public TemporaryTopic createTemporaryTopic() {
        return null;
    }

    @Override
    public void unsubscribe(String name) {

    }

    @Override
    public void acknowledge() {

    }
}
