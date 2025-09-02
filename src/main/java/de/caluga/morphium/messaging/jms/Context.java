package de.caluga.morphium.messaging.jms;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.config.MessagingSettings;

import javax.jms.*;
import java.io.Serializable;

public class Context implements javax.jms.JMSContext {

    private final Morphium morphium;
    private final MorphiumMessaging messaging;
    private boolean startOnCosumerCreate = true;

    public Context(Morphium morphium, String name, int sessionMode) {
        this.morphium = morphium;
        try {
            this.messaging = morphium.createMessaging();
        } catch (Exception e) {
            throw new RuntimeException("Could not create MorphiumMessaging via factory", e);
        }

        // match previous defaults: pause=100, multithreadded=true, windowSize=10
        this.messaging.setPause(100).setMultithreadded(true).setWindowSize(10);
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
        return new Producer(messaging);
    }

    @Override
    public String getClientID() {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public void setClientID(String clientID) {
        messaging.setSenderId(clientID);
    }

    @Override
    public ConnectionMetaData getMetaData() {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public ExceptionListener getExceptionListener() {
        throw new IllegalArgumentException("not implemented yet, sorry");
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
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public MapMessage createMapMessage() {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public Message createMessage() {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public ObjectMessage createObjectMessage() {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable object) {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public StreamMessage createStreamMessage() {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public TextMessage createTextMessage() {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public TextMessage createTextMessage(String text) {
        throw new IllegalArgumentException("not implemented yet, sorry");
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

        return new Consumer(messaging, destination);
    }

    @Override
    public JMSConsumer createConsumer(Destination destination, String messageSelector) {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public JMSConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public Queue createQueue(String queueName) {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public Topic createTopic(String topicName) {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name) {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name, String messageSelector, boolean noLocal) {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name) {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name, String messageSelector) {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName) {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName, String messageSelector) {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public TemporaryQueue createTemporaryQueue() {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public TemporaryTopic createTemporaryTopic() {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public void unsubscribe(String name) {
    }

    @Override
    public void acknowledge() {
    }
}
