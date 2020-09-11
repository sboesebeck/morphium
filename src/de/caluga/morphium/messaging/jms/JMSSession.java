package de.caluga.morphium.messaging.jms;

import javax.jms.*;
import java.io.Serializable;

public class JMSSession implements Session {

    @Override
    public BytesMessage createBytesMessage() throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public MapMessage createMapMessage() throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public Message createMessage() throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public ObjectMessage createObjectMessage() throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable object) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public StreamMessage createStreamMessage() throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public TextMessage createTextMessage() throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public TextMessage createTextMessage(String text) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public boolean getTransacted() throws JMSException {
        return false;
    }

    @Override
    public int getAcknowledgeMode() throws JMSException {
        return 0;
    }

    @Override
    public void commit() throws JMSException {

    }

    @Override
    public void rollback() throws JMSException {

    }

    @Override
    public void close() throws JMSException {

    }

    @Override
    public void recover() throws JMSException {

    }

    @Override
    public MessageListener getMessageListener() throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public void setMessageListener(MessageListener listener) throws JMSException {

    }

    @Override
    public void run() {

    }

    @Override
    public MessageProducer createProducer(Destination destination) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public MessageConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public MessageConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName, String messageSelector) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public Queue createQueue(String queueName) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public Topic createTopic(String topicName) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public MessageConsumer createDurableConsumer(Topic topic, String name) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public MessageConsumer createDurableConsumer(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name, String messageSelector) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public void unsubscribe(String name) throws JMSException {

    }
}
