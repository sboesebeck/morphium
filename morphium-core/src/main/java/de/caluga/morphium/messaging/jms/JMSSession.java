package de.caluga.morphium.messaging.jms;

import javax.jms.*;
import java.io.Serializable;

public class JMSSession implements Session {

    @SuppressWarnings("RedundantThrows")
    @Override
    public BytesMessage createBytesMessage() throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public MapMessage createMapMessage() throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public Message createMessage() throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public ObjectMessage createObjectMessage() throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public ObjectMessage createObjectMessage(Serializable object) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public StreamMessage createStreamMessage() throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public TextMessage createTextMessage() throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public TextMessage createTextMessage(String text) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public boolean getTransacted() throws JMSException {
        return false;
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public int getAcknowledgeMode() throws JMSException {
        return 0;
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void commit() throws JMSException {

    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void rollback() throws JMSException {

    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void close() throws JMSException {

    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void recover() throws JMSException {

    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public MessageListener getMessageListener() throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void setMessageListener(MessageListener listener) throws JMSException {

    }

    @Override
    public void run() {

    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public MessageProducer createProducer(Destination destination) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public MessageConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public MessageConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName, String messageSelector) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public Queue createQueue(String queueName) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public Topic createTopic(String topicName) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public MessageConsumer createDurableConsumer(Topic topic, String name) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public MessageConsumer createDurableConsumer(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name, String messageSelector) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void unsubscribe(String name) throws JMSException {

    }
}
