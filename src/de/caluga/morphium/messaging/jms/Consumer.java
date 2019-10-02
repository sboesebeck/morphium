package de.caluga.morphium.messaging.jms;

import de.caluga.morphium.messaging.Messaging;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.Stack;

public class Consumer implements JMSConsumer, de.caluga.morphium.messaging.MessageListener<JMSMessage> {

    private final Destination destination;
    private String selector;
    private Messaging messaging;
    private MessageListener listener;
    private Stack<JMSMessage> incomingQueue = new Stack<>();

    private Logger log = LoggerFactory.getLogger(Consumer.class);

    public Consumer(Messaging messaging, Destination dst) {
        this.messaging = messaging;
        this.destination = dst;
        try {
            if (dst instanceof JMSTopic) {
                selector = ((JMSTopic) dst).getTopicName();
            } else if (dst instanceof JMSQueue) {
                selector = ((JMSQueue) dst).getQueueName();
            } else {
                throw new IllegalArgumentException("Wrong type of destination");
            }
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
        //messaging.addMessageListener(this);
        messaging.addListenerForMessageNamed(selector, this);
    }

    @Override
    public String getMessageSelector() {
        return selector;
    }

    @Override
    public MessageListener getMessageListener() throws JMSRuntimeException {
        return listener;
    }

    @Override
    public void setMessageListener(MessageListener listener) throws JMSRuntimeException {
        this.listener = listener;
    }

    @Override
    public Message receive() {
        return receive(0);
    }

    @Override
    public Message receive(long timeout) {
        if (getMessageListener() != null) {
            throw new RuntimeException("calling synchronous receive not allowed when async listener is set");
        }
        long start = System.currentTimeMillis();
        while (incomingQueue.size() == 0) {
            Thread.yield();
            if (timeout > 0 && System.currentTimeMillis() - start > timeout) {
                return null;
            }
        }
        JMSMessage msg = incomingQueue.pop();
        ack(msg);
        return msg;
    }

    @Override
    public Message receiveNoWait() {
        return receive(1);
    }

    @Override
    public void close() {
        messaging.removeListenerForMessageNamed(selector, this);
    }

    @Override
    public <T> T receiveBody(Class<T> c) {
        return null;
    }

    @Override
    public <T> T receiveBody(Class<T> c, long timeout) {
        return null;
    }

    @Override
    public <T> T receiveBodyNoWait(Class<T> c) {
        return null;
    }

    @Override
    public JMSMessage onMessage(Messaging msg, JMSMessage m) throws InterruptedException {
        log.info("Incoming message...");
        if (getMessageListener() != null) {
            getMessageListener().onMessage(m);
            JMSMessage ans = getAckMessage(m);
            return ans;
        }
        incomingQueue.push(m);
        return null;
    }

    private JMSMessage getAckMessage(JMSMessage m) {
        //send ACK
        JMSMessage ans = new JMSMessage();
        ans.setName("ack");
        ans.setInAnswerTo(m.getMsgId());
        ans.setMsg("ack");
        ans.setRecipient(m.getSender());
        return ans;
    }

    private void ack(JMSMessage m) {
        log.info("Sending ack...");
        messaging.sendMessage(getAckMessage(m));
    }
}
