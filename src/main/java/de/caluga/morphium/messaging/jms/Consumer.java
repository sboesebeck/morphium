package de.caluga.morphium.messaging.jms;

import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.StdMessaging;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.Stack;
import java.util.concurrent.locks.LockSupport;

public class Consumer implements JMSConsumer, de.caluga.morphium.messaging.MessageListener<JMSMessage> {

    private final String selector;
    private final MorphiumMessaging messaging;
    private MessageListener listener;
    private final Stack<JMSMessage> incomingQueue = new Stack<>();

    private final Logger log = LoggerFactory.getLogger(Consumer.class);

    public Consumer(MorphiumMessaging messaging, Destination dst) {
        this.messaging = messaging;
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
            // try {
            //     Thread.sleep(20);
            // } catch (InterruptedException e) {
            // }
            LockSupport.parkNanos(20000000);
            if (timeout > 0 && System.currentTimeMillis() - start > timeout) {
                log.warn("Did not receive message in time!");
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
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public <T> T receiveBody(Class<T> c, long timeout) {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public <T> T receiveBodyNoWait(Class<T> c) {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public JMSMessage onMessage(MorphiumMessaging msg, JMSMessage m) {
        log.info("Incoming message...");
        if (getMessageListener() != null) {
            getMessageListener().onMessage(m);
        }

        incomingQueue.push(m);
        return getAckMessage(m);
    }

    private JMSMessage getAckMessage(JMSMessage m) {
        //send ACK
        JMSMessage ans = new JMSMessage();
        ans.setName("ack");
        ans.setInAnswerTo(m.getMsgId());
        ans.setMsg("ack");
        ans.addRecipient(m.getSender());
        return ans;
    }

    private void ack(JMSMessage m) {
        log.info("Sending ack...");
        messaging.sendMessage(getAckMessage(m));
    }
}
