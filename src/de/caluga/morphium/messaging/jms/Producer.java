package de.caluga.morphium.messaging.jms;

import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

public class Producer implements JMSProducer {

    private Messaging messaging;
    private Map<String, Object> properties;
    private long ttl = 30000;
    private int priority = 500;
    private int deliveryMode;
    private boolean disableTimestamp;
    private boolean disableId;
    private Vector<Object> waitingForAck;
    private CompletionListener completionListener;

    private Logger log = LoggerFactory.getLogger(Producer.class);


    public Producer(Messaging messaging) {
        this.messaging = messaging;
        messaging.start();
        properties = new ConcurrentHashMap<>();
        waitingForAck = new Vector<>();
        messaging.addListenerForMessageNamed("ack", (MessageListener<JMSMessage>) (msg, m) -> {
            if (waitingForAck.contains(m.getInAnswerTo())) {
                completionListener.onCompletion(m);
                waitingForAck.remove(m.getInAnswerTo());
            } else {
                if (m.getInAnswerTo() == null) {
                    log.error("Got broadcasted ack-Message?!?!?");
                } else {
                    log.error("Got answer for an unknown message " + m.getInAnswerTo());
                }
            }
            return null;
        });
    }

    @Override
    public JMSProducer send(Destination destination, Message message) {

        return this;
    }

    @Override
    public JMSProducer send(Destination destination, String body) {
        try {
            JMSTextMessgae txt = new JMSTextMessgae();
            txt.setText(body);
            for (String k : properties.keySet()) {
                txt.setObjectProperty(k, properties.get(k));
            }
            txt.setPriority(priority);
            txt.setTtl(ttl);
            if (destination instanceof JMSTopic) {
                txt.setExclusive(true);
                txt.setName(((JMSTopic) destination).getTopicName());
            } else if (destination instanceof JMSQueue) {
                txt.setExclusive(false);
                txt.setName(((JMSQueue) destination).getQueueName());
            } else {
                throw new IllegalArgumentException("Destination has invalid type!");
            }
            if (getAsync() != null) {
                messaging.storeMessage(txt);
                waitingForAck.add(txt.getMsgId());
            } else {
                try {
                    messaging.sendAndAwaitFirstAnswer(txt, 5000);
                    if (log.isDebugEnabled()) log.debug("Message " + txt.getMsgId() + " acknowledged");
                } catch (Exception e) {
                    throw new RuntimeException("message " + txt.getMsgId() + " was not acknowledged", e);
                }
            }

        } catch (JMSException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        return this;
    }

    @Override
    public JMSProducer send(Destination destination, Map<String, Object> body) {
        return this;
    }

    @Override
    public JMSProducer send(Destination destination, byte[] body) {
        return this;
    }

    @Override
    public JMSProducer send(Destination destination, Serializable body) {
        return this;
    }

    @Override
    public JMSProducer setDisableMessageID(boolean value) {
        disableId = value;
        return this;
    }

    @Override
    public boolean getDisableMessageID() {
        return disableId;
    }

    @Override
    public JMSProducer setDisableMessageTimestamp(boolean value) {
        disableTimestamp = value;
        return this;
    }

    @Override
    public boolean getDisableMessageTimestamp() {
        return disableTimestamp;
    }

    @Override
    public JMSProducer setDeliveryMode(int deliveryMode) {
        this.deliveryMode = deliveryMode;
        return this;
    }

    @Override
    public int getDeliveryMode() {
        return deliveryMode;
    }

    @Override
    public JMSProducer setPriority(int priority) {
        this.priority = priority;
        return this;
    }

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public JMSProducer setTimeToLive(long timeToLive) {
        ttl = timeToLive;
        return this;
    }

    @Override
    public long getTimeToLive() {
        return ttl;
    }

    @Override
    public JMSProducer setDeliveryDelay(long deliveryDelay) {
        return this;
    }

    @Override
    public long getDeliveryDelay() {
        return 0;
    }

    @Override
    public JMSProducer setAsync(CompletionListener completionListener) {
        this.completionListener = completionListener;
        return this;
    }

    @Override
    public CompletionListener getAsync() {
        return completionListener;
    }

    @Override
    public JMSProducer setProperty(String name, boolean value) {

        properties.put(name, value);
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, byte value) {
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, short value) {
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, int value) {
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, long value) {
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, float value) {
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, double value) {
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, String value) {
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, Object value) {
        return this;
    }

    @Override
    public JMSProducer clearProperties() {
        properties.clear();
        return this;
    }

    @Override
    public boolean propertyExists(String name) {
        properties.containsKey(name);
        return false;
    }

    @Override
    public boolean getBooleanProperty(String name) {
        return false;
    }

    @Override
    public byte getByteProperty(String name) {
        return 0;
    }

    @Override
    public short getShortProperty(String name) {
        return 0;
    }

    @Override
    public int getIntProperty(String name) {
        return 0;
    }

    @Override
    public long getLongProperty(String name) {
        return 0;
    }

    @Override
    public float getFloatProperty(String name) {
        return 0;
    }

    @Override
    public double getDoubleProperty(String name) {
        return 0;
    }

    @Override
    public String getStringProperty(String name) {
        return null;
    }

    @Override
    public Object getObjectProperty(String name) {
        return null;
    }

    @Override
    public Set<String> getPropertyNames() {
        return null;
    }

    @Override
    public JMSProducer setJMSCorrelationIDAsBytes(byte[] correlationID) {
        messaging.setSenderId(new String(correlationID));
        return this;
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() {
        return messaging.getSenderId().getBytes();
    }

    @Override
    public JMSProducer setJMSCorrelationID(String correlationID) {
        messaging.setSenderId(correlationID);
        return this;
    }

    @Override
    public String getJMSCorrelationID() {
        return messaging.getSenderId();
    }

    @Override
    public JMSProducer setJMSType(String type) {
        return this;
    }

    @Override
    public String getJMSType() {
        return null;
    }

    @Override
    public JMSProducer setJMSReplyTo(Destination replyTo) {
        return this;
    }

    @Override
    public Destination getJMSReplyTo() {
        return null;
    }
}
