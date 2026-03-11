package de.caluga.morphium.messaging.jms;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.Msg;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import java.util.Collections;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;

@Entity(typeId = "jmsmessage", polymorph = true)
public class JMSMessage extends Msg implements Message {
    private boolean redelivered = false;
    private Object body;
    private Destination replyTo;
    private Destination destination;


    public JMSMessage() {
        setMapValue(new ConcurrentHashMap<>());
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public String getJMSMessageID() throws JMSException {
        return getMsgId().toString();
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void setJMSMessageID(String id) throws JMSException {
        setMsgId(new MorphiumId(id));
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public long getJMSTimestamp() throws JMSException {
        return getTimestamp();
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void setJMSTimestamp(long timestamp) throws JMSException {
        setTimestamp(timestamp);
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        return getMsgId().getBytes();
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException {
        setMsgId(new MorphiumId(correlationID));
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public String getJMSCorrelationID() throws JMSException {
        return getMsgId().toString();
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void setJMSCorrelationID(String correlationID) throws JMSException {
        setMsgId(new MorphiumId(correlationID));
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public Destination getJMSReplyTo() throws JMSException {
        return replyTo;
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void setJMSReplyTo(Destination replyTo) throws JMSException {
        this.replyTo = replyTo;
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public Destination getJMSDestination() throws JMSException {
        return destination;
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void setJMSDestination(Destination destination) throws JMSException {
        this.destination = destination;
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public int getJMSDeliveryMode() throws JMSException {
        return 2;
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void setJMSDeliveryMode(int deliveryMode) throws JMSException {

    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public boolean getJMSRedelivered() throws JMSException {
        return redelivered;
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void setJMSRedelivered(boolean redelivered) throws JMSException {
        this.redelivered = redelivered;
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public String getJMSType() throws JMSException {
        return getTopic();
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void setJMSType(String type) throws JMSException {
        setTopic(type);
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public long getJMSExpiration() throws JMSException {
        return System.currentTimeMillis() + getTtl();
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void setJMSExpiration(long expiration) throws JMSException {
        setTtl(System.currentTimeMillis() - expiration);
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public long getJMSDeliveryTime() throws JMSException {
        return getTimestamp();
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void setJMSDeliveryTime(long deliveryTime) throws JMSException {
        setTimestamp(deliveryTime);
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public int getJMSPriority() throws JMSException {
        return getPriority();
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void setJMSPriority(int priority) throws JMSException {
        setPriority(priority);
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void clearProperties() throws JMSException {
        getMapValue().clear();
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public boolean propertyExists(String name) throws JMSException {
        return getMapValue().containsKey(name);
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public boolean getBooleanProperty(String name) throws JMSException {
        return getMapValue().get(name).equals("true");
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public byte getByteProperty(String name) throws JMSException {
        return (Byte) getMapValue().get(name);
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public short getShortProperty(String name) throws JMSException {
        return (Short) getMapValue().get(name);
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public int getIntProperty(String name) throws JMSException {
        return (Integer) getMapValue().get(name);
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public long getLongProperty(String name) throws JMSException {
        return (Long) getMapValue().get(name);
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public float getFloatProperty(String name) throws JMSException {
        return (Float) getMapValue().get(name);
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public double getDoubleProperty(String name) throws JMSException {
        return (Double) getMapValue().get(name);
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public String getStringProperty(String name) throws JMSException {
        return (String) getMapValue().get(name);
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public Object getObjectProperty(String name) throws JMSException {
        return null;
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public Enumeration getPropertyNames() throws JMSException {
        return Collections.enumeration(getMapValue().keySet());
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void setBooleanProperty(String name, boolean value) throws JMSException {
        getMapValue().put(name, "" + value);
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void setByteProperty(String name, byte value) throws JMSException {
        getMapValue().put(name, value);
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void setShortProperty(String name, short value) throws JMSException {
        getMapValue().put(name, value);
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void setIntProperty(String name, int value) throws JMSException {
        getMapValue().put(name, value);
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void setLongProperty(String name, long value) throws JMSException {
        getMapValue().put(name, value);

    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void setFloatProperty(String name, float value) throws JMSException {
        getMapValue().put(name, value);

    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void setDoubleProperty(String name, double value) throws JMSException {
        getMapValue().put(name, value);

    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void setStringProperty(String name, String value) throws JMSException {
        getMapValue().put(name, value);

    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void setObjectProperty(String name, Object value) throws JMSException {
        getMapValue().put(name, value);

    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void acknowledge() throws JMSException {

    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void clearBody() throws JMSException {
        body = null;
    }

    @SuppressWarnings({"RedundantThrows", "unchecked"})
    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        return (T) body;
    }

    @SuppressWarnings({"RedundantThrows", "unchecked"})
    @Override
    public boolean isBodyAssignableTo(Class c) throws JMSException {
        if (body == null) return false;
        return c.isAssignableFrom(body.getClass());
    }
}
