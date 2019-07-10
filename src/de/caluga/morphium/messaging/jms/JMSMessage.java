package de.caluga.morphium.messaging.jms;

import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.Msg;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import java.util.Collections;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;

public class JMSMessage extends Msg implements Message {
    private boolean redelivered = false;
    private Object body;
    private Destination replyTo;
    private Destination destination;


    public JMSMessage() {
        setMapValue(new ConcurrentHashMap<>());
    }

    @Override
    public String getJMSMessageID() throws JMSException {
        return getMsgId().toString();
    }

    @Override
    public void setJMSMessageID(String id) throws JMSException {
        setMsgId(new MorphiumId(id));
    }

    @Override
    public long getJMSTimestamp() throws JMSException {
        return getTimestamp();
    }

    @Override
    public void setJMSTimestamp(long timestamp) throws JMSException {
        setTimestamp(timestamp);
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        return getMsgId().getBytes();
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException {
        setMsgId(new MorphiumId(correlationID));
    }

    @Override
    public String getJMSCorrelationID() throws JMSException {
        return getMsgId().toString();
    }

    @Override
    public void setJMSCorrelationID(String correlationID) throws JMSException {
        setMsgId(new MorphiumId(correlationID));
    }

    @Override
    public Destination getJMSReplyTo() throws JMSException {
        return replyTo;
    }

    @Override
    public void setJMSReplyTo(Destination replyTo) throws JMSException {
        this.replyTo = replyTo;
    }

    @Override
    public Destination getJMSDestination() throws JMSException {
        return destination;
    }

    @Override
    public void setJMSDestination(Destination destination) throws JMSException {
        this.destination = destination;
    }

    @Override
    public int getJMSDeliveryMode() throws JMSException {
        return 2;
    }

    @Override
    public void setJMSDeliveryMode(int deliveryMode) throws JMSException {

    }

    @Override
    public boolean getJMSRedelivered() throws JMSException {
        return redelivered;
    }

    @Override
    public void setJMSRedelivered(boolean redelivered) throws JMSException {
        this.redelivered = redelivered;
    }

    @Override
    public String getJMSType() throws JMSException {
        return getName();
    }

    @Override
    public void setJMSType(String type) throws JMSException {
        setName(type);
    }

    @Override
    public long getJMSExpiration() throws JMSException {
        return System.currentTimeMillis() + getTtl();
    }

    @Override
    public void setJMSExpiration(long expiration) throws JMSException {
        setTtl(System.currentTimeMillis() - expiration);
    }

    @Override
    public long getJMSDeliveryTime() throws JMSException {
        return getTimestamp();
    }

    @Override
    public void setJMSDeliveryTime(long deliveryTime) throws JMSException {
        setTimestamp(deliveryTime);
    }

    @Override
    public int getJMSPriority() throws JMSException {
        return getPriority();
    }

    @Override
    public void setJMSPriority(int priority) throws JMSException {
        setPriority(priority);
    }

    @Override
    public void clearProperties() throws JMSException {
        getMapValue().clear();
    }

    @Override
    public boolean propertyExists(String name) throws JMSException {
        return getMapValue().containsKey(name);
    }

    @Override
    public boolean getBooleanProperty(String name) throws JMSException {
        return getMapValue().get(name).equals("true");
    }

    @Override
    public byte getByteProperty(String name) throws JMSException {
        return (Byte) getMapValue().get(name);
    }

    @Override
    public short getShortProperty(String name) throws JMSException {
        return (Short) getMapValue().get(name);
    }

    @Override
    public int getIntProperty(String name) throws JMSException {
        return (Integer) getMapValue().get(name);
    }

    @Override
    public long getLongProperty(String name) throws JMSException {
        return (Long) getMapValue().get(name);
    }

    @Override
    public float getFloatProperty(String name) throws JMSException {
        return (Float) getMapValue().get(name);
    }

    @Override
    public double getDoubleProperty(String name) throws JMSException {
        return (Double) getMapValue().get(name);
    }

    @Override
    public String getStringProperty(String name) throws JMSException {
        return (String) getMapValue().get(name);
    }

    @Override
    public Object getObjectProperty(String name) throws JMSException {
        return null;
    }

    @Override
    public Enumeration getPropertyNames() throws JMSException {
        return Collections.enumeration(getMapValue().keySet());
    }

    @Override
    public void setBooleanProperty(String name, boolean value) throws JMSException {
        getMapValue().put(name, "" + value);
    }

    @Override
    public void setByteProperty(String name, byte value) throws JMSException {
        getMapValue().put(name, value);
    }

    @Override
    public void setShortProperty(String name, short value) throws JMSException {
        getMapValue().put(name, value);
    }

    @Override
    public void setIntProperty(String name, int value) throws JMSException {
        getMapValue().put(name, value);
    }

    @Override
    public void setLongProperty(String name, long value) throws JMSException {
        getMapValue().put(name, value);

    }

    @Override
    public void setFloatProperty(String name, float value) throws JMSException {
        getMapValue().put(name, value);

    }

    @Override
    public void setDoubleProperty(String name, double value) throws JMSException {
        getMapValue().put(name, value);

    }

    @Override
    public void setStringProperty(String name, String value) throws JMSException {
        getMapValue().put(name, value);

    }

    @Override
    public void setObjectProperty(String name, Object value) throws JMSException {
        getMapValue().put(name, value);

    }

    @Override
    public void acknowledge() throws JMSException {

    }

    @Override
    public void clearBody() throws JMSException {
        body = null;
    }

    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        return (T) body;
    }

    @Override
    public boolean isBodyAssignableTo(Class c) throws JMSException {
        if (body == null) return false;
        return c.isAssignableFrom(body.getClass());
    }
}
