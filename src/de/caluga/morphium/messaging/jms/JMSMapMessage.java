package de.caluga.morphium.messaging.jms;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import java.util.Enumeration;

public class JMSMapMessage extends JMSMessage implements MapMessage {
    @Override
    public boolean getBoolean(String name) throws JMSException {
        return (boolean) getMapValue().get(name);
    }

    @Override
    public byte getByte(String name) throws JMSException {
        return (byte) getMapValue().get(name);
    }

    @Override
    public short getShort(String name) throws JMSException {
        return (short) getMapValue().get(name);
    }

    @Override
    public char getChar(String name) throws JMSException {
        return (char) getMapValue().get(name);
    }

    @Override
    public int getInt(String name) throws JMSException {
        return (int) getMapValue().get(name);
    }

    @Override
    public long getLong(String name) throws JMSException {
        return (long) getMapValue().get(name);
    }

    @Override
    public float getFloat(String name) throws JMSException {
        return (float) getMapValue().get(name);
    }

    @Override
    public double getDouble(String name) throws JMSException {
        return (double) getMapValue().get(name);
    }

    @Override
    public String getString(String name) throws JMSException {
        return (String) getMapValue().get(name);
    }

    @Override
    public byte[] getBytes(String name) throws JMSException {
        return (byte[]) getMapValue().get(name);
    }

    @Override
    public Object getObject(String name) throws JMSException {
        return getMapValue().get(name);
    }

    @Override
    public Enumeration getMapNames() throws JMSException {
        return null;
    }

    @Override
    public void setBoolean(String name, boolean value) throws JMSException {

    }

    @Override
    public void setByte(String name, byte value) throws JMSException {

    }

    @Override
    public void setShort(String name, short value) throws JMSException {

    }

    @Override
    public void setChar(String name, char value) throws JMSException {

    }

    @Override
    public void setInt(String name, int value) throws JMSException {

    }

    @Override
    public void setLong(String name, long value) throws JMSException {

    }

    @Override
    public void setFloat(String name, float value) throws JMSException {

    }

    @Override
    public void setDouble(String name, double value) throws JMSException {

    }

    @Override
    public void setString(String name, String value) throws JMSException {

    }

    @Override
    public void setBytes(String name, byte[] value) throws JMSException {

    }

    @Override
    public void setBytes(String name, byte[] value, int offset, int length) throws JMSException {

    }

    @Override
    public void setObject(String name, Object value) throws JMSException {

    }

    @Override
    public boolean itemExists(String name) throws JMSException {
        return false;
    }
}
