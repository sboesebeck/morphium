package de.caluga.morphium.messaging.jms;

import de.caluga.morphium.annotations.Transient;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class JMSBytesMessage extends JMSMessage implements BytesMessage {
    private byte[] body;

    @Transient
    private ByteArrayInputStream bin;

    @Transient
    private ByteArrayOutputStream bout;

    @Override
    public long getBodyLength() throws JMSException {
        return body.length;
    }

    private ByteArrayInputStream getByteIn() {
        if (bin == null) {
            bin = new ByteArrayInputStream(body);
        }
        return bin;
    }

    private ByteArrayOutputStream getBout() {
        if (bout == null) {
            bout = new ByteArrayOutputStream();
        }
        return bout;
    }

    @Override
    public boolean readBoolean() throws JMSException {
        return getByteIn().read() == 1;
    }

    @Override
    public byte readByte() throws JMSException {
        return (byte) getByteIn().read();
    }

    @Override
    public int readUnsignedByte() throws JMSException {
        return (byte) getByteIn().read();
    }

    @Override
    public short readShort() throws JMSException {
        return (short) getByteIn().read();
    }

    @Override
    public int readUnsignedShort() throws JMSException {
        return (short) getByteIn().read();
    }

    @Override
    public char readChar() throws JMSException {
        return (char) getByteIn().read();
    }

    @Override
    public int readInt() throws JMSException {
        return getByteIn().read();
    }

    @Override
    public long readLong() throws JMSException {
        return ((long) getByteIn().read() << 32 | (long) getByteIn().read());
    }

    @Override
    public float readFloat() throws JMSException {
        return 0;
    }

    @Override
    public double readDouble() throws JMSException {
        return 0;
    }

    @Override
    public String readUTF() throws JMSException {
        throw new IllegalArgumentException("not implemented yet, sorry");
    }

    @Override
    public int readBytes(byte[] value) throws JMSException {
        try {
            return getByteIn().read(value);
        } catch (IOException e) {
            throw new JMSException("error");
        }
    }

    @Override
    public int readBytes(byte[] value, int length) throws JMSException {
        return getByteIn().read(value, 0, length);
    }

    @Override
    public void writeBoolean(boolean value) throws JMSException {
        if (value) {
            getBout().write(1);
        } else {
            getBout().write(0);
        }
    }

    @Override
    public void writeByte(byte value) throws JMSException {
        getBout().write(value);
    }

    @Override
    public void writeShort(short value) throws JMSException {
        getBout().write(value);
    }

    @Override
    public void writeChar(char value) throws JMSException {
        getBout().write(value);
    }

    @Override
    public void writeInt(int value) throws JMSException {
        getBout().write(value);
    }

    @Override
    public void writeLong(long value) throws JMSException {
    }

    @Override
    public void writeFloat(float value) throws JMSException {

    }

    @Override
    public void writeDouble(double value) throws JMSException {

    }

    @Override
    public void writeUTF(String value) throws JMSException {

    }

    @Override
    public void writeBytes(byte[] value) throws JMSException {

    }

    @Override
    public void writeBytes(byte[] value, int offset, int length) throws JMSException {

    }

    @Override
    public void writeObject(Object value) throws JMSException {

    }

    @Override
    public void reset() throws JMSException {

    }
}
