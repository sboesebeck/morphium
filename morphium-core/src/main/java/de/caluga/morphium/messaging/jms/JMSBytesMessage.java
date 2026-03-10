package de.caluga.morphium.messaging.jms;

import de.caluga.morphium.annotations.Transient;
import de.caluga.morphium.annotations.lifecycle.PreStore;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JMSBytesMessage extends JMSMessage implements BytesMessage {
    private List<Byte> body;

    @Transient
    private ByteArrayInputStream bin;

    @Transient
    private ByteArrayOutputStream bout;


    @PreStore
    public void preStore() {
        body = new ArrayList<>();
        for (byte b : bout.toByteArray()) {
            body.add(b);
        }
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public long getBodyLength() throws JMSException {
        return body.size();
    }

    private ByteArrayInputStream getByteIn() {
        if (bin == null) {
            byte[] b = new byte[body.size()];
            for (int i = 0; i < b.length; i++) {
                b[i] = body.get(i);
            }
            bin = new ByteArrayInputStream(b);
        }
        return bin;
    }

    private ByteArrayOutputStream getBout() {
        if (bout == null) {
            bout = new ByteArrayOutputStream();
        }
        return bout;
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public boolean readBoolean() throws JMSException {
        return getByteIn().read() == 1;
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public byte readByte() throws JMSException {
        return (byte) getByteIn().read();
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public int readUnsignedByte() throws JMSException {
        return (byte) getByteIn().read();
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public short readShort() throws JMSException {
        return (short) getByteIn().read();
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public int readUnsignedShort() throws JMSException {
        return (short) getByteIn().read();
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public char readChar() throws JMSException {
        return (char) getByteIn().read();
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public int readInt() throws JMSException {
        return getByteIn().read();
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public long readLong() throws JMSException {
        return ((long) getByteIn().read() << 32 | (long) getByteIn().read());
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public float readFloat() throws JMSException {
        return 0;
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public double readDouble() throws JMSException {
        return 0;
    }

    @SuppressWarnings("RedundantThrows")
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

    @SuppressWarnings("RedundantThrows")
    @Override
    public int readBytes(byte[] value, int length) throws JMSException {
        return getByteIn().read(value, 0, length);
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void writeBoolean(boolean value) throws JMSException {
        if (value) {
            getBout().write(1);
        } else {
            getBout().write(0);
        }
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void writeByte(byte value) throws JMSException {
        getBout().write(value);
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void writeShort(short value) throws JMSException {
        getBout().write(value);
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void writeChar(char value) throws JMSException {
        getBout().write(value);
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void writeInt(int value) throws JMSException {
        getBout().write(value);
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void writeLong(long value) throws JMSException {
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void writeFloat(float value) throws JMSException {

    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void writeDouble(double value) throws JMSException {

    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void writeUTF(String value) throws JMSException {

    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void writeBytes(byte[] value) throws JMSException {

    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void writeBytes(byte[] value, int offset, int length) throws JMSException {

    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void writeObject(Object value) throws JMSException {

    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void reset() throws JMSException {

    }
}
