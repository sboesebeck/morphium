package de.caluga.morphium.driver.wireprotocol;/**
 * Created by stephan on 04.11.15.
 */

import de.caluga.morphium.driver.MorphiumDriverNetworkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@SuppressWarnings("WeakerAccess")
public abstract class WireProtocolMessage {
    private int size;
    private int messageId;
    private int responseTo;
    private Logger log = LoggerFactory.getLogger(WireProtocolMessage.class);

    public static WireProtocolMessage parseFromStream(InputStream in) throws java.net.SocketException {
        byte[] inBuffer = new byte[16];
        int numRead;
        try {
            if (in==null){
                return null;
            }
            numRead = in.read(inBuffer, 0, 16);
            while (numRead < 16) {
                numRead += in.read(inBuffer, numRead, 16 - numRead);
            }
            int size = WireProtocolMessage.readInt(inBuffer, 0);
            int offset = 4;
            int messageId = WireProtocolMessage.readInt(inBuffer, offset);
            offset += 4;
            int responseTo = WireProtocolMessage.readInt(inBuffer, offset);
            offset += 4;
            int opCode = WireProtocolMessage.readInt(inBuffer, offset);
            offset += 4;
            WireProtocolMessage message = null;
            OpCode c = OpCode.findByCode(opCode);
            if (c == null) {
                throw new RuntimeException("Illegal opcode " + opCode);
            }
            message = c.handler.getDeclaredConstructor().newInstance();

            message.setMessageId(messageId);
            message.setSize(size);
            message.setResponseTo(responseTo);

            byte buf[] = new byte[size - 16];

            numRead = in.read(buf, 0, size - 16);
            while (numRead < size - 16) {
                numRead += in.read(buf, numRead, size - 16 - numRead);
            }

            try {
                //LoggerFactory.getLogger(WireProtocolMessage.class).info("Parsing incoming data: \n"+ Utils.getHex(buf));
                message.parsePayload(buf, 0);

                return message;
            } catch (Exception e) {
                //log.error("Could not read");
                throw new MorphiumDriverNetworkException("could not read from socket", e);
            }
        // } catch (java.net.SocketException se) {
        //     //probably closed - ignore
        //     return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String readString(byte[] bytes, int idx) {
        int i = idx;
        while (bytes[i] != 0) {
            i++;
        }
        return new String(bytes, idx, i - idx);
    }

    public static int strLen(byte[] bytes, int idx) {
        int i = idx;
        while (bytes[i] != 0) {
            i++;
        }
        return i - idx + 1;
    }

    public static int readInt(byte[] bytes, int idx) {
        return (bytes[idx] & 0xff) | ((bytes[idx + 1] & 0xff) << 8) | ((bytes[idx + 2] & 0xff) << 16) | ((bytes[idx + 3] & 0xff) << 24);
    }

    public static long readLong(byte[] bytes, int idx) {
        return ((long) ((bytes[idx] & 0xFF))) |
                ((long) ((bytes[idx + 1] & 0xFF)) << 8) |
                ((long) (bytes[idx + 2] & 0xFF) << 16) |
                ((long) (bytes[idx + 3] & 0xFF) << 24) |
                ((long) (bytes[idx + 4] & 0xFF) << 32) |
                ((long) (bytes[idx + 5] & 0xFF) << 40) |
                ((long) (bytes[idx + 6] & 0xFF) << 48) |
                ((long) (bytes[idx + 7] & 0xFF) << 56);

    }

    public static void writeLong(long lng, OutputStream out) throws IOException {
        for (int i = 7; i >= 0; i--) out.write((byte) ((lng >> ((7 - i) * 8)) & 0xff));
    }

    public abstract void parsePayload(byte[] bytes, int offset) throws IOException;

    public abstract byte[] getPayload() throws IOException;

    public abstract int getOpCode();

    public final byte[] bytes() throws IOException {
        byte[] payload = getPayload();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeInt(payload.length + 16, out);
        writeInt(messageId, out);
        writeInt(responseTo, out);
        writeInt(getOpCode(), out);
        out.write(payload);
        return out.toByteArray();
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getMessageId() {
        return messageId;
    }

    public void setMessageId(int messageId) {
        this.messageId = messageId;
    }

    public int getResponseTo() {
        return responseTo;
    }

    public void setResponseTo(int responseTo) {
        this.responseTo = responseTo;
    }

    public void writeInt(int value, OutputStream to) throws IOException {
        to.write(((byte) ((value) & 0xff)));
        to.write(((byte) ((value >> 8) & 0xff)));
        to.write(((byte) ((value >> 16) & 0xff)));
        to.write(((byte) ((value >> 24) & 0xff)));
    }

    public void writeString(String n, OutputStream to) throws IOException {
        to.write(n.getBytes("UTF-8"));
        to.write((byte) 0);
    }

    public enum OpCode {
        OP_REPLY(1, OpReply.class),
        OP_UPDATE(2001, OpUpdate.class),
        OP_INSERT(2002, OpInsert.class),
        OP_QUERY(2004, OpQuery.class),
        OP_GET_MORE(2005, OpGetMore.class),
        OP_DELETE(2006, OpDelete.class),
        OP_KILL_CURSORS(2007, OpKillCursors.class),
        OP_COMPRESSED(2012, OpCompressed.class),
        OP_MSG(2013, OpMsg.class);


        int opCode;
        Class<? extends WireProtocolMessage> handler;

        OpCode(int opCode, Class<? extends WireProtocolMessage> handler) {
            this.opCode = opCode;
            this.handler = handler;
        }

        static OpCode findByCode(int c) {
            for (OpCode o : OpCode.values()) {
                if (o.opCode == c) return o;
            }
            return null;
        }

    }


}
