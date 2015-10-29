package de.caluga.morphium.driver.bson;/**
 * Created by stephan on 27.10.15.
 */

import de.caluga.morphium.Logger;

import java.net.NetworkInterface;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TODO: Add Documentation here
 **/
public class MongoId {

    private final static int THE_MACHINE_ID;
    private int machineId;
    private static final AtomicInteger COUNT = new AtomicInteger(new SecureRandom().nextInt());
    private final short PID;
    private final int counter;
    private final int timestamp;

    static {
        try {
            THE_MACHINE_ID = createMachineId();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public MongoId() {
        PID = createPID();
        counter = COUNT.getAndIncrement();
        timestamp = (int) (System.currentTimeMillis() / 1000);
        machineId = THE_MACHINE_ID;
    }

    private MongoId(short pid, int c, int ts) {
        this.PID = pid;
        counter = c;
        timestamp = ts;
        machineId = THE_MACHINE_ID;
    }

    public MongoId(byte[] bytes, int idx) {
        if (bytes == null) {
            throw new IllegalArgumentException();
        } else if (idx + 12 >= bytes.length) {
            throw new IllegalArgumentException("not enough data, 12 bytes needed");
        } else {
            this.timestamp = readInt(bytes, idx);
            this.machineId = readInt(new byte[]{(byte) 0, bytes[idx + 4], bytes[idx + 5], bytes[idx + 6]}, 0);
            this.PID = (short) readInt(new byte[]{(byte) 0, (byte) 0, bytes[idx + 7], bytes[idx + 8]}, 0);
            this.counter = readInt(new byte[]{(byte) 0, bytes[idx + 9], bytes[idx + 10], bytes[idx + 11]}, 0);
        }
    }

    private int readInt(byte[] bytes, int idx) {
        return bytes[idx] << 24 | (bytes[idx + 1] & 0xFF) << 16 | (bytes[idx + 2] & 0xFF) << 8 | (bytes[idx + 3] & 0xFF);

    }


    private void storeInt(byte[] arr, int offset, int val) {
        for (int i = 0; i < 4; i++) arr[i + offset] = ((byte) ((val >> ((7 - i) * 8)) & 0xff));
    }

    private void storeShort(byte[] arr, int offset, int val) {
        for (int i = 0; i < 2; i++) arr[i + offset] = ((byte) ((val >> ((3 - i) * 8)) & 0xff));
    }

    private void storeInt3Byte(byte[] arr, int offset, int val) {
        for (int i = 1; i < 4; i++) arr[i + offset - 1] = ((byte) ((val >> ((7 - i) * 8)) & 0xff));
    }

    public byte[] getBytes() {
        byte[] bytes = new byte[12];

        storeInt(bytes, 0, timestamp);
        storeInt3Byte(bytes, 4, machineId);
        storeShort(bytes, 7, PID);
        storeShort(bytes, 10, counter);
        return bytes;
    }

    private static short createPID() {
        short processId;
        try {
            String pName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
            if (pName.contains("@")) {
                processId = (short) Integer.parseInt(pName.substring(0, pName.indexOf('@')));
            } else {
                processId = (short) pName.hashCode();
            }
        } catch (Throwable t) {
            new Logger(MongoId.class).error("could not get processID - using random fallback");
            processId = (short) new SecureRandom().nextInt();
        }

        return processId;
    }


    private static int createMachineId() {
        int machineId = 0;
        try {
            Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
            StringBuilder b = new StringBuilder();

            while (e.hasMoreElements()) {
                NetworkInterface ni = e.nextElement();
                byte[] hwAdress = ni.getHardwareAddress();
                if (hwAdress != null) {
                    ByteBuffer buf = ByteBuffer.wrap(hwAdress);
                    try {
                        b.append(buf.getChar());
                        b.append(buf.getChar());
                        b.append(buf.getChar());
                    } catch (BufferUnderflowException shortHardwareAddressException) {
                        //cannot be
                    }
                }
            }
            machineId = b.toString().hashCode();
        } catch (Throwable t) {
            new Logger(MongoId.class).error("error accessing nics to create machine identifier... using fallback", t);
        }

        if (machineId == 0) machineId = (new SecureRandom().nextInt());

        machineId = machineId & 0x00ffffff;
        return machineId;
    }
}
