package de.caluga.morphium.driver.bson;/**
 * Created by stephan on 27.10.15.
 */

import de.caluga.morphium.Logger;

import java.net.NetworkInterface;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Date;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Morphium representation of an ID. similar to BSON ID
 **/
public class MorphiumId implements Comparable<MorphiumId> {

    private static final int THE_MACHINE_ID;
    private static final AtomicInteger COUNT = new AtomicInteger(new SecureRandom().nextInt());
    @SuppressWarnings("unused")
    public static ThreadLocal<Short> threadPid;

    static {
        try {
            THE_MACHINE_ID = createMachineId();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private final short pid;
    private final int counter;
    private final int timestamp;
    private int machineId;

    public MorphiumId() {
        this((Date) null);
    }

    public MorphiumId(Date date) {

        pid = createPID();
        counter = COUNT.getAndIncrement() & 0x00ffffff;
        machineId = THE_MACHINE_ID;

        long time = date == null ? System.currentTimeMillis() : date.getTime();
        timestamp = (int) (time / 1000);
    }

    public MorphiumId(String hexString) {
        this(hexToByte(hexString));
    }

    public MorphiumId(byte[] bytes) {
        this(bytes, 0);
    }

    private MorphiumId(short pid, int c, int ts) {
        this.pid = pid;
        counter = c;
        timestamp = ts;
        machineId = THE_MACHINE_ID;
    }

    public MorphiumId(byte[] bytes, int idx) {
        if (bytes == null) {
            throw new IllegalArgumentException();
        } else if (idx + 12 > bytes.length) {
            throw new IllegalArgumentException("not enough data, 12 bytes needed");
        } else {
            this.timestamp = readInt(bytes, idx, 4);
            this.machineId = readInt(bytes, idx + 4, 3);
            this.pid = (short) readInt(bytes, idx + 7, 2);
            this.counter = readInt(bytes, idx + 9, 3);
        }
    }

    private static byte[] hexToByte(String s) {
        if (s == null || s.length() != 24 || s.matches("[g-zG-Z]")) {
            throw new IllegalArgumentException("no hex string: " + s);
        } else {
            byte[] b = new byte[12];

            for (int i = 0; i < b.length; ++i) {
                b[i] = (byte) Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16);
            }

            return b;
        }
    }

    private static short createPID() {
        //        if (threadPid == null || threadPid.get() == null) {

        short processId;
        try {
            String pName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
            if (pName.contains("@")) {
                processId = (short) Integer.parseInt(pName.substring(0, pName.indexOf('@')));
            } else {
                processId = (short) pName.hashCode();
            }
        } catch (Throwable t) {
            new Logger(MorphiumId.class).error("could not get processID - using random fallback");
            processId = (short) new SecureRandom().nextInt();
        }
        //            threadPid = new ThreadLocal<>();
        //            threadPid.set(processId);
        return processId;
        //        }
        //
        //        return threadPid.get();
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
            new Logger(MorphiumId.class).error("error accessing nics to create machine identifier... using fallback", t);
        }

        if (machineId == 0) {
            machineId = (new SecureRandom().nextInt());
        }

        machineId = machineId & 0x00ffffff;
        return machineId;
    }

    private int readInt(byte[] bytes, int idx, int len) {
        switch (len) {
            case 4:
                return bytes[idx] << 24 | (bytes[idx + 1] & 0xFF) << 16 | (bytes[idx + 2] & 0xFF) << 8 | (bytes[idx + 3] & 0xFF);
            case 3:
                return (bytes[idx] & 0xFF) << 16 | (bytes[idx + 1] & 0xFF) << 8 | (bytes[idx + 2] & 0xFF);
            case 2:
                return (bytes[idx] & 0xFF) << 8 | (bytes[idx + 1] & 0xFF);
            default:
                return 0;
        }

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (o.getClass() != this.getClass()) {
            return false;
        }

        MorphiumId morphiumId = (MorphiumId) o;

        return machineId == morphiumId.machineId && pid == morphiumId.pid && counter == morphiumId.counter && timestamp == morphiumId.timestamp;

    }

    @Override
    public int hashCode() {
        int result = machineId;
        result = 31 * result + (int) pid;
        result = 31 * result + counter;
        result = 31 * result + timestamp;
        return result;
    }

    private void storeInt(byte[] arr, int offset, int val) {
        for (int i = 0; i < 4; i++) arr[i + offset] = ((byte) ((val >> ((7 - i) * 8)) & 0xff));
    }

    private void storeShort(byte[] arr, int offset, int val) {
        arr[offset] = ((byte) ((val >> 8) & 0xff));
        arr[offset + 1] = ((byte) ((val) & 0xff));
    }

    private void storeInt3Byte(byte[] arr, int offset, int val) {
        arr[offset] = ((byte) ((val >> 16) & 0xff));
        arr[offset + 1] = ((byte) ((val >> 8) & 0xff));
        arr[offset + 2] = ((byte) ((val) & 0xff));
    }

    public byte[] getBytes() {
        byte[] bytes = new byte[12];

        storeInt(bytes, 0, timestamp);
        storeInt3Byte(bytes, 4, machineId);
        storeShort(bytes, 7, pid);
        storeInt3Byte(bytes, 9, counter);
        return bytes;
    }

    public long getTime() {
        return timestamp * 1000;
    }

    public String toString() {
        String[] chars = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F",};
        byte[] b = getBytes();

        StringBuilder bld = new StringBuilder();

        for (byte by : b) {
            int idx = (by >>> 4) & 0x0f;
            bld.append(chars[idx]);
            idx = by & 0x0f;
            bld.append(chars[idx]);
        }
        return bld.toString();
    }

    @Override
    public int compareTo(MorphiumId o) {
        if (o == null) {
            return -1;
        }
        return toString().compareTo(o.toString());
    }
}
