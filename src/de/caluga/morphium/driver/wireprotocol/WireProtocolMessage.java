package de.caluga.morphium.driver.wireprotocol;/**
 * Created by stephan on 04.11.15.
 */

import java.io.IOException;
import java.io.OutputStream;

/**
 * base class for mongodb wire protocol messages:
 * * bit num	name	description
 * 0	Reserved	Must be set to 0.
 * 1	TailableCursor	Tailable means cursor is not closed when the last data is retrieved. Rather, the cursor marks the final object’s position. You can resume using the cursor later, from where it was located, if more data were received. Like any “latent cursor”, the cursor may become invalid at some point (CursorNotFound) – for example if the final object it references were deleted.
 * 2	SlaveOk	Allow query of replica slave. Normally these return an error except for namespace “local”.
 * 3	OplogReplay	Internal replication use only - driver should not set
 * 4	NoCursorTimeout	The server normally times out idle cursors after an inactivity period (10 minutes) to prevent excess memory use. Set this option to prevent that.
 * 5	AwaitData	Use with TailableCursor. If we are at the end of the data, block for a while rather than returning no data. After a timeout period, we do return as normal.
 * 6	Exhaust	Stream the data down full blast in multiple “more” packages, on the assumption that the client will fully read all data queried. Faster when you are pulling a lot of data and know you want to pull it all down. Note: the client is not allowed to not read all the data unless it closes the connection.
 * 7	Partial	Get partial results from a mongos if some shards are down (instead of throwing an error)
 * 8-31	Reserved	Must be set to 0.
 **/
@SuppressWarnings("WeakerAccess")
public abstract class WireProtocolMessage {


    public static final int TAILABLE_CURSOR = 2;
    public static final int SLAVE_OK = 4;
    public static final int NO_CURSOR_TIMEOUT = 16;
    public static final int AWAIT_DATA = 32;
    //    public static final int EXHAUST=64;
    public static final int PARTIAL = 128;

    /**

     */
    protected int flags;

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

    public int getFlags() {
        return flags;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    @SuppressWarnings("unused")
    public void setTailableCursor(boolean t) {
        if (t) {
            flags = flags | TAILABLE_CURSOR;
        } else {
            flags = flags & ~TAILABLE_CURSOR;
        }
    }

    @SuppressWarnings("unused")
    public boolean isPartial() {
        return (flags & PARTIAL) != 0;
    }

    @SuppressWarnings("unused")
    public void setPartial(boolean t) {
        if (t) {
            flags = flags | PARTIAL;
        } else {
            flags = flags & ~PARTIAL;
        }
    }

    @SuppressWarnings("unused")
    public boolean isAwaitData() {
        return (flags & AWAIT_DATA) != 0;
    }

    @SuppressWarnings("unused")
    public void setAwaitData(boolean t) {
        if (t) {
            flags = flags | AWAIT_DATA;
        } else {
            flags = flags & ~AWAIT_DATA;
        }
    }

    @SuppressWarnings("unused")
    public boolean isTailable() {
        return (flags & TAILABLE_CURSOR) != 0;
    }


    @SuppressWarnings("unused")
    public boolean isSlaveOk() {
        return (flags & SLAVE_OK) != 0;
    }

    @SuppressWarnings("unused")
    public void setSlaveOk(boolean t) {
        if (t) {
            flags = flags | SLAVE_OK;
        } else {
            flags = flags & ~SLAVE_OK;
        }
    }

    @SuppressWarnings("unused")
    public boolean isNoCursorTimeout() {
        return (flags & NO_CURSOR_TIMEOUT) != 0;
    }

    @SuppressWarnings("unused")
    public void setNoCursorTimeout(boolean t) {
        if (t) {
            flags = flags | NO_CURSOR_TIMEOUT;
        } else {
            flags = flags & ~NO_CURSOR_TIMEOUT;
        }
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
}
