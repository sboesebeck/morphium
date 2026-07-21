package de.caluga.morphium.driver.wireprotocol;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;

import de.caluga.morphium.driver.MorphiumDriverNetworkException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A socket read timeout that strikes AFTER parseFromStream already consumed bytes of a
 * message leaves the TCP stream misaligned: the consumed bytes are gone, and the next
 * parse attempt reads mid-message payload as a header. Seen in the 2026-07-20 overnight
 * run as "Illegal opcode 1919906674" - the "opcode" was the ASCII text "rsor" from a
 * "\x03cursor" BSON field of a half-read getMore reply, exactly 16 bytes (one header)
 * into the abandoned message.
 *
 * Contract under test: a timeout with ZERO bytes consumed is benign (stream still
 * aligned, caller may retry on the same connection). A timeout after ANY byte was
 * consumed must surface as a fatal MorphiumDriverNetworkException so callers close
 * the connection instead of retrying/pooling it.
 */
@Tag("core")
public class WireProtocolTimeoutDesyncTest {

    /** Delivers the given bytes, then throws SocketTimeoutException on every further read. */
    private static class TimingOutStream extends InputStream {
        private final byte[] data;
        private int pos = 0;

        TimingOutStream(byte[] data) {
            this.data = data;
        }

        @Override
        public int read() throws IOException {
            if (pos >= data.length) {
                throw new SocketTimeoutException("Read timed out");
            }
            return data[pos++] & 0xff;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (pos >= data.length) {
                throw new SocketTimeoutException("Read timed out");
            }
            int n = Math.min(len, data.length - pos);
            System.arraycopy(data, pos, b, off, n);
            pos += n;
            return n;
        }
    }

    private static byte[] header(int size, int msgId, int responseTo, int opCode) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeInt(size, out);
        writeInt(msgId, out);
        writeInt(responseTo, out);
        writeInt(opCode, out);
        return out.toByteArray();
    }

    private static void writeInt(int value, ByteArrayOutputStream out) {
        out.write(value & 0xff);
        out.write((value >> 8) & 0xff);
        out.write((value >> 16) & 0xff);
        out.write((value >> 24) & 0xff);
    }

    @Test
    public void timeoutBeforeAnyByteStaysBenign() {
        Throwable t = catchThrowable(() ->
                WireProtocolMessage.parseFromStream(new TimingOutStream(new byte[0])));

        assertThat(t).as("timeout with 0 bytes consumed leaves the stream aligned - must stay retryable")
                .isInstanceOf(SocketTimeoutException.class);
    }

    @Test
    public void timeoutMidHeaderIsFatal() throws IOException {
        byte[] partialHeader = new byte[8];

        Throwable t = catchThrowable(() ->
                WireProtocolMessage.parseFromStream(new TimingOutStream(partialHeader)));

        assertThat(t).as("8 of 16 header bytes consumed - retrying on this stream reads garbage")
                .isInstanceOf(MorphiumDriverNetworkException.class);
        assertThat(t.getMessage()).containsIgnoringCase("desync");
    }

    @Test
    public void timeoutAfterHeaderIsFatal() throws IOException {
        // complete OP_MSG header announcing a 100-byte message whose body never arrives
        byte[] headerOnly = header(100, 42, 0, WireProtocolMessage.OpCode.OP_MSG.opCode);

        Throwable t = catchThrowable(() ->
                WireProtocolMessage.parseFromStream(new TimingOutStream(headerOnly)));

        assertThat(t).as("header consumed, body missing - the next parse would read payload as header")
                .isInstanceOf(MorphiumDriverNetworkException.class);
        assertThat(t.getMessage()).containsIgnoringCase("desync");
    }

    @Test
    public void timeoutMidBodyIsFatal() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(header(100, 42, 0, WireProtocolMessage.OpCode.OP_MSG.opCode));
        out.write(new byte[20]); // 20 of 84 body bytes, then silence

        Throwable t = catchThrowable(() ->
                WireProtocolMessage.parseFromStream(new TimingOutStream(out.toByteArray())));

        assertThat(t).as("partial body consumed - stream is mid-message")
                .isInstanceOf(MorphiumDriverNetworkException.class);
        assertThat(t.getMessage()).containsIgnoringCase("desync");
    }
}
