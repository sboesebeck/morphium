package de.caluga.morphium.driver.wireprotocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import org.xerial.snappy.Snappy;

public class OpCompressed extends WireProtocolMessage {
    public final static int COMPRESSOR_NOOP = 0;
    public final static int COMPRESSOR_SNAPPY = 1;
    public final static int COMPRESSOR_ZLIB = 2;
    public final static int COMPRESSOR_ZSTD = 3;
    private int originalOpCode;
    private int uncompressedSize;
    private int compressorId;
    private byte[] compressedMessage;

    @Override
    public void parsePayload(byte[] bytes, int offset) throws IOException {
        int idx = 0;
        originalOpCode = readInt(bytes, 0);
        idx += 4;
        uncompressedSize = readInt(bytes, idx);
        idx += 4;
        compressorId = (byte)bytes[idx];
        idx++;
        compressedMessage = new byte[bytes.length - idx];
        System.arraycopy(bytes, idx, compressedMessage, 0, compressedMessage.length);
        compressedMessage = Snappy.uncompress(compressedMessage);
    }

    @Override
    public byte[] getPayload() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeInt(originalOpCode, out);
        writeInt(uncompressedSize, out);
        out.write((byte)compressorId);

        if (compressorId == COMPRESSOR_SNAPPY) {
            out.write(Snappy.compress(compressedMessage));
        } else if (compressorId == COMPRESSOR_ZLIB) {
            DeflaterOutputStream zlibOut = new DeflaterOutputStream(out);
            zlibOut.write(compressedMessage);
            zlibOut.flush();
        } else if (compressorId == COMPRESSOR_ZSTD) {
            throw new IllegalArgumentException("ZSTD compression not supported!");
        } else {
            throw new IllegalArgumentException("unsupported compression id: " + compressorId);
        }

        return out.toByteArray();
    }

    @Override
    public int getOpCode() {
        return OpCode.OP_COMPRESSED.opCode;
    }

    public int getOriginalOpCode() {
        return originalOpCode;
    }

    public void setOriginalOpCode(int originalOpCode) {
        this.originalOpCode = originalOpCode;
    }

    public int getUncompressedSize() {
        return uncompressedSize;
    }

    public void setUncompressedSize(int uncompressedSize) {
        this.uncompressedSize = uncompressedSize;
    }

    public int getCompressorId() {
        return compressorId;
    }

    public void setCompressorId(int compressorId) {
        this.compressorId = compressorId;
    }

    public byte[] getCompressedMessage() {
        return compressedMessage;
    }

    public void setCompressedMessage(byte[] compressedMessage) {
        this.compressedMessage = compressedMessage;
    }
}
