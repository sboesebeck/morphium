package de.caluga.poppydb.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage.OpCode;

import java.util.List;

/**
 * Netty decoder for MongoDB wire protocol messages.
 * Converts ByteBuf to WireProtocolMessage objects.
 *
 * Wire protocol format:
 * - 4 bytes: message length (including header)
 * - 4 bytes: request ID
 * - 4 bytes: response to
 * - 4 bytes: opcode
 * - N bytes: payload (length - 16)
 */
public class MongoWireProtocolDecoder extends ByteToMessageDecoder {

    private static final Logger log = LoggerFactory.getLogger(MongoWireProtocolDecoder.class);
    private static final int HEADER_SIZE = 16;
    private static final int MAX_MESSAGE_SIZE = 48 * 1024 * 1024; // 48MB max message

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // Need at least the header to determine message size
        if (in.readableBytes() < HEADER_SIZE) {
            return;
        }

        // Mark the current position in case we need to reset
        in.markReaderIndex();

        // Read message size (first 4 bytes, little-endian)
        int messageSize = in.readIntLE();

        // Validate message size
        if (messageSize < HEADER_SIZE || messageSize > MAX_MESSAGE_SIZE) {
            log.error("Invalid message size: {} — closing connection (stream is corrupted)", messageSize);
            // Stream is corrupted beyond recovery — we don't know where the next message starts
            ctx.close();
            return;
        }

        // Check if we have the full message
        // We already read 4 bytes (size), so we need messageSize - 4 more bytes
        if (in.readableBytes() < messageSize - 4) {
            // Reset to beginning and wait for more data
            in.resetReaderIndex();
            return;
        }

        // Read header fields
        int requestId = in.readIntLE();
        int responseTo = in.readIntLE();
        int opCode = in.readIntLE();

        // Read payload (always consume the bytes to keep the stream in sync)
        int payloadSize = messageSize - HEADER_SIZE;

        // Zero-copy fast-path: if the ByteBuf is backed by a heap array, we can pass
        // the backing array + offset directly to parsePayload() and avoid allocating
        // a temporary byte[]. For direct/pooled buffers we fall back to the copy path.
        byte[] payload;
        int payloadOffset;
        if (in.hasArray()) {
            payload = in.array();
            payloadOffset = in.arrayOffset() + in.readerIndex();
            in.skipBytes(payloadSize);
        } else {
            payload = new byte[payloadSize];
            in.readBytes(payload);
            payloadOffset = 0;
        }

        // Find the opcode handler
        OpCode code = OpCode.findByCode(opCode);
        if (code == null) {
            log.error("Unknown opcode: {} (requestId={}, size={}) — skipping message", opCode, requestId, messageSize);
            // Bytes already consumed above, stream stays in sync — don't close the connection
            return;
        }

        // Create message instance
        try {
            WireProtocolMessage message = code.handler.getDeclaredConstructor().newInstance();
            message.setSize(messageSize);
            message.setMessageId(requestId);
            message.setResponseTo(responseTo);
            message.parsePayload(payload, payloadOffset);

            log.debug("Decoded {} message, id={}, size={}", code.name(), requestId, messageSize);
            out.add(message);
        } catch (Exception e) {
            log.error("Failed to parse {} message (requestId={}, size={}): {} — skipping",
                    code.name(), requestId, messageSize, e.getMessage());
            // Bytes already consumed, stream stays in sync — don't close the connection
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Decoder error on {}: {}", ctx.channel().remoteAddress(), cause.getMessage());
        // Only close on fatal I/O errors, not on parse errors
        if (cause instanceof java.io.IOException) {
            ctx.close();
        }
    }
}
