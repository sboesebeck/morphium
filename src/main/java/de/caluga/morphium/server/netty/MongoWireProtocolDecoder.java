package de.caluga.morphium.server.netty;

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
            log.error("Invalid message size: {}", messageSize);
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

        // Find the opcode handler
        OpCode code = OpCode.findByCode(opCode);
        if (code == null) {
            log.error("Unknown opcode: {}", opCode);
            ctx.close();
            return;
        }

        // Read payload
        int payloadSize = messageSize - HEADER_SIZE;
        byte[] payload = new byte[payloadSize];
        in.readBytes(payload);

        // Create message instance
        try {
            WireProtocolMessage message = code.handler.getDeclaredConstructor().newInstance();
            message.setSize(messageSize);
            message.setMessageId(requestId);
            message.setResponseTo(responseTo);
            message.parsePayload(payload, 0);

            log.debug("Decoded {} message, id={}, size={}", code.name(), requestId, messageSize);
            out.add(message);
        } catch (Exception e) {
            log.error("Failed to parse {} message: {}", code.name(), e.getMessage(), e);
            ctx.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Decoder error: {}", cause.getMessage());
        ctx.close();
    }
}
