package de.caluga.morphium.server.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;
import de.caluga.morphium.driver.wireprotocol.OpCompressed;

/**
 * Netty encoder for MongoDB wire protocol messages.
 * Converts WireProtocolMessage objects to ByteBuf.
 */
public class MongoWireProtocolEncoder extends MessageToByteEncoder<WireProtocolMessage> {

    private static final Logger log = LoggerFactory.getLogger(MongoWireProtocolEncoder.class);
    private final int compressorId;

    public MongoWireProtocolEncoder() {
        this(OpCompressed.COMPRESSOR_NOOP);
    }

    public MongoWireProtocolEncoder(int compressorId) {
        this.compressorId = compressorId;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, WireProtocolMessage msg, ByteBuf out) throws Exception {
        byte[] bytes;

        if (compressorId != OpCompressed.COMPRESSOR_NOOP && !(msg instanceof OpCompressed)) {
            // Compress the message
            OpCompressed compressed = new OpCompressed();
            compressed.setMessageId(msg.getMessageId());
            compressed.setResponseTo(msg.getResponseTo());
            compressed.setCompressorId(compressorId);
            compressed.setOriginalOpCode(msg.getOpCode());

            byte[] originalPayload = msg.getPayload();
            compressed.setUncompressedSize(originalPayload.length);
            compressed.setCompressedMessage(originalPayload);

            bytes = compressed.bytes();
            log.debug("Encoding compressed message: {} bytes (uncompressed: {})", bytes.length, originalPayload.length);
        } else {
            bytes = msg.bytes();
            log.debug("Encoding message: {} bytes, id={}, responseTo={}",
                    bytes.length, msg.getMessageId(), msg.getResponseTo());
        }

        out.writeBytes(bytes);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Encoder error: {}", cause.getMessage());
        ctx.close();
    }
}
