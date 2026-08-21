package de.caluga.poppydb.netty;

import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;

/**
 * Shared exceptionCaught logging for the wire-protocol channel handlers (#331).
 *
 * <p>A client dropping its connection - deploys, restarts, load balancers, crashed processes -
 * surfaces here as the IOException family (Connection reset by peer, Broken pipe, timeouts).
 * That is routine operation, not a server error: on ACC a single reconnect-looping client
 * produced 140 ERROR lines in 40 minutes of nothing but resets, and unconditional ERROR
 * logging is how poppy.log grew into the gigabytes. IO-classed causes are logged at DEBUG;
 * everything else (decode bugs, handler NPEs, ...) stays at ERROR with the full stack trace.
 *
 * <p>Deliberately says nothing about closing the channel - each handler keeps its own close
 * policy.
 */
final class NettyChannelLogging {

    private NettyChannelLogging() {
    }

    static void logChannelException(Logger log, String where, ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof java.io.IOException) {
            log.debug("{}: client I/O ended on {}: {}", where, ctx.channel().remoteAddress(), cause.getMessage());
        } else {
            log.error("{} error on {}: {}", where, ctx.channel().remoteAddress(), cause.getMessage(), cause);
        }
    }
}
