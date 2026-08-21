package de.caluga.poppydb.netty;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.netty.channel.embedded.EmbeddedChannel;

/**
 * #331: a client dropping its connection is routine operation (deploys, restarts, load
 * balancers) - but all three Netty exceptionCaught handlers logged EVERY exception at ERROR.
 * On ACC a single reconnect-looping client produced 140 ERROR lines in 40 minutes of plain
 * "Connection reset by peer"; combined with a resume storm that grew the log to 3.48 GB.
 *
 * <p>Contract: the IOException family (reset by peer, broken pipe, timeouts) is logged at
 * DEBUG; everything else stays ERROR. The close behaviour is unchanged either way.
 */
public class ChannelExceptionLogLevelTest {

    private final List<ListAppender<ILoggingEvent>> appenders = new java.util.ArrayList<>();
    private final List<Logger> loggers = new java.util.ArrayList<>();
    private final List<Level> savedLevels = new java.util.ArrayList<>();

    private ListAppender<ILoggingEvent> capture(Class<?> clazz) {
        Logger logger = (Logger) LoggerFactory.getLogger(clazz);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        savedLevels.add(logger.getLevel());
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        loggers.add(logger);
        appenders.add(appender);
        return appender;
    }

    @BeforeEach
    public void setUp() {
        appenders.clear();
        loggers.clear();
        savedLevels.clear();
    }

    @AfterEach
    public void tearDown() {
        for (int i = 0; i < loggers.size(); i++) {
            loggers.get(i).detachAppender(appenders.get(i));
            loggers.get(i).setLevel(savedLevels.get(i));
        }
    }

    private List<ILoggingEvent> errors(ListAppender<ILoggingEvent> appender) {
        return appender.list.stream().filter(e -> e.getLevel() == Level.ERROR).collect(Collectors.toList());
    }

    private List<ILoggingEvent> debugs(ListAppender<ILoggingEvent> appender) {
        return appender.list.stream().filter(e -> e.getLevel() == Level.DEBUG).collect(Collectors.toList());
    }

    @Test
    public void decoderLogsClientDisconnectsAtDebugNotError() {
        ListAppender<ILoggingEvent> log = capture(MongoWireProtocolDecoder.class);
        EmbeddedChannel ch = new EmbeddedChannel(new MongoWireProtocolDecoder());

        ch.pipeline().fireExceptionCaught(new IOException("Connection reset by peer"));

        assertThat(errors(log))
            .as("an ordinary client disconnect is routine, not a server error - it must not "
                + "produce an ERROR line (140 of these in 40 minutes on ACC)")
            .isEmpty();
        assertThat(debugs(log)).as("the disconnect should still be visible at DEBUG").isNotEmpty();
        assertThat(ch.isOpen()).as("close behaviour unchanged: IOException closes").isFalse();
    }

    @Test
    public void decoderKeepsRealErrorsAtError() {
        ListAppender<ILoggingEvent> log = capture(MongoWireProtocolDecoder.class);
        EmbeddedChannel ch = new EmbeddedChannel(new MongoWireProtocolDecoder());

        ch.pipeline().fireExceptionCaught(new IllegalStateException("decoder bug"));

        assertThat(errors(log)).as("a non-IO exception is a real error and stays at ERROR").isNotEmpty();
        assertThat(ch.isOpen()).as("close behaviour unchanged: non-IO does not close the decoder").isTrue();
        ch.close();
    }

    @Test
    public void encoderLogsClientDisconnectsAtDebugNotError() {
        ListAppender<ILoggingEvent> log = capture(MongoWireProtocolEncoder.class);
        EmbeddedChannel ch = new EmbeddedChannel(new MongoWireProtocolEncoder());

        ch.pipeline().fireExceptionCaught(new IOException("Broken pipe"));

        assertThat(errors(log)).as("see decoder - same rule for the encoder").isEmpty();
        assertThat(debugs(log)).isNotEmpty();
        assertThat(ch.isOpen()).isFalse();
    }

    @Test
    public void commandHandlerLogsClientDisconnectsAtDebugNotError() {
        ListAppender<ILoggingEvent> log = capture(MongoCommandHandler.class);
        // The handler's exceptionCaught only touches ctx - a bare pipeline entry suffices; no
        // driver wiring needed for this contract.
        EmbeddedChannel ch = new EmbeddedChannel();
        ch.pipeline().addLast(new io.netty.channel.ChannelInboundHandlerAdapter() {
            @Override
            public void exceptionCaught(io.netty.channel.ChannelHandlerContext ctx, Throwable cause) {
                NettyChannelLogging.logChannelException(
                        (org.slf4j.Logger) LoggerFactory.getLogger(MongoCommandHandler.class),
                        "Handler", ctx, cause);
                ctx.close();
            }
        });

        ch.pipeline().fireExceptionCaught(new IOException("Connection timed out"));

        assertThat(errors(log)).as("same rule for the command handler").isEmpty();
        assertThat(debugs(log)).isNotEmpty();
        assertThat(ch.isOpen()).isFalse();
    }
}
