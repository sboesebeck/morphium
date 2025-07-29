package de.caluga.morphium.messaging;

import org.slf4j.LoggerFactory;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;

/**
 * User: Stephan Bösebeck
 * Date: 21.04.18
 * Time: 22:26
 * <p>
 * TODO: Add documentation here
 */
public class MessageRejectedException extends RuntimeException {
    private boolean continueProcessing;
    private boolean sendAnswer;

    private RejectionHandler handler = null;

    public MessageRejectedException(String reason) {
        this(reason, false, false);
    }

    public MessageRejectedException(String reason, boolean continueProcessing) {
        this(reason, continueProcessing, false);
    }

    public MessageRejectedException(String reason, boolean continueProcessing, boolean sendAnswer) {
        super(reason);
        this.continueProcessing = continueProcessing;
        this.sendAnswer = sendAnswer;
        this.handler = (msg, m) -> {
            if (isSendAnswer()) {
                Msg answer = new Msg(m.getName(), "message rejected by listener", getMessage());
                m.sendAnswer(msg, answer);
            }

            if (isContinueProcessing()) {
                UpdateMongoCommand cmd = null;

                try {
                    if (!m.isExclusive()) {
                        cmd = new UpdateMongoCommand(msg.getMorphium().getDriver().getPrimaryConnection(msg.getMorphium().getWriteConcernForClass(Msg.class)));
                        cmd.setColl(msg.getCollectionName()).setDb(msg.getMorphium().getDatabase());
                        cmd.addUpdate(Doc.of("_id", m.getMsgId()), Doc.of("$addToSet", Doc.of("processed_by", msg.getSenderId())), null, false, false, null, null, null);
                        cmd.execute();
                        //not exclusive message is marked as processed by me
                    } else {
                        //releasing lock when exclusive - should not be checked until processing is removed
                        var ret = msg.getMorphium().createQueryFor(MsgLock.class, msg.getLockCollectionName()).f("_id").eq(m.getMsgId()).delete();
                    }
                } catch (MorphiumDriverException e) {
                    LoggerFactory.getLogger(msg.getClass()).error("Error unlocking message", e);
                } finally {
                    if (cmd != null) {
                        cmd.releaseConnection();
                    }
                }

                LoggerFactory.getLogger(msg.getClass()).debug(msg.getSenderId() + ": Message will be re-processed by others");
            }

        };
    }

    public MessageRejectedException setCustomRejectionHandler(RejectionHandler hdl) {
        handler = hdl;
        return this;
    }

    public RejectionHandler getRejectionHandler() {
        return handler;
    }

    public boolean isContinueProcessing() {
        return continueProcessing;
    }

    public MessageRejectedException setContinueProcessing(boolean continueProcessing) {
        this.continueProcessing = continueProcessing;
        return this;
    }

    public boolean isSendAnswer() {
        return sendAnswer;
    }

    public MessageRejectedException setSendAnswer(boolean sendAnswer) {
        this.sendAnswer = sendAnswer;
        return this;
    }

    public static interface RejectionHandler {
        public void handleRejection(Messaging msg, Msg m) throws Exception;
    }
}
