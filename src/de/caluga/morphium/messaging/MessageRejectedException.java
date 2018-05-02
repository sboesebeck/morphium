package de.caluga.morphium.messaging;

/**
 * User: Stephan Bösebeck
 * Date: 21.04.18
 * Time: 22:26
 * <p>
 * TODO: Add documentation here
 */
public class MessageRejectedException extends RuntimeException {
    private boolean continueProcessing = false;
    private boolean sendAnswer = false;

    public MessageRejectedException(String reason){
        this(reason,false,false);
    }

    public MessageRejectedException(String reason,boolean continueProcessing){
        this(reason,continueProcessing,false);
    }
    public MessageRejectedException(String reason,boolean continueProcessing,boolean sendAnswer){
        super(reason);
        this.continueProcessing=continueProcessing;
        this.sendAnswer=sendAnswer;
    }

    public boolean isContinueProcessing() {
        return continueProcessing;
    }

    public void setContinueProcessing(boolean continueProcessing) {
        this.continueProcessing = continueProcessing;
    }

    public boolean isSendAnswer() {
        return sendAnswer;
    }

    public void setSendAnswer(boolean sendAnswer) {
        this.sendAnswer = sendAnswer;
    }
}
