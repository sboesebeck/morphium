package de.caluga.morphium.driver.commands.result;

import java.util.Map;

public class RunCommandResult<T extends RunCommandResult<T>> {
    private Map<String, Object> metadata;
    private String server;
    private long duration;

    private int messageId;

    public String getServer() {
        return server;
    }

    @SuppressWarnings("unchecked")
    public T setServer(String server) {
        this.server = server;
        return (T) this;
    }

    public long getDuration() {
        return duration;
    }

    @SuppressWarnings("unchecked")
    public T setDuration(long duration) {
        this.duration = duration;
        return (T) this;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    @SuppressWarnings("unchecked")
    public T setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
        return (T) this;
    }

    public int getMessageId() {
        return messageId;
    }

    @SuppressWarnings("unchecked")
    public T setMessageId(int messageId) {
        this.messageId = messageId;
        return (T) this;
    }
}
