package de.caluga.morphium.driver.commands;

public class GetMoreCmdSettings extends CmdSettings<GetMoreCmdSettings> {
    private long cursorId;
    private Integer batchSize;
    private Integer maxTimeMs;

    public long getCursorId() {
        return cursorId;
    }

    public GetMoreCmdSettings setCursorId(long cursorId) {
        this.cursorId = cursorId;
        return this;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public GetMoreCmdSettings setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public Integer getMaxTimeMs() {
        return maxTimeMs;
    }

    public GetMoreCmdSettings setMaxTimeMs(Integer maxTimeMs) {
        this.maxTimeMs = maxTimeMs;
        return this;
    }
}
