package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.MorphiumDriver;

public class GetMoreMongoCommand extends MongoCommand<GetMoreMongoCommand> {
    private long cursorId;
    private Integer batchSize;
    private Integer maxTimeMs;

    public GetMoreMongoCommand(MorphiumDriver d) {
        super(d);
    }

    public long getCursorId() {
        return cursorId;
    }

    public GetMoreMongoCommand setCursorId(long cursorId) {
        this.cursorId = cursorId;
        return this;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public GetMoreMongoCommand setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public Integer getMaxTimeMs() {
        return maxTimeMs;
    }

    public GetMoreMongoCommand setMaxTimeMs(Integer maxTimeMs) {
        this.maxTimeMs = maxTimeMs;
        return this;
    }

    @Override
    public String getCommandName() {
        return "getMore";
    }
}
