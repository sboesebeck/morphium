package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.Map;

public class GetMoreMongoCommand extends MongoCommand<GetMoreMongoCommand> {
    private long cursorId;
    private Integer batchSize;
    private Integer maxTimeMs;

    public GetMoreMongoCommand(MongoConnection d) {
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

    @Override
    public Map<String, Object> asMap() {
        var m = super.asMap();
        m.put(getCommandName(), cursorId);
        m.put("collection", getColl());
        m.remove("cursorId");
        return m;
    }

    public MorphiumCursor execute() throws MorphiumDriverException {
        int c = getConnection().sendCommand(asMap());
        return getConnection().getAnswerFor(c);
    }

}
