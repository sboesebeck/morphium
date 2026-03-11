package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.Map;

import org.slf4j.LoggerFactory;

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

    @Override
    public GetMoreMongoCommand fromMap(Map<String, Object> m) {
        super.fromMap(m);

        if (m.get(getCommandName()) instanceof String) {
            try {
                cursorId = Long.parseLong((String)m.get(getCommandName()));
            } catch (Exception e) {
                LoggerFactory.getLogger(GetMoreMongoCommand.class).error("Cursorid is wrong {}", m.get(getCommandName()));
                cursorId = 0;
            }
        } else {
            cursorId = (Long)m.get(getCommandName());
        }

        setColl((String) m.get("collection"));
        return this;
    }

    public MorphiumCursor execute() throws MorphiumDriverException {
        int c = getConnection().sendCommand(this);
        return getConnection().getAnswerFor(c, getBatchSize());
    }

}
