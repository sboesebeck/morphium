package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.Map;

public class DbStatsCommand extends MongoCommand<DbStatsCommand> {

    private boolean withStorage = false;

    public DbStatsCommand(MongoConnection d) {
        super(d);
        setColl("ALL");
    }

    @Override
    public Map<String, Object> asMap() {
        var ret = super.asMap();

        if (getColl().equals("ALL")) {
            ret.put(getCommandName(), 1);
        }
        ret.put("scale", 1024);
        if (withStorage) {
            ret.put("freeStorage", 1);
        }

        return ret;
    }

    @Override
    public String getCommandName() {
        return "dbStats";
    }

    public DbStatsCommand setWithStorage(boolean storage) {
        withStorage = storage;
        return this;
    }

    public boolean isWithStorage() {
        return withStorage;
    }

    public Map<String, Object> execute() throws MorphiumDriverException {
        var msgid = getConnection().sendCommand(this);
        return getConnection().readSingleAnswer(msgid);
    }

}
