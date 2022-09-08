package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.Map;

public class DbStatsCommand extends MongoCommand<DbStatsCommand> {

    public DbStatsCommand(MongoConnection d) {
        super(d);
        setColl("1");
    }

    @Override
    public String getCommandName() {
        return "dbstats";
    }

    public Map<String, Object> execute() throws MorphiumDriverException {
        var msgid = getConnection().sendCommand(this);
        return getConnection().readSingleAnswer(msgid);
    }

}
