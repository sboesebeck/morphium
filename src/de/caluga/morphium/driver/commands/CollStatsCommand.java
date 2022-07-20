package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.Map;

public class CollStatsCommand extends MongoCommand<CollStatsCommand> {
    private int scale = 1024;

    public CollStatsCommand(MongoConnection d) {
        super(d);
    }

    public int getScale() {
        return scale;
    }

    public CollStatsCommand setScale(int scale) {
        this.scale = scale;
        return this;
    }

    @Override
    public String getCommandName() {
        return "collStats";
    }

    public Map<String, Object> execute() throws MorphiumDriverException {
        var msgid = getConnection().sendCommand(asMap());
        return getConnection().readSingleAnswer(msgid);
    }
}
