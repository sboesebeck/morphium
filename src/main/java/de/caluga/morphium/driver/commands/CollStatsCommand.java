package de.caluga.morphium.driver.commands;

import de.caluga.morphium.annotations.Transient;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.Map;

public class CollStatsCommand extends MongoCommand<CollStatsCommand> {
    private int scale = 1024;
    @Transient
    private MorphiumDriver drv;

    public CollStatsCommand(MongoConnection d) {
        super(d);
    }

    @Override
    public int executeAsync() throws MorphiumDriverException {
        if (getConnection() != null) {
            return super.executeAsync();
        }

        try {
            var ret = super.executeAsync();
            return ret;
        } finally {
        }
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
        try {
            var msgid = getConnection().sendCommand(this);
            var ret = getConnection().readSingleAnswer(msgid);
            return ret;
        } finally {
            releaseConnection();
        }
    }
}
