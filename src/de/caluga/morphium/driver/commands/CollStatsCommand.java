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

    public CollStatsCommand(MorphiumDriver drv) {
        super(null);
        this.drv = drv;
    }

    @Override
    public int executeAsync() throws MorphiumDriverException {
        if (getConnection() != null) {
            return super.executeAsync();
        }
        MongoConnection con = null;
        try {
            con = drv.getPrimaryConnection(null);
            setConnection(con);
            var ret = super.executeAsync();
            setConnection(null);
            con.release();
            return ret;
        } finally {
            if (con != null)
                con.release();
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
        if (getConnection() != null) {
            var msgid = getConnection().sendCommand(asMap());
            return getConnection().readSingleAnswer(msgid);
        }
        MongoConnection con = null;
        try {
            con = drv.getPrimaryConnection(null);

            var msgid = con.sendCommand(asMap());
            var ret = con.readSingleAnswer(msgid);
            return ret;
        } finally {
            if (con != null) con.release();
        }
    }
}
