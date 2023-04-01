package de.caluga.morphium.driver.commands;

import de.caluga.morphium.annotations.Transient;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.List;
import java.util.Map;

public class CurrentOpCommand extends MongoCommand<CurrentOpCommand> {
    private int secsRunning;
    private Map<String, Object> filter;

    @Transient
    private MorphiumDriver drv;

    public CurrentOpCommand(MongoConnection d) {
        super(d);
    }

    public CurrentOpCommand(MorphiumDriver drv) {
        super(null);
        this.drv = drv;
    }

    public int getSecsRunning() {
        return secsRunning;
    }

    public CurrentOpCommand setSecsRunning(int secsRunning) {
        this.secsRunning = secsRunning;
        return this;
    }

    @Override
    public int executeAsync() throws MorphiumDriverException {
        if (getConnection() != null) {
            return super.executeAsync();
        }

        try {
            MongoConnection con = null;
            con = drv.getPrimaryConnection(null);
            setConnection(con);
            var ret = super.executeAsync();
            setConnection(null);
            return ret;
        } finally {
            // if (con != null) con.release();
        }
    }

    public Map<String, Object> getFilter() {
        return filter;
    }

    public CurrentOpCommand setFilter(Map<String, Object> filter) {
        this.filter = filter;
        return this;
    }

    @Override
    public String getCommandName() {
        return "currentOp";
    }

    @Override
    public Map<String, Object> asMap() {
        var m = super.asMap();
        m.put(getCommandName(), "1");
        m.remove("secsRunning");

        if (secsRunning != 0) {
            m.put("secs_running", Doc.of("$gt", secsRunning));
        }

        if (filter != null) {
            m.putAll(filter);
        }

        return m;
    }

    @Override
    public CurrentOpCommand fromMap(Map<String, Object> m) {
        super.fromMap(m);
        secsRunning = (int)((Map) m.get("secs_running")).get("$gt");
        return this;
    }

    public List<Map<String, Object>> execute() throws MorphiumDriverException {
        var msgid = getConnection().sendCommand(this);
        var res = getConnection().readSingleAnswer(msgid);
        return (List<Map<String, Object>>) res.get("inprog");
    }
}
