package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.List;
import java.util.Map;

public class CurrentOpCommand extends MongoCommand<CurrentOpCommand> {
    private int secsRunning;
    private Map<String, Object> filter;

    public CurrentOpCommand(MongoConnection d) {
        super(d);
    }

    public int getSecsRunning() {
        return secsRunning;
    }

    public CurrentOpCommand setSecsRunning(int secsRunning) {
        this.secsRunning = secsRunning;
        return this;
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

    public List<Map<String, Object>> execute() throws MorphiumDriverException {
        var msgid = getConnection().sendCommand(asMap());
        var res = getConnection().readSingleAnswer(msgid);
        return (List<Map<String, Object>>) res.get("inprog");
    }
}
