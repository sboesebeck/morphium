package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.morphium.replicaset.ReplicaSetStatus;

import java.util.Map;

public class ReplicastStatusCommand extends MongoCommand<ReplicastStatusCommand> {
    public ReplicastStatusCommand(MongoConnection d) {
        super(d);
    }

    @Override
    public String getCommandName() {
        return "replSetGetStatus";
    }

    public Map<String, Object> execute() throws MorphiumDriverException {
        var msgId = getConnection().sendCommand(this);
        var result = getConnection().readSingleAnswer(msgId);
        return result;
    }

    @Override
    public Map<String, Object> asMap() {
        var m = super.asMap();
        //"admin", Doc.of("replSetGetStatus", 1)
        m.put("$db", "admin");
        m.put(getCommandName(), 1);
        return m;
    }
}
