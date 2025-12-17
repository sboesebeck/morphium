package de.caluga.morphium.driver.commands;

import de.caluga.morphium.annotations.Transient;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class GenericCommand extends MongoCommand<GenericCommand> {
    @Transient
    private String commandName = "not_set";
    @Transient
    private Map<String, Object> cmdData;

    public GenericCommand(MongoConnection c) {
        super(c);
    }

    @Override
    public String getCommandName() {
        return commandName;
    }

    public GenericCommand setCommandName(String n) {
        commandName = n;
        return this;
    }

    public GenericCommand setCmdData(Map<String, Object> cmd) {
        cmdData = cmd;
        return this;
    }

    public GenericCommand addKey(String key, Object value) {
        if (cmdData == null) cmdData = new LinkedHashMap<>();
        cmdData.put(key, value);
        return this;
    }

    @Override
    public Map<String, Object> asMap() {
        var m = Doc.of();
        m.putAll(super.asMap());
        if (cmdData != null)
            m.putAll(cmdData);
        return m;
    }

    @Override
    public GenericCommand fromMap(Map<String, Object> m) {
        super.fromMap(m);
        // Use LinkedHashMap to preserve key ordering - critical for MongoDB wire protocol
        // where the command name must be the first key
        cmdData = new LinkedHashMap<>();
        cmdData.putAll(m);
        commandName = m.keySet().toArray(new String[m.size()])[0];
        if (cmdData.get(commandName) instanceof String) {
            setColl((String) cmdData.remove(commandName));
        } else {
            Object removed = cmdData.remove(commandName);
            if (removed != null)
                setColl(removed.toString());
        }

        return this;
    }
}
