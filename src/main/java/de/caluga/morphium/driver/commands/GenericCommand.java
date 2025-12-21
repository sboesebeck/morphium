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
        // Use LinkedHashMap to preserve key ordering
        cmdData = cmd instanceof LinkedHashMap ? cmd : new LinkedHashMap<>(cmd);
        // First key is the command name - critical for MongoDB wire protocol
        if (cmd != null && !cmd.isEmpty()) {
            commandName = cmd.keySet().iterator().next();
        }
        return this;
    }

    public GenericCommand addKey(String key, Object value) {
        if (cmdData == null) {
            cmdData = new LinkedHashMap<>();
            // First key added is the command name - critical for MongoDB wire protocol
            // where the command name must be the first key in the BSON document
            commandName = key;
        }
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
        Object commandValue = cmdData.get(commandName);
        if (commandValue instanceof String) {
            // Simple command where value is the collection name (e.g., find, insert, delete)
            setColl((String) cmdData.remove(commandName));
        } else if (commandValue instanceof Map) {
            // Complex command where value is a nested command (e.g., explain)
            // Keep the command data in cmdData, don't remove it
            // Don't set coll from a Map - the nested command has its own collection
        } else if (commandValue instanceof Number) {
            // Command with numeric value (e.g., getMore with cursor id)
            // Keep the value in cmdData
        } else if (commandValue != null) {
            // Other types - remove and set coll for backwards compatibility
            cmdData.remove(commandName);
            setColl(commandValue.toString());
        }

        return this;
    }
}
