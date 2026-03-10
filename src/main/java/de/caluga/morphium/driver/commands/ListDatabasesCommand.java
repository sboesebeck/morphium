package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ListDatabasesCommand extends MongoCommand<ListDatabasesCommand> {
    public ListDatabasesCommand(MongoConnection d) {
        super(d);
    }

    @Override
    public String getCommandName() {
        return "listDatabases";
    }

    public List<Map<String, Object>> getList() throws MorphiumDriverException {
        var ret = getConnection().sendCommand(this);
        var result = getConnection().readSingleAnswer(ret);
        if (result.containsKey("databases")) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> databases = (List<Map<String, Object>>) result.get("databases");
            return databases;
        }
        return null;

    }

    @Override
    public Map<String, Object> asMap() {
        var m = super.asMap();
        m.put(getCommandName(), "1");
        m.put("$db", "admin");
        return m;
    }
}
