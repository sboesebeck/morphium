package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wire.NetworkCallHelper;

import java.util.Map;

public abstract class AdminMongoCommand<T extends MongoCommand> extends MongoCommand<T> implements SingleResultCommand {
    public AdminMongoCommand(MongoConnection d) {
        super(d);
    }

    @Override
    public Map<String, Object> asMap() {
        var m = super.asMap();
        m.put("$db", "admin");
        return m;
    }


    public Map<String, Object> execute() throws MorphiumDriverException {
        MongoConnection connection = getConnection();
        //noinspection unchecked

        setMetaData("server", connection.getConnectedTo());
        long start = System.currentTimeMillis();
        var m = asMap();
        m.put("$db", "admin");
        var msg = connection.sendCommand(m);
        var crs = connection.readSingleAnswer(msg);
        long dur = System.currentTimeMillis() - start;
        setMetaData("duration", dur);
        return crs;
    }

    @Override
    public int executeAsync() throws MorphiumDriverException {
        MongoConnection connection = getConnection();
        if (connection == null) throw new IllegalArgumentException("you need to set the connection!");
        //noinspection unchecked
        return new NetworkCallHelper<Integer>().doCall(() -> {
            setMetaData("server", connection.getConnectedTo());
            //long start = System.currentTimeMillis();
            var m = asMap();
            m.put("$db", "admin");
            var id = connection.sendCommand(m);

            // long dur = System.currentTimeMillis() - start;
            setMetaData("duration", 0); //not waiting!
            return id;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }
}
