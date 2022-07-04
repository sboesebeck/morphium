package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.sync.NetworkCallHelper;

import java.util.Map;

public abstract class AdminMongoCommand<T extends MongoCommand> extends MongoCommand<T> implements SingleResultCommand {
    public AdminMongoCommand(MorphiumDriver d) {
        super(d);
    }

    @Override
    public Map<String, Object> asMap() {
        var m = super.asMap();
        m.put("$db", "admin");
        return m;
    }


    public Map<String, Object> execute() throws MorphiumDriverException {
        MorphiumDriver driver = getDriver();
        //noinspection unchecked

        setMetaData(Doc.of("server", driver.getHostSeed()[0]));
        long start = System.currentTimeMillis();
        MorphiumCursor crs = driver.runCommand("admin", asMap());
        long dur = System.currentTimeMillis() - start;
        getMetaData().put("duration", dur);
        return crs.next();
    }

    @Override
    public int executeAsync() throws MorphiumDriverException {
        MorphiumDriver driver = getDriver();
        if (driver == null) throw new IllegalArgumentException("you need to set the driver!");
        //noinspection unchecked
        return new NetworkCallHelper<Integer>().doCall(() -> {
            setMetaData(Doc.of("server", driver.getHostSeed()[0]));
            //long start = System.currentTimeMillis();
            int id = driver.sendCommand("admin", asMap());
            // long dur = System.currentTimeMillis() - start;
            getMetaData().put("duration", 0); //not waiting!
            return id;
        }, driver.getRetriesOnNetworkError(), driver.getSleepBetweenErrorRetries());
    }
}
