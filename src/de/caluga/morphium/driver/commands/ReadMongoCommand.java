package de.caluga.morphium.driver.commands;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.sync.NetworkCallHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class ReadMongoCommand<T extends MongoCommand> extends MongoCommand<T> implements MultiResultCommand {


    public ReadMongoCommand(MorphiumDriver d) {
        super(d);
    }

    public List<Map<String, Object>> execute() throws MorphiumDriverException {
        MorphiumDriver driver = getDriver();
        if (driver == null) throw new IllegalArgumentException("you need to set the driver!");
        //noinspection unchecked
        return new NetworkCallHelper<List<Map<String, Object>>>().doCall(() -> {
            List<Map<String, Object>> ret = new ArrayList<>();
            setMetaData(Doc.of("server", driver.getHostSeed()[0]));
            long start = System.currentTimeMillis();
            MorphiumCursor crs = driver.runCommand(getDb(), asMap());
            while (crs.hasNext()) {
                ret.addAll(crs.getBatch());
                crs.ahead(crs.getBatch().size());
            }
            long dur = System.currentTimeMillis() - start;
            getMetaData().put("duration", dur);
            return ret;
        }, driver.getRetriesOnNetworkError(), driver.getSleepBetweenErrorRetries());
    }

    @Override
    public MorphiumCursor executeIterable() throws MorphiumDriverException {
        MorphiumDriver driver = getDriver();
        if (driver == null) throw new IllegalArgumentException("you need to set the driver!");
        //noinspection unchecked
        return new NetworkCallHelper<MorphiumCursor>().doCall(() -> {
            List<Map<String, Object>> ret = new ArrayList<>();
            setMetaData(Doc.of("server", driver.getHostSeed()[0]));
            long start = System.currentTimeMillis();
            MorphiumCursor crs = driver.runCommand(getDb(), asMap());
            long dur = System.currentTimeMillis() - start;
            getMetaData().put("duration", dur);
            return crs;
        }, driver.getRetriesOnNetworkError(), driver.getSleepBetweenErrorRetries());
    }

}