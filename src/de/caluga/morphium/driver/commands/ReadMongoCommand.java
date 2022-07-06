package de.caluga.morphium.driver.commands;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.sync.NetworkCallHelper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class ReadMongoCommand<T extends MongoCommand> extends MongoCommand<T> implements MultiResultCommand, Iterable<Map<String, Object>> {
    private ReadPreference readPreference;

    public ReadMongoCommand(MorphiumDriver d) {
        super(d);
    }

    public ReadPreference getReadPreference() {
        return readPreference;
    }

    public T setReadPreference(ReadPreference readPreference) {
        this.readPreference = readPreference;
        return (T) this;
    }

    @Override
    public Iterator<Map<String, Object>> iterator() {
        try {
            return executeIterable();
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Map<String, Object>> execute() throws MorphiumDriverException {
        MorphiumDriver driver = getDriver();
        if (driver == null) throw new IllegalArgumentException("you need to set the driver!");
        //noinspection unchecked
        return new NetworkCallHelper<List<Map<String, Object>>>().doCall(() -> {
            List<Map<String, Object>> ret = new ArrayList<>();
            setMetaData(Doc.of("server", driver.getHostSeed().get(0)));
            long start = System.currentTimeMillis();
            MorphiumCursor crs = driver.runCommand(getDb(), asMap()).getCursor();
            while (crs.hasNext()) {
                List<Map<String, Object>> batch = crs.getBatch();
                if (batch.size() == 1 && batch.get(0).containsKey("ok") && batch.get(0).get("ok").equals(Double.valueOf(0))) {
                    throw new MorphiumDriverException("Error: " + batch.get(0).get("code") + ": " + batch.get(0).get("errmsg"));
                }
                ret.addAll(batch);
                crs.ahead(batch.size());
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
            List<Map<String, Object>> ret = new ArrayList<>();
        setMetaData(Doc.of("server", driver.getHostSeed().get(0)));
        long start = System.currentTimeMillis();
        MorphiumCursor crs = driver.runCommand(getDb(), asMap()).getCursor();
        long dur = System.currentTimeMillis() - start;
            getMetaData().put("duration", dur);
            return crs;
    }

}
