package de.caluga.morphium.driver.commands;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wire.NetworkCallHelper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class ReadMongoCommand<T extends MongoCommand> extends MongoCommand<T> implements MultiResultCommand, Iterable<Map<String, Object>> {

    public ReadMongoCommand(MongoConnection d) {
        super(d);
    }



    @Override
    public Iterator<Map<String, Object>> iterator() {
        return executeIterable(getConnection().getDriver().getDefaultBatchSize());
    }

    public List<Map<String, Object>> execute() throws MorphiumDriverException {
        MongoConnection connection = getConnection();
        if (connection == null) throw new IllegalArgumentException("you need to set the connection!");
        MorphiumDriver driver = connection.getDriver();
        //noinspection unchecked
        return new NetworkCallHelper<List<Map<String, Object>>>().doCall(() -> {
            // If the connection was closed (e.g. due to a transient transaction error on
            // a previous attempt), acquire a fresh connection from the pool.
            MongoConnection activeCon = getConnection();
            if (activeCon == null || !activeCon.isConnected()) {
                activeCon = driver.getReadConnection(driver.getDefaultReadPreference());
                setConnection(activeCon);
            }
            List<Map<String, Object>> ret = new ArrayList<>();
            setMetaData("server", activeCon.getConnectedTo());
            long start = System.currentTimeMillis();
            var msg = activeCon.sendCommand(this);
            MorphiumCursor crs = activeCon.getAnswerFor(msg, driver.getDefaultBatchSize());
            while (crs.hasNext()) {
                List<Map<String, Object>> batch = crs.getBatch();
                if (batch.size() == 1 && batch.get(0).containsKey("ok") && batch.get(0).get("ok").equals((double) 0)) {
                    throw new MorphiumDriverException("Error: " + batch.get(0).get("code") + ": " + batch.get(0).get("errmsg"));
                }
                ret.addAll(batch);
                crs.ahead(batch.size());
            }
            setConnection(null); //was released!
            long dur = System.currentTimeMillis() - start;
            setMetaData("duration", dur);
            return ret;
        }, driver.getRetriesOnNetworkError(), driver.getSleepBetweenErrorRetries());
    }

    @Override
    public MorphiumCursor executeIterable(int batchsize) throws MorphiumDriverException {
        MongoConnection connection = getConnection();
        if (connection == null) throw new IllegalArgumentException("you need to set the connection!");
        //noinspection unchecked
        List<Map<String, Object>> ret = new ArrayList<>();
        setMetaData("server", connection.getConnectedTo());
        long start = System.currentTimeMillis();
        int msg = connection.sendCommand(this);
        MorphiumCursor crs = connection.getAnswerFor(msg, batchsize);
        long dur = System.currentTimeMillis() - start;
        setMetaData("duration", dur);
        return crs;
    }

}
