package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wire.NetworkCallHelper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class ReadMongoCommand<T extends MongoCommand> extends MongoCommand<T> implements MultiResultCommand, Iterable<Map<String, Object>> {
    private ReadPreference readPreference;

    public ReadMongoCommand(MongoConnection d) {
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
            return executeIterable(getConnection().getDriver().getDefaultBatchSize());
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Map<String, Object>> execute() throws MorphiumDriverException {
        MongoConnection connection = getConnection();
        if (connection == null) throw new IllegalArgumentException("you need to set the connection!");
        //noinspection unchecked
        return new NetworkCallHelper<List<Map<String, Object>>>().doCall(() -> {
            List<Map<String, Object>> ret = new ArrayList<>();
            setMetaData("server", connection.getConnectedTo());
            long start = System.currentTimeMillis();
            var msg = connection.sendCommand(this);
            MorphiumCursor crs = connection.getAnswerFor(msg, getConnection().getDriver().getDefaultBatchSize());
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
        }, connection.getDriver().getRetriesOnNetworkError(), connection.getDriver().getSleepBetweenErrorRetries());
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
