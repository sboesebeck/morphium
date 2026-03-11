package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.ExplainCommand.ExplainVerbosity;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wire.NetworkCallHelper;

import java.util.List;
import java.util.Map;

public class DistinctMongoCommand extends MongoCommand<DistinctMongoCommand> {
    private String key;
    private Map<String, Object> query;
    private Map<String, Object> readConcern;
    private Map<String, Object> collation;

    public DistinctMongoCommand(MongoConnection d) {
        super(d);
    }

    @Override
    public String getCommandName() {
        return "distinct";
    }

    public String getKey() {
        return key;
    }

    public DistinctMongoCommand setKey(String key) {
        this.key = key;
        return this;
    }

    public Map<String, Object> getQuery() {
        return query;
    }

    public DistinctMongoCommand setQuery(Map<String, Object> query) {
        this.query = query;
        return this;
    }

    public Map<String, Object> getReadConcern() {
        return readConcern;
    }

    public DistinctMongoCommand setReadConcern(Map<String, Object> readConcern) {
        this.readConcern = readConcern;
        return this;
    }

    public Map<String, Object> getCollation() {
        return collation;
    }

    public DistinctMongoCommand setCollation(Map<String, Object> collation) {
        this.collation = collation;
        return this;
    }

    public Map<String,Object> explain(ExplainVerbosity verbosity) throws MorphiumDriverException{
        ExplainCommand explainCommand = new ExplainCommand(getConnection());
        explainCommand.setVerbosity(verbosity);
        var m=asMap();
        m.remove("$db");
        m.remove("coll");
        explainCommand.setCommand(m);
        explainCommand.setDb(getDb()).setColl(getColl());
        int msg=explainCommand.executeAsync();
        return explainCommand.getConnection().readSingleAnswer(msg);
    }
    public List<Object> execute() throws MorphiumDriverException {
        MongoConnection connection = getConnection();
        if (connection == null) throw new IllegalArgumentException("you need to set the connection!");
        //noinspection unchecked
        return new NetworkCallHelper<List<Object>>().doCall(() -> {
            setMetaData("server", connection.getConnectedTo());
            long start = System.currentTimeMillis();
            var msg = connection.sendCommand(this);
            var res = connection.readSingleAnswer(msg);
            long dur = System.currentTimeMillis() - start;
            setMetaData("duration", dur);
            @SuppressWarnings("unchecked")
            List<Object> values = (List<Object>) res.get("values");
            return values;
        }, connection.getDriver().getRetriesOnNetworkError(), connection.getDriver().getSleepBetweenErrorRetries());
    }
}
