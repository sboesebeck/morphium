package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.sync.NetworkCallHelper;
import de.caluga.morphium.driver.wireprotocol.OpMsg;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DistinctMongoCommand extends MongoCommand<DistinctMongoCommand> {
    private String key;
    private Doc query;
    private Doc readConcern;
    private Doc collation;

    public DistinctMongoCommand(MorphiumDriver d) {
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

    public Doc getQuery() {
        return query;
    }

    public DistinctMongoCommand setQuery(Doc query) {
        this.query = query;
        return this;
    }

    public Doc getReadConcern() {
        return readConcern;
    }

    public DistinctMongoCommand setReadConcern(Doc readConcern) {
        this.readConcern = readConcern;
        return this;
    }

    public Doc getCollation() {
        return collation;
    }

    public DistinctMongoCommand setCollation(Doc collation) {
        this.collation = collation;
        return this;
    }


    public List<Object> execute() throws MorphiumDriverException {
        MorphiumDriver driver = getDriver();
        if (driver == null) throw new IllegalArgumentException("you need to set the driver!");
        //noinspection unchecked
        return new NetworkCallHelper<List<Object>>().doCall(() -> {
            setMetaData(Doc.of("server", driver.getHostSeed()[0]));
            long start = System.currentTimeMillis();
            var res = driver.runCommandSingleResult(getDb(), asMap());
            long dur = System.currentTimeMillis() - start;
            getMetaData().put("duration", dur);
            return (List<Object>) res.get("values");
        }, driver.getRetriesOnNetworkError(), driver.getSleepBetweenErrorRetries());
    }
}
