package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.sync.DriverBase;
import de.caluga.morphium.driver.sync.NetworkCallHelper;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CountMongoCommand extends MongoCommand<CountMongoCommand> {
    private Doc query;
    private Integer limit;
    private Integer skip;
    private Object hint;
    private Doc readConcern;
    private Doc collation;

    public CountMongoCommand(DriverBase d) {
        super(d);
    }


    public Doc getQuery() {
        return query;
    }

    public CountMongoCommand setQuery(Doc query) {
        this.query = query;
        return this;
    }

    public Integer getLimit() {
        return limit;
    }

    public CountMongoCommand setLimit(Integer limit) {
        this.limit = limit;
        return this;
    }

    public Integer getSkip() {
        return skip;
    }

    public CountMongoCommand setSkip(Integer skip) {
        this.skip = skip;
        return this;
    }

    public Object getHint() {
        return hint;
    }

    public CountMongoCommand setHint(Object hint) {
        this.hint = hint;
        return this;
    }

    public Doc getReadConcern() {
        return readConcern;
    }

    public CountMongoCommand setReadConcern(Doc readConcern) {
        this.readConcern = readConcern;
        return this;
    }

    public Doc getCollation() {
        return collation;
    }

    public CountMongoCommand setCollation(Doc collation) {
        this.collation = collation;
        return this;
    }

    private Map<String, Object> doCount() throws MorphiumDriverException {
        if (getDriver().isTransactionInProgress()) {
            //log.warn("Cannot count while in transaction, will use IDlist!");
            //TODO: use Aggregation
            FindCommand fs = new FindCommand(getDriver());
            fs.setMetaData(getMetaData());
            fs.setDb(getDb());
            fs.setColl(getColl());
            fs.setFilter(getQuery());
            fs.setProjection(Doc.of("_id", 1)); //forcing ID-list
            fs.setCollation(getCollation());
            return Doc.of("n", fs.executeGetResult().size());
        }
        Map<String, Object> ret = new NetworkCallHelper().doCall(() -> {
            Doc doc = asMap("count");
            setMetaData("server", getDriver().getHostSeed()[0]);

            long start = System.currentTimeMillis();
            var result = getDriver().runCommand(getDb(), doc);
            setMetaData("duration", System.currentTimeMillis() - start);
            return result.getBatch().get(0);
        }, getDriver().getRetriesOnNetworkError(), getDriver().getSleepBetweenErrorRetries());
        return ret;
    }

    @Override
    public List<Map<String, Object>> executeGetResult() throws MorphiumDriverException {
        return Arrays.asList(doCount());

    }

    @Override
    public MorphiumCursor execute() throws MorphiumDriverException {
        var doc = doCount();
        return new SingleElementCursor(doc);
    }

    @Override
    public int executeGetMsgID() throws MorphiumDriverException {
        if (getDriver().isTransactionInProgress()) {
            throw new MorphiumDriverException("not possible during transaction");
        }
        Map<String, Object> ret = new NetworkCallHelper().doCall(() -> {
            Doc doc = asMap("count");
            setMetaData("server", getDriver().getHostSeed()[0]);

            long start = System.currentTimeMillis();
            var result = getDriver().sendCommand(getDb(), doc);
            setMetaData("duration", System.currentTimeMillis() - start);
            return Doc.of("id", result);
        }, getDriver().getRetriesOnNetworkError(), getDriver().getSleepBetweenErrorRetries());
        return (int) ret.get("id");
    }
}
