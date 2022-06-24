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


    @Override
    public String getCommandName() {
        return "count";
    }

    @Override
    public List<Map<String, Object>> executeGetResult() throws MorphiumDriverException {
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
            return List.of(Doc.of("n", fs.executeGetResult().size()));
        }
        return super.executeGetResult();
    }

    @Override
    public MorphiumCursor execute() throws MorphiumDriverException {
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
            return new SingleElementCursor(Doc.of("n", fs.executeGetResult().size()));
        }
        return super.execute();
    }

    @Override
    public int executeGetMsgID() throws MorphiumDriverException {
        if (getDriver().isTransactionInProgress()) {
            throw new MorphiumDriverException("Count during transaction is not allowed");
        }
        return super.executeGetMsgID();
    }
}
