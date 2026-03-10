package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.ExplainCommand.ExplainVerbosity;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.Map;

public class CountMongoCommand extends MongoCommand<CountMongoCommand> implements SingleResultCommand {
    private Map<String, Object> query;
    private Integer limit;
    private Integer skip;
    private Object hint;
    private Map<String, Object> readConcern;
    private Map<String, Object> collation;

    public CountMongoCommand(MongoConnection d) {
        super(d);
    }

    public Map<String, Object> getQuery() {
        return query;
    }

    public CountMongoCommand setQuery(Map<String, Object> query) {
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

    public Map<String, Object> getReadConcern() {
        return readConcern;
    }

    public CountMongoCommand setReadConcern(Map<String, Object> readConcern) {
        this.readConcern = readConcern;
        return this;
    }

    public Map<String, Object> getCollation() {
        return collation;
    }

    public CountMongoCommand setCollation(Map<String, Object> collation) {
        this.collation = collation;
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CountMongoCommand fromMap(Map<String, Object> m) {
        super.fromMap(m);

        if (m.containsKey("query")) {
            setQuery((Map<String, Object>) m.get("query"));
        }

        if (m.containsKey("limit")) {
            setLimit(((Number) m.get("limit")).intValue());
        }

        if (m.containsKey("skip")) {
            setSkip(((Number) m.get("skip")).intValue());
        }

        if (m.containsKey("hint")) {
            setHint(m.get("hint"));
        }

        if (m.containsKey("readConcern")) {
            setReadConcern((Map<String, Object>) m.get("readConcern"));
        }

        if (m.containsKey("collation")) {
            setCollation((Map<String, Object>) m.get("collation"));
        }

        return this;
    }

    @Override
    public String getCommandName() {
        return "count";
    }

    public Map<String,Object> explain() throws MorphiumDriverException{
        return explain(null);
    }
    public Map<String, Object> explain(ExplainVerbosity verbosity) throws MorphiumDriverException {
        ExplainCommand explainCommand = new ExplainCommand(getConnection());
        explainCommand.setVerbosity(verbosity);
        var m = asMap();
        m.remove("$db");
        m.remove("coll");
        explainCommand.setCommand(m);
        explainCommand.setDb(getDb()).setColl(getColl());
        int msg = explainCommand.executeAsync();
        return explainCommand.getConnection().readSingleAnswer(msg);
    }

    @Override
    public Map<String, Object> execute() throws MorphiumDriverException {
        if (getConnection()==null) throw new RuntimeException("Connection not set!");
        if (getConnection().getDriver().isTransactionInProgress()) {
            // log.warn("Cannot count while in transaction, will use IDlist!");
            // TODO: use Aggregation
            FindCommand fs = new FindCommand(getConnection());
            fs.setMetaData(getMetaData());
            fs.setDb(getDb());
            fs.setColl(getColl());
            fs.setFilter(getQuery());
            fs.setProjection(Doc.of("_id", 1)); // forcing ID-list
            fs.setCollation(getCollation());
            return Doc.of("n", fs.execute().size());
        }

        int id = executeAsync();
        return getConnection().readSingleAnswer(id);
    }

    public int getCount() throws MorphiumDriverException {
        var ret = execute();

        if (ret == null || ret.get("n")==null) {
            return 0;
        }

        return (int) ret.get("n");
    }

    @Override
    public int executeAsync() throws MorphiumDriverException {
        if (getConnection().getDriver().isTransactionInProgress()) {
            throw new MorphiumDriverException("Count during transaction is not allowed");
        }

        return super.executeAsync();
    }
}
