package de.caluga.morphium.driver.commands;

import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Transient;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.ExplainCommand.ExplainVerbosity;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class AggregateMongoCommand extends ReadMongoCommand<AggregateMongoCommand> implements MultiResultCommand {
    private List<Map<String, Object>> pipeline;
    private Boolean explain;
    private Boolean allowDiskUse;
    private Integer maxWaitTime;
    private Boolean bypassDocumentValidation;
    private Map<String, Object> readConcern;
    private Map<String, Object> collation;
    private Object hint;
    private Map<String, Object> writeConern;
    private Map<String, Object> let;
    private Integer batchSize;
    private Map<String, Object> cursor;

    public AggregateMongoCommand(MongoConnection d) {
        super(d);
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public AggregateMongoCommand setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public List<Map<String, Object>> getPipeline() {
        return pipeline;
    }

    public AggregateMongoCommand setPipeline(List<Map<String, Object>> pipeline) {
        this.pipeline = pipeline;
        return this;
    }

    public Boolean getExplain() {
        return explain;
    }

    public AggregateMongoCommand setExplain(Boolean explain) {
        this.explain = explain;
        return this;
    }

    public Boolean getAllowDiskUse() {
        return allowDiskUse;
    }

    public AggregateMongoCommand setAllowDiskUse(Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    public Integer getMaxWaitTime() {
        return maxWaitTime;
    }

    public AggregateMongoCommand setMaxWaitTime(Integer maxWaitTime) {
        this.maxWaitTime = maxWaitTime;
        return this;
    }

    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    public AggregateMongoCommand setBypassDocumentValidation(Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    public Map<String, Object> getReadConcern() {
        return readConcern;
    }

    public AggregateMongoCommand setReadConcern(Map<String, Object> readConcern) {
        this.readConcern = readConcern;
        return this;
    }

    public Map<String, Object> getCollation() {
        return collation;
    }

    public AggregateMongoCommand setCollation(Map<String, Object> collation) {
        this.collation = collation;
        return this;
    }

    public Object getHint() {
        return hint;
    }

    public AggregateMongoCommand setHint(Object hint) {
        this.hint = hint;
        return this;
    }

    public Map<String, Object> getWriteConern() {
        return writeConern;
    }

    public AggregateMongoCommand setWriteConern(Map<String, Object> writeConern) {
        this.writeConern = writeConern;
        return this;
    }

    public Map<String, Object> getLet() {
        return let;
    }

    public AggregateMongoCommand setLet(Map<String, Object> let) {
        this.let = let;
        return this;
    }

    public Map<String, Object> getCursor() {
        return cursor;
    }

    public AggregateMongoCommand setCursor(Map<String, Object> cursor) {
        this.cursor = cursor;
        return this;
    }


    @Override
    public Map<String, Object> asMap() {
        var doc = super.asMap();
        doc.putIfAbsent("cursor", new Doc());
        if (getBatchSize() != null) {
            ((Map) doc.get("cursor")).put("batchSize", getBatchSize());
            doc.remove("batchSize");
        } else {
            ((Map) doc.get("cursor")).put("batchSize", getConnection().getDriver().getDefaultBatchSize());
        }
        return doc;
    }

    @Override
    public String getCommandName() {
        return "aggregate";
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
    @Override
    public AggregateMongoCommand fromMap(Map<String, Object> m) {
        super.fromMap(m);
        if (m.containsKey("cursor")){
            batchSize= (Integer) ((Map)m.get("cursor")).get("batchSize");
        }
        return this;
    }
}
