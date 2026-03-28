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

/**
 * aggregate command
 */
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

    /**
     * Creates a new AggregateMongoCommand with the given connection.
     * @param d the connection
     */
    public AggregateMongoCommand(MongoConnection d) {
        super(d);
    }

    /**
     * Returns the batch size for cursor results.
     * @return batch size
     */
    public Integer getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the batch size for cursor results.
     * @param batchSize batch size to set
     * @return the command itself
     */
    public AggregateMongoCommand setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Returns the aggregation pipeline stages.
     * @return pipeline
     */
    public List<Map<String, Object>> getPipeline() {
        return pipeline;
    }

    /**
     * Sets the aggregation pipeline stages.
     * @param pipeline pipeline to set
     * @return the command itself
     */
    public AggregateMongoCommand setPipeline(List<Map<String, Object>> pipeline) {
        this.pipeline = pipeline;
        return this;
    }

    /**
     * Returns whether explain mode is enabled.
     * @return explain flag
     */
    public Boolean getExplain() {
        return explain;
    }

    /**
     * Sets the explain mode flag.
     * @param explain explain flag to set
     * @return the command itself
     */
    public AggregateMongoCommand setExplain(Boolean explain) {
        this.explain = explain;
        return this;
    }

    /**
     * Returns whether disk use is allowed for large aggregations.
     * @return allow disk use flag
     */
    public Boolean getAllowDiskUse() {
        return allowDiskUse;
    }

    /**
     * Sets whether disk use is allowed for large aggregations.
     * @param allowDiskUse flag to set
     * @return the command itself
     */
    public AggregateMongoCommand setAllowDiskUse(Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    /**
     * Returns the maximum wait time in milliseconds.
     * @return max wait time
     */
    public Integer getMaxWaitTime() {
        return maxWaitTime;
    }

    /**
     * Sets the maximum wait time in milliseconds.
     * @param maxWaitTime wait time to set
     * @return the command itself
     */
    public AggregateMongoCommand setMaxWaitTime(Integer maxWaitTime) {
        this.maxWaitTime = maxWaitTime;
        return this;
    }

    /**
     * Returns whether document validation is bypassed.
     * @return bypass document validation flag
     */
    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    /**
     * Sets whether to bypass document validation.
     * @param bypassDocumentValidation flag to set
     * @return the command itself
     */
    public AggregateMongoCommand setBypassDocumentValidation(Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    /**
     * Returns the read concern configuration.
     * @return read concern
     */
    public Map<String, Object> getReadConcern() {
        return readConcern;
    }

    /**
     * Sets the read concern configuration.
     * @param readConcern concern to set
     * @return the command itself
     */
    public AggregateMongoCommand setReadConcern(Map<String, Object> readConcern) {
        this.readConcern = readConcern;
        return this;
    }

    /**
     * Returns the collation settings for this command.
     * @return collation
     */
    public Map<String, Object> getCollation() {
        return collation;
    }

    /**
     * Sets the collation settings for this command.
     * @param collation collation to set
     * @return the command itself
     */
    public AggregateMongoCommand setCollation(Map<String, Object> collation) {
        this.collation = collation;
        return this;
    }

    /**
     * Returns the index hint for this command.
     * @return hint
     */
    public Object getHint() {
        return hint;
    }

    /**
     * Sets the index hint for this command.
     * @param hint hint to set
     * @return the command itself
     */
    public AggregateMongoCommand setHint(Object hint) {
        this.hint = hint;
        return this;
    }

    /**
     * Returns the write concern configuration.
     * @return write concern
     */
    public Map<String, Object> getWriteConern() {
        return writeConern;
    }

    /**
     * Sets the write concern configuration.
     * @param writeConern concern to set
     * @return the command itself
     */
    public AggregateMongoCommand setWriteConern(Map<String, Object> writeConern) {
        this.writeConern = writeConern;
        return this;
    }

    /**
     * Returns the let variables map for this command.
     * @return let map
     */
    public Map<String, Object> getLet() {
        return let;
    }

    /**
     * Sets the let variables map for this command.
     * @param let map to set
     * @return the command itself
     */
    public AggregateMongoCommand setLet(Map<String, Object> let) {
        this.let = let;
        return this;
    }

    /**
     * Returns the cursor configuration map.
     * @return cursor map
     */
    public Map<String, Object> getCursor() {
        return cursor;
    }

    /**
     * Sets the cursor configuration map.
     * @param cursor map to set
     * @return the command itself
     */
    public AggregateMongoCommand setCursor(Map<String, Object> cursor) {
        this.cursor = cursor;
        return this;
    }


    @Override
    public Map<String, Object> asMap() {
        var doc = super.asMap();
        doc.putIfAbsent("cursor", new Doc());
        if (getBatchSize() != null) {
            @SuppressWarnings("unchecked")
            Map<String, Object> cursorBatch = (Map<String, Object>) doc.get("cursor");
            cursorBatch.put("batchSize", getBatchSize());
            doc.remove("batchSize");
        } else {
            @SuppressWarnings("unchecked")
            Map<String, Object> cursorDefault = (Map<String, Object>) doc.get("cursor");
            cursorDefault.put("batchSize", getConnection().getDriver().getDefaultBatchSize());
        }
        return doc;
    }

    @Override
    public String getCommandName() {
        return "aggregate";
    }

    /**
     * Executes this command in explain mode with the given verbosity.
     * @param verbosity verbosity level
     * @return explain result
     * @throws MorphiumDriverException in case of error
     */
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
