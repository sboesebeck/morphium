package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;

import java.util.List;
import java.util.Map;

public class AggregateMongoCommand extends MongoCommand<AggregateMongoCommand> {
    private List<Doc> pipeline;
    private Boolean explain;
    private Boolean allowDiskUse;
    private Integer maxWaitTime;
    private Boolean bypassDocumentValidation;
    private Doc readConcern;
    private Doc collation;
    private Object hint;
    private Doc writeConern;
    private Doc let;
    private Integer batchSize;
    private Doc cursor;


    public Integer getBatchSize() {
        return batchSize;
    }

    public AggregateMongoCommand setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public List<Doc> getPipeline() {
        return pipeline;
    }

    public AggregateMongoCommand setPipeline(List<Doc> pipeline) {
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

    public Doc getReadConcern() {
        return readConcern;
    }

    public AggregateMongoCommand setReadConcern(Doc readConcern) {
        this.readConcern = readConcern;
        return this;
    }

    public Doc getCollation() {
        return collation;
    }

    public AggregateMongoCommand setCollation(Doc collation) {
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

    public Doc getWriteConern() {
        return writeConern;
    }

    public AggregateMongoCommand setWriteConern(Doc writeConern) {
        this.writeConern = writeConern;
        return this;
    }

    public Doc getLet() {
        return let;
    }

    public AggregateMongoCommand setLet(Doc let) {
        this.let = let;
        return this;
    }

    public Doc getCursor() {
        return cursor;
    }

    public AggregateMongoCommand setCursor(Doc cursor) {
        this.cursor = cursor;
        return this;
    }

}
