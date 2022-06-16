package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;

import java.util.List;
import java.util.Map;

public class AggregateCmdSettings<T> extends CmdSettings<AggregateCmdSettings> {
    private List<Doc> pipeline;
    private Boolean explain;
    private Boolean allowDiskUse;
    private Integer maxWaitTime;
    private Boolean bypassDocumentValidation;
    private Doc readConvern;
    private Doc collation;
    private Object hint;
    private Doc writeConern;
    private Doc let;
    private Integer batchSize;
    private Doc cursor;


    public Integer getBatchSize() {
        return batchSize;
    }

    public AggregateCmdSettings<T> setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public List<Doc> getPipeline() {
        return pipeline;
    }

    public AggregateCmdSettings setPipeline(List<Doc> pipeline) {
        this.pipeline = pipeline;
        return this;
    }

    public Boolean getExplain() {
        return explain;
    }

    public AggregateCmdSettings setExplain(Boolean explain) {
        this.explain = explain;
        return this;
    }

    public Boolean getAllowDiskUse() {
        return allowDiskUse;
    }

    public AggregateCmdSettings setAllowDiskUse(Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    public Integer getMaxWaitTime() {
        return maxWaitTime;
    }

    public AggregateCmdSettings setMaxWaitTime(Integer maxWaitTime) {
        this.maxWaitTime = maxWaitTime;
        return this;
    }

    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    public AggregateCmdSettings setBypassDocumentValidation(Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    public Doc getReadConvern() {
        return readConvern;
    }

    public AggregateCmdSettings setReadConvern(Doc readConvern) {
        this.readConvern = readConvern;
        return this;
    }

    public Doc getCollation() {
        return collation;
    }

    public AggregateCmdSettings setCollation(Doc collation) {
        this.collation = collation;
        return this;
    }

    public Object getHint() {
        return hint;
    }

    public AggregateCmdSettings setHint(Object hint) {
        this.hint = hint;
        return this;
    }

    public Doc getWriteConern() {
        return writeConern;
    }

    public AggregateCmdSettings setWriteConern(Doc writeConern) {
        this.writeConern = writeConern;
        return this;
    }

    public Doc getLet() {
        return let;
    }

    public AggregateCmdSettings setLet(Doc let) {
        this.let = let;
        return this;
    }

    public Doc getCursor() {
        return cursor;
    }

    public AggregateCmdSettings setCursor(Doc cursor) {
        this.cursor = cursor;
        return this;
    }

}
