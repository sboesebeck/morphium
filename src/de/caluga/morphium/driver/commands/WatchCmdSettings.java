package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;

import java.util.List;

public class WatchCmdSettings extends CmdSettings<WatchCmdSettings> {
    private DriverTailableIterationCallback cb;
    private Integer batchSize;
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
    private Doc cursor;

    public List<Doc> getPipeline() {
        return pipeline;
    }

    public WatchCmdSettings setPipeline(List<Doc> pipeline) {
        this.pipeline = pipeline;
        return this;
    }

    public Boolean getExplain() {
        return explain;
    }

    public WatchCmdSettings setExplain(Boolean explain) {
        this.explain = explain;
        return this;
    }

    public Boolean getAllowDiskUse() {
        return allowDiskUse;
    }

    public WatchCmdSettings setAllowDiskUse(Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    public Integer getMaxWaitTime() {
        return maxWaitTime;
    }

    public WatchCmdSettings setMaxWaitTime(Integer maxWaitTime) {
        this.maxWaitTime = maxWaitTime;
        return this;
    }

    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    public WatchCmdSettings setBypassDocumentValidation(Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    public Doc getReadConvern() {
        return readConvern;
    }

    public WatchCmdSettings setReadConvern(Doc readConvern) {
        this.readConvern = readConvern;
        return this;
    }

    public Doc getCollation() {
        return collation;
    }

    public WatchCmdSettings setCollation(Doc collation) {
        this.collation = collation;
        return this;
    }

    public Object getHint() {
        return hint;
    }

    public WatchCmdSettings setHint(Object hint) {
        this.hint = hint;
        return this;
    }

    public Doc getWriteConern() {
        return writeConern;
    }

    public WatchCmdSettings setWriteConern(Doc writeConern) {
        this.writeConern = writeConern;
        return this;
    }

    public Doc getLet() {
        return let;
    }

    public WatchCmdSettings setLet(Doc let) {
        this.let = let;
        return this;
    }

    public Doc getCursor() {
        return cursor;
    }

    public WatchCmdSettings setCursor(Doc cursor) {
        this.cursor = cursor;
        return this;
    }

    public DriverTailableIterationCallback getCb() {
        return cb;
    }

    public WatchCmdSettings setCb(DriverTailableIterationCallback cb) {
        this.cb = cb;
        return this;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public WatchCmdSettings setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }
}
