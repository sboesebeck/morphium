package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.*;

import java.util.List;

public class WatchSettings extends MongoCommand<WatchSettings> {
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
    private FullDocumentBeforeChangeEnum fullDocumentBeforeChange;
    private FullDocumentEnum fullDocument;

    public WatchSettings() {
        super(null);
    }

    public FullDocumentBeforeChangeEnum getFullDocumentBeforeChange() {
        return fullDocumentBeforeChange;
    }

    public WatchSettings setFullDocumentBeforeChange(FullDocumentBeforeChangeEnum fullDocumentBeforeChange) {
        this.fullDocumentBeforeChange = fullDocumentBeforeChange;
        return this;
    }

    public FullDocumentEnum getFullDocument() {
        return fullDocument;
    }

    public WatchSettings setFullDocument(FullDocumentEnum fullDocument) {
        this.fullDocument = fullDocument;
        return this;
    }

    public List<Doc> getPipeline() {
        return pipeline;
    }

    public WatchSettings setPipeline(List<Doc> pipeline) {
        this.pipeline = pipeline;
        return this;
    }

    public Boolean getExplain() {
        return explain;
    }

    public WatchSettings setExplain(Boolean explain) {
        this.explain = explain;
        return this;
    }

    public Boolean getAllowDiskUse() {
        return allowDiskUse;
    }

    public WatchSettings setAllowDiskUse(Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    public Integer getMaxWaitTime() {
        return maxWaitTime;
    }

    public WatchSettings setMaxWaitTime(Integer maxWaitTime) {
        this.maxWaitTime = maxWaitTime;
        return this;
    }

    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    public WatchSettings setBypassDocumentValidation(Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    public Doc getReadConvern() {
        return readConvern;
    }

    public WatchSettings setReadConvern(Doc readConvern) {
        this.readConvern = readConvern;
        return this;
    }

    public Doc getCollation() {
        return collation;
    }

    public WatchSettings setCollation(Doc collation) {
        this.collation = collation;
        return this;
    }

    public Object getHint() {
        return hint;
    }

    public WatchSettings setHint(Object hint) {
        this.hint = hint;
        return this;
    }

    public Doc getWriteConern() {
        return writeConern;
    }

    public WatchSettings setWriteConern(Doc writeConern) {
        this.writeConern = writeConern;
        return this;
    }

    public Doc getLet() {
        return let;
    }

    public WatchSettings setLet(Doc let) {
        this.let = let;
        return this;
    }

    public Doc getCursor() {
        return cursor;
    }

    public WatchSettings setCursor(Doc cursor) {
        this.cursor = cursor;
        return this;
    }

    public DriverTailableIterationCallback getCb() {
        return cb;
    }

    public WatchSettings setCb(DriverTailableIterationCallback cb) {
        this.cb = cb;
        return this;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public WatchSettings setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }


    public enum FullDocumentBeforeChangeEnum {
        whenAvailable, required, off
    }

    public enum FullDocumentEnum {
        updateLookup("updateLookup"), whenAvailable("whenAvailable"), required("required"), defaultValue("default");

        String n;

        FullDocumentEnum(String name) {
            n = name;
        }

        public String toString() {
            return n;
        }
    }

    @Override
    public String getCommandName() {
        return null;
    }

    @Override
    public int executeAsync() throws MorphiumDriverException {
        throw new IllegalArgumentException("Please use DriverBase.watch()");
    }

}
