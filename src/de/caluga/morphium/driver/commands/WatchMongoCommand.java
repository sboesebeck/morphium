package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriverException;

import java.util.List;
import java.util.Map;

public class WatchMongoCommand extends MongoCommand<WatchMongoCommand> {
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

    public FullDocumentBeforeChangeEnum getFullDocumentBeforeChange() {
        return fullDocumentBeforeChange;
    }

    public WatchMongoCommand setFullDocumentBeforeChange(FullDocumentBeforeChangeEnum fullDocumentBeforeChange) {
        this.fullDocumentBeforeChange = fullDocumentBeforeChange;
        return this;
    }

    public FullDocumentEnum getFullDocument() {
        return fullDocument;
    }

    public WatchMongoCommand setFullDocument(FullDocumentEnum fullDocument) {
        this.fullDocument = fullDocument;
        return this;
    }

    public List<Doc> getPipeline() {
        return pipeline;
    }

    public WatchMongoCommand setPipeline(List<Doc> pipeline) {
        this.pipeline = pipeline;
        return this;
    }

    public Boolean getExplain() {
        return explain;
    }

    public WatchMongoCommand setExplain(Boolean explain) {
        this.explain = explain;
        return this;
    }

    public Boolean getAllowDiskUse() {
        return allowDiskUse;
    }

    public WatchMongoCommand setAllowDiskUse(Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    public Integer getMaxWaitTime() {
        return maxWaitTime;
    }

    public WatchMongoCommand setMaxWaitTime(Integer maxWaitTime) {
        this.maxWaitTime = maxWaitTime;
        return this;
    }

    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    public WatchMongoCommand setBypassDocumentValidation(Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    public Doc getReadConvern() {
        return readConvern;
    }

    public WatchMongoCommand setReadConvern(Doc readConvern) {
        this.readConvern = readConvern;
        return this;
    }

    public Doc getCollation() {
        return collation;
    }

    public WatchMongoCommand setCollation(Doc collation) {
        this.collation = collation;
        return this;
    }

    public Object getHint() {
        return hint;
    }

    public WatchMongoCommand setHint(Object hint) {
        this.hint = hint;
        return this;
    }

    public Doc getWriteConern() {
        return writeConern;
    }

    public WatchMongoCommand setWriteConern(Doc writeConern) {
        this.writeConern = writeConern;
        return this;
    }

    public Doc getLet() {
        return let;
    }

    public WatchMongoCommand setLet(Doc let) {
        this.let = let;
        return this;
    }

    public Doc getCursor() {
        return cursor;
    }

    public WatchMongoCommand setCursor(Doc cursor) {
        this.cursor = cursor;
        return this;
    }

    public DriverTailableIterationCallback getCb() {
        return cb;
    }

    public WatchMongoCommand setCb(DriverTailableIterationCallback cb) {
        this.cb = cb;
        return this;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public WatchMongoCommand setBatchSize(Integer batchSize) {
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
    public List<Map<String, Object>> executeGetResult() throws MorphiumDriverException {
        throw new IllegalArgumentException("Please use DriverBase.watch()");
    }

    @Override
    public MorphiumCursor execute() throws MorphiumDriverException {
        throw new IllegalArgumentException("Please use DriverBase.watch()");
    }

    @Override
    public int executeGetMsgID() throws MorphiumDriverException {
        throw new IllegalArgumentException("Please use DriverBase.watch()");
    }
}
