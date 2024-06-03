package de.caluga.morphium.driver.commands;

import de.caluga.morphium.annotations.Transient;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WatchCommand extends MongoCommand<WatchCommand> {
    @Transient
    private DriverTailableIterationCallback cb;
    private Integer batchSize;
    private List<Map<String, Object>> pipeline;
    private Boolean explain;
    private Boolean allowDiskUse;
    private Integer maxTimeMS;
    private Boolean bypassDocumentValidation;
    private Map<String, Object> readConcern;
    private Map<String, Object> collation;
    private Object hint;
    private Doc writeConern;
    private Doc let;
    private Doc cursor;
    private FullDocumentBeforeChangeEnum fullDocumentBeforeChange;
    private FullDocumentEnum fullDocument;
    private Boolean showExpandedEvents;

    public WatchCommand(MongoConnection d) {
        super(d);
    }

    public Boolean getShowExpandedEvents() {
        return showExpandedEvents;
    }

    public WatchCommand setShowExpandedEvents(Boolean showExpandedEvents) {
        this.showExpandedEvents = showExpandedEvents;
        return this;
    }

    public FullDocumentBeforeChangeEnum getFullDocumentBeforeChange() {
        return fullDocumentBeforeChange;
    }

    public WatchCommand setFullDocumentBeforeChange(FullDocumentBeforeChangeEnum fullDocumentBeforeChange) {
        this.fullDocumentBeforeChange = fullDocumentBeforeChange;
        return this;
    }

    public FullDocumentEnum getFullDocument() {
        return fullDocument;
    }

    public WatchCommand setFullDocument(FullDocumentEnum fullDocument) {
        this.fullDocument = fullDocument;
        return this;
    }

    public List<Map<String, Object>> getPipeline() {
        return pipeline;
    }

    public WatchCommand setPipeline(List<Map<String, Object>> pipeline) {
        this.pipeline = pipeline;
        return this;
    }

    public Boolean getExplain() {
        return explain;
    }

    public WatchCommand setExplain(Boolean explain) {
        this.explain = explain;
        return this;
    }

    public Boolean getAllowDiskUse() {
        return allowDiskUse;
    }

    public WatchCommand setAllowDiskUse(Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    public Integer getMaxTimeMS() {
        return maxTimeMS;
    }

    public WatchCommand setMaxTimeMS(Integer maxTimeMS) {
        this.maxTimeMS = maxTimeMS;
        return this;
    }

    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    public WatchCommand setBypassDocumentValidation(Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    public Map<String, Object> getReadConcern() {
        return readConcern;
    }

    public WatchCommand setReadConcern(Map<String, Object> readConcern) {
        this.readConcern = readConcern;
        return this;
    }

    public Map<String, Object> getCollation() {
        return collation;
    }

    public WatchCommand setCollation(Map<String, Object> collation) {
        this.collation = collation;
        return this;
    }

    public Object getHint() {
        return hint;
    }

    public WatchCommand setHint(Object hint) {
        this.hint = hint;
        return this;
    }

    public Doc getWriteConern() {
        return writeConern;
    }

    public WatchCommand setWriteConern(Doc writeConern) {
        this.writeConern = writeConern;
        return this;
    }

    public Doc getLet() {
        return let;
    }

    public WatchCommand setLet(Doc let) {
        this.let = let;
        return this;
    }

    public Doc getCursor() {
        return cursor;
    }

    public WatchCommand setCursor(Doc cursor) {
        this.cursor = cursor;
        return this;
    }

    public DriverTailableIterationCallback getCb() {
        return cb;
    }

    public WatchCommand setCb(DriverTailableIterationCallback cb) {
        this.cb = cb;
        return this;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public WatchCommand setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }


    public enum FullDocumentBeforeChangeEnum {
        whenAvailable, required, off
    }

    public enum FullDocumentEnum {
        updateLookup("updateLookup"), defaultValue("default"); //, whenAvailable("whenAvailable"), required("true");

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
        return "aggregate";
    }

    @Override
    public int executeAsync() throws MorphiumDriverException {
        throw new IllegalArgumentException("Please use watch()");
    }


    public void watch() throws MorphiumDriverException {
        getConnection().watch(this);
    }

    @Override
    public Map<String, Object> asMap() {
        var m = super.asMap();
        m.remove("cb");
        ArrayList<Map<String, Object>> localPipeline = new ArrayList<>();
        Doc changeStream = Doc.of();
        localPipeline.add(Doc.of("$changeStream", changeStream));

        if (getPipeline() != null && !getPipeline().isEmpty())
            localPipeline.addAll(getPipeline());

        Doc cmd = Doc.of("aggregate", getColl() == null ? (int)1 : getColl()).add("pipeline", localPipeline);

        if (fullDocument != null) {
            m.remove("fullDocument");
            changeStream.put("fullDocument", fullDocument.n);
        }

        if (fullDocumentBeforeChange != null) {
            m.remove("fullDocumentBeforeChange");
            changeStream.add("fullDocumentBeforeChange", fullDocumentBeforeChange.name());
        }

        if (showExpandedEvents != null) {
            m.remove("showExpandedEvents");
            changeStream.put("showExpandedEvents", showExpandedEvents);
        }

        if (cursor == null) {
            cmd.add("cursor", Doc.of("batchSize", batchSize == null ? getConnection().getDriver().getDefaultBatchSize() : batchSize));
        } //getDefaultBatchSize()

        m.remove("batchSize");
        m.putAll(cmd);
        return m;
    }

    @Override
    public WatchCommand fromMap(Map<String, Object> m) {
        super.fromMap(m);
        pipeline = new ArrayList<>((List<Map<String, Object>>)(List)m.get("pipeline"));
        var cstr = pipeline.remove(0);
        cstr = (Map<String, Object>) cstr.get("$changeStream");

        if (cstr.get("fullDocument") != null) {
            for (var fde : FullDocumentEnum.values()) {
                if (fde.n.equals(cstr.get("fullDocument"))) {
                    fullDocument = fde;
                }

                // fullDocument = FullDocumentEnum.valueOf((String) cstr.get("fullDocument"));
            }
        }

        if (cstr.containsKey("fullDocumentBeforeChange")) {
            fullDocumentBeforeChange = FullDocumentBeforeChangeEnum.valueOf(((String)cstr.get("fullDocumentBeforeChange")));
        }

        if (cstr.get("showExpandedEvents") instanceof Boolean) {
            showExpandedEvents = (Boolean) cstr.get("showExpandedEvents");
        } else {
            showExpandedEvents = "true".equals(cstr.get("showExpandedEvents"));
        }

        return this;
    }
}
