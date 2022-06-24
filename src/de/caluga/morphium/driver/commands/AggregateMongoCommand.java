package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.sync.DriverBase;
import de.caluga.morphium.driver.sync.NetworkCallHelper;
import de.caluga.morphium.driver.sync.SynchronousConnectCursor;
import de.caluga.morphium.driver.sync.SynchronousMongoConnection;
import de.caluga.morphium.driver.wireprotocol.OpMsg;

import java.util.ArrayList;
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

    public AggregateMongoCommand(DriverBase d) {
        super(d);
    }


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

    @Override
    public List<Map<String, Object>> executeGetResult() throws MorphiumDriverException {
        return aggregate();
    }

    @Override
    public MorphiumCursor execute() throws MorphiumDriverException {
        return initAggregationIteration();
    }

    @Override
    public int executeGetMsgID() throws MorphiumDriverException {
        return 0;
    }

    public MorphiumCursor initAggregationIteration() throws MorphiumDriverException {
        return new NetworkCallHelper<MorphiumCursor>().doCall(() -> {
            setMetaData("server", getDriver().getHostSeed()[0]);
            Doc doc = asMap("aggregate");
            doc.putIfAbsent("cursor", new Doc());
            if (getBatchSize() != null) {
                ((Map) doc.get("cursor")).put("batchSize", getBatchSize());
                doc.remove("batchSize");
            } else {
                ((Map) doc.get("cursor")).put("batchSize", getDriver().getDefaultBatchSize());
            }
            var crs = getDriver().runCommand(getDb(), doc);
            return crs;
        }, getDriver().getRetriesOnNetworkError(), getDriver().getSleepBetweenErrorRetries());

    }

    public List<Map<String, Object>> aggregate() throws MorphiumDriverException {
        //noinspection unchecked
        return (List<Map<String, Object>>) new NetworkCallHelper().doCall(() -> {
            Doc doc = asMap("aggregate");
            doc.put("cursor", Doc.of("batchSize", getDriver().getDefaultBatchSize()));
            setMetaData("server", getDriver().getHostSeed()[0]);
            long start = System.currentTimeMillis();

            List<Map<String, Object>> ret = new ArrayList<>();
            var crs = getDriver().runCommand(getDb(), doc);
            while (crs.hasNext()) {
                ret.addAll(crs.getBatch());
                crs.ahead(crs.getBatch().size());
            }
//                List<Map<String, Object>> lst = getDriver().readBatches(q.getMessageId(), settings.getDb(), settings.getColl(), getDriver().getMaxWriteBatchSize());
            setMetaData("duration", System.currentTimeMillis() - start);
            return Doc.of("result", ret);

        }, getDriver().getRetriesOnNetworkError(), getDriver().getSleepBetweenErrorRetries());
    }
}
