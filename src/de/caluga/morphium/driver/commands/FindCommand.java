package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.sync.DriverBase;
import de.caluga.morphium.driver.sync.NetworkCallHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FindCommand extends MongoCommand<FindCommand> {
    private DriverBase driver;
    private Map<String, Object> filter;
    private Map<String, Object> sort;
    private Map<String, Object> projection;
    private Object hint;
    private Integer skip;
    private Integer limit;
    private Integer batchSize;
    private Boolean singleBatch = false;
    private Integer maxTimeMS;
    private Map<String, Object> max;
    private Map<String, Object> min;
    private Boolean returnKey;
    private Boolean showRecordId;
    private Boolean tailable;
    private Boolean oplogReplay;
    private Boolean noCursorTimeout;
    private Boolean awaitData;
    private Boolean allowPartialResults;
    private Map<String, Object> collation;
    private Boolean allowDiskUse;
    private Map<String, Object> let;


    public DriverBase getDriver() {
        return driver;
    }

    public FindCommand setDriver(DriverBase driver) {
        this.driver = driver;
        return this;
    }

    public Map<String, Object> getFilter() {
        return filter;
    }

    public FindCommand setFilter(Map<String, Object> filter) {
        this.filter = filter;
        return this;
    }

    public Map<String, Object> getSort() {
        return sort;
    }

    public FindCommand setSort(Map<String, Object> sort) {
        this.sort = sort;
        return this;
    }

    public Map<String, Object> getProjection() {
        return projection;
    }

    public FindCommand setProjection(Map<String, Object> projection) {
        this.projection = projection;
        return this;
    }

    public Object getHint() {
        return hint;
    }

    public FindCommand setHint(Object hint) {
        this.hint = hint;
        return this;
    }

    public Integer getSkip() {
        return skip;
    }

    public FindCommand setSkip(Integer skip) {
        this.skip = skip;
        return this;
    }

    public Integer getLimit() {
        return limit;
    }

    public FindCommand setLimit(Integer limit) {
        this.limit = limit;
        return this;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public FindCommand setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public Boolean isSingleBatch() {
        return singleBatch;
    }

    public FindCommand setSingleBatch(Boolean singleBatch) {
        this.singleBatch = singleBatch;
        return this;
    }

    public Integer getMaxTimeMS() {
        return maxTimeMS;
    }

    public FindCommand setMaxTimeMS(Integer maxTimeMS) {
        this.maxTimeMS = maxTimeMS;
        return this;
    }

    public Map<String, Object> getMax() {
        return max;
    }

    public FindCommand setMax(Map<String, Object> max) {
        this.max = max;
        return this;
    }

    public Map<String, Object> getMin() {
        return min;
    }

    public FindCommand setMin(Map<String, Object> min) {
        this.min = min;
        return this;
    }

    public Boolean isReturnKey() {
        return returnKey;
    }

    public FindCommand setReturnKey(Boolean returnKey) {
        this.returnKey = returnKey;
        return this;
    }

    public Boolean isShowRecordId() {
        return showRecordId;
    }

    public FindCommand setShowRecordId(Boolean showRecordId) {
        this.showRecordId = showRecordId;
        return this;
    }

    public Boolean isTailable() {
        return tailable;
    }

    public FindCommand setTailable(Boolean tailable) {
        this.tailable = tailable;
        return this;
    }

    public Boolean isOplogReplay() {
        return oplogReplay;
    }

    public FindCommand setOplogReplay(Boolean oplogReplay) {
        this.oplogReplay = oplogReplay;
        return this;
    }

    public Boolean isNoCursorTimeout() {
        return noCursorTimeout;
    }

    public FindCommand setNoCursorTimeout(Boolean noCursorTimeout) {
        this.noCursorTimeout = noCursorTimeout;
        return this;
    }

    public Boolean isAwaitData() {
        return awaitData;
    }

    public FindCommand setAwaitData(Boolean awaitData) {
        this.awaitData = awaitData;
        return this;
    }

    public Boolean isAllowPartialResults() {
        return allowPartialResults;
    }

    public FindCommand setAllowPartialResults(Boolean allowPartialResults) {
        this.allowPartialResults = allowPartialResults;
        return this;
    }

    public Map<String, Object> getCollation() {
        return collation;
    }

    public FindCommand setCollation(Map<String, Object> collation) {
        this.collation = collation;
        return this;
    }

    public Boolean isAllowDiskUse() {
        return allowDiskUse;
    }

    public FindCommand setAllowDiskUse(Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    public Map<String, Object> getLet() {
        return let;
    }

    public FindCommand setLet(Map<String, Object> let) {
        this.let = let;
        return this;
    }

    public Boolean getSingleBatch() {
        return singleBatch;
    }

    public Boolean getReturnKey() {
        return returnKey;
    }

    public Boolean getShowRecordId() {
        return showRecordId;
    }

    public Boolean getTailable() {
        return tailable;
    }

    public Boolean getOplogReplay() {
        return oplogReplay;
    }

    public Boolean getNoCursorTimeout() {
        return noCursorTimeout;
    }

    public Boolean getAwaitData() {
        return awaitData;
    }

    public Boolean getAllowPartialResults() {
        return allowPartialResults;
    }

    public Boolean getAllowDiskUse() {
        return allowDiskUse;
    }

    //    @Override
    public List<Map<String, Object>> executeGetResult() throws MorphiumDriverException {
        if (driver == null) throw new IllegalArgumentException("you need to set the driver!");
        //noinspection unchecked
        return (List<Map<String, Object>>) new NetworkCallHelper().doCall(() -> {

            List<Map<String, Object>> ret = new ArrayList<>();

            setMetaData(Doc.of("server", driver.getHostSeed()[0]));
            long start = System.currentTimeMillis();
            MorphiumCursor crs = driver.runCommand(getDb(), asMap("find"));
            while (crs.hasNext()) {
                ret.addAll(crs.getBatch());
                crs.ahead(crs.getBatch().size());
            }
            long dur = System.currentTimeMillis() - start;
            getMetaData().put("duration", dur);
            return Doc.of("values", ret);
        }, driver.getRetriesOnNetworkError(), driver.getSleepBetweenErrorRetries()).get("values");
    }

    //    @Override
    public MorphiumCursor execute() throws MorphiumDriverException {
        if (driver == null) throw new IllegalArgumentException("you need to set the driver!");
        //noinspection unchecked
        return (MorphiumCursor) new NetworkCallHelper().doCall(() -> {
            setMetaData(Doc.of("server", driver.getHostSeed()[0]));
            long start = System.currentTimeMillis();
            MorphiumCursor crs = driver.runCommand(getDb(), asMap("find"));
            long dur = System.currentTimeMillis() - start;
            getMetaData().put("duration", dur);
            return Doc.of("cursor", crs);
        }, driver.getRetriesOnNetworkError(), driver.getSleepBetweenErrorRetries()).get("cursor");
    }

    //    @Override_
    public int executeGetID() throws MorphiumDriverException {
        if (driver == null) throw new IllegalArgumentException("you need to set the driver!");
        //noinspection unchecked
        return (Integer) new NetworkCallHelper().doCall(() -> {
            setMetaData(Doc.of("server", driver.getHostSeed()[0]));
            long start = System.currentTimeMillis();
            int id = driver.sendCommand(getDb(), asMap("find"));
            long dur = System.currentTimeMillis() - start;
            getMetaData().put("duration", dur);
            return Doc.of("id", id);
        }, driver.getRetriesOnNetworkError(), driver.getSleepBetweenErrorRetries()).get("id");
    }
}
