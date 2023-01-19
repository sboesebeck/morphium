package de.caluga.morphium.driver.commands;

import java.util.Map;

import de.caluga.morphium.driver.wire.MongoConnection;

public class FindCommand extends ReadMongoCommand<FindCommand> {
    private Map<String, Object> filter;
    private Map<String, Object> sort;
    private Map<String, Object> projection;
    private Object hint;
    private Integer skip;
    private Integer limit;
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
    private Integer batchSize;

    public Integer getBatchSize() {
        return batchSize;
    }

    public FindCommand setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public FindCommand(MongoConnection d) {
        super(d);
    }

    @Override
    public String getCommandName() {
        return "find";
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

}
