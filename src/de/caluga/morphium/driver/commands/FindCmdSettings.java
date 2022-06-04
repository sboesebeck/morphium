package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;

public class FindCmdSettings extends CmdSettings<FindCmdSettings> {
    private Doc filter;
    private Doc sort;
    private Doc projection;
    private Object hint;
    private Integer skip;
    private Integer limit;
    private Integer batchSize;
    private Boolean singleBatch = false;
    private Integer maxTimeMS;
    private Doc max;
    private Doc min;
    private Boolean returnKey;
    private Boolean showRecordId;
    private Boolean tailable;
    private Boolean oplogReplay;
    private Boolean noCursorTimeout;
    private Boolean awaitData;
    private Boolean allowPartialResults;
    private Doc collation;
    private Boolean allowDiskUse;
    private Doc let;

    public Doc getFilter() {
        return filter;
    }

    public FindCmdSettings setFilter(Doc filter) {
        this.filter = filter;
        return this;
    }

    public Doc getSort() {
        return sort;
    }

    public FindCmdSettings setSort(Doc sort) {
        this.sort = sort;
        return this;
    }

    public Doc getProjection() {
        return projection;
    }

    public FindCmdSettings setProjection(Doc projection) {
        this.projection = projection;
        return this;
    }

    public Object getHint() {
        return hint;
    }

    public FindCmdSettings setHint(Object hint) {
        this.hint = hint;
        return this;
    }

    public Integer getSkip() {
        return skip;
    }

    public FindCmdSettings setSkip(Integer skip) {
        this.skip = skip;
        return this;
    }

    public Integer getLimit() {
        return limit;
    }

    public FindCmdSettings setLimit(Integer limit) {
        this.limit = limit;
        return this;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public FindCmdSettings setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public Boolean isSingleBatch() {
        return singleBatch;
    }

    public FindCmdSettings setSingleBatch(Boolean singleBatch) {
        this.singleBatch = singleBatch;
        return this;
    }

    public Integer getMaxTimeMS() {
        return maxTimeMS;
    }

    public FindCmdSettings setMaxTimeMS(Integer maxTimeMS) {
        this.maxTimeMS = maxTimeMS;
        return this;
    }

    public Doc getMax() {
        return max;
    }

    public FindCmdSettings setMax(Doc max) {
        this.max = max;
        return this;
    }

    public Doc getMin() {
        return min;
    }

    public FindCmdSettings setMin(Doc min) {
        this.min = min;
        return this;
    }

    public Boolean isReturnKey() {
        return returnKey;
    }

    public FindCmdSettings setReturnKey(Boolean returnKey) {
        this.returnKey = returnKey;
        return this;
    }

    public Boolean isShowRecordId() {
        return showRecordId;
    }

    public FindCmdSettings setShowRecordId(Boolean showRecordId) {
        this.showRecordId = showRecordId;
        return this;
    }

    public Boolean isTailable() {
        return tailable;
    }

    public FindCmdSettings setTailable(Boolean tailable) {
        this.tailable = tailable;
        return this;
    }

    public Boolean isOplogReplay() {
        return oplogReplay;
    }

    public FindCmdSettings setOplogReplay(Boolean oplogReplay) {
        this.oplogReplay = oplogReplay;
        return this;
    }

    public Boolean isNoCursorTimeout() {
        return noCursorTimeout;
    }

    public FindCmdSettings setNoCursorTimeout(Boolean noCursorTimeout) {
        this.noCursorTimeout = noCursorTimeout;
        return this;
    }

    public Boolean isAwaitData() {
        return awaitData;
    }

    public FindCmdSettings setAwaitData(Boolean awaitData) {
        this.awaitData = awaitData;
        return this;
    }

    public Boolean isAllowPartialResults() {
        return allowPartialResults;
    }

    public FindCmdSettings setAllowPartialResults(Boolean allowPartialResults) {
        this.allowPartialResults = allowPartialResults;
        return this;
    }

    public Doc getCollation() {
        return collation;
    }

    public FindCmdSettings setCollation(Doc collation) {
        this.collation = collation;
        return this;
    }

    public Boolean isAllowDiskUse() {
        return allowDiskUse;
    }

    public FindCmdSettings setAllowDiskUse(Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    public Doc getLet() {
        return let;
    }

    public FindCmdSettings setLet(Doc let) {
        this.let = let;
        return this;
    }
}
