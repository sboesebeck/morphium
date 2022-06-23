package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;

public class MapReduceSettings extends MongoCommand<MapReduceSettings> {
    private String map;
    private String reduce;
    private String finalize;
    private String outColl;
    private Doc outConfig;

    private Doc query;
    private Doc sort;
    private Integer limit;
    private Doc scope;
    private Boolean jsMode;
    private Boolean verbose;

    private Boolean bypassDocumentValidation;
    private Doc collation;
    private Doc writeConcern;
    private String comment;


    public String getMap() {
        return map;
    }

    public MapReduceSettings setMap(String map) {
        this.map = map;
        return this;
    }

    public String getReduce() {
        return reduce;
    }

    public MapReduceSettings setReduce(String reduce) {
        this.reduce = reduce;
        return this;
    }

    public String getFinalize() {
        return finalize;
    }

    public MapReduceSettings setFinalize(String finalize) {
        this.finalize = finalize;
        return this;
    }

    public String getOutColl() {
        return outColl;
    }

    public MapReduceSettings setOutColl(String outColl) {
        this.outColl = outColl;
        return this;
    }

    public Doc getOutConfig() {
        return outConfig;
    }

    public MapReduceSettings setOutConfig(Doc outConfig) {
        this.outConfig = outConfig;
        return this;
    }

    public Doc getQuery() {
        return query;
    }

    public MapReduceSettings setQuery(Doc query) {
        this.query = query;
        return this;
    }

    public Doc getSort() {
        return sort;
    }

    public MapReduceSettings setSort(Doc sort) {
        this.sort = sort;
        return this;
    }

    public Integer getLimit() {
        return limit;
    }

    public MapReduceSettings setLimit(Integer limit) {
        this.limit = limit;
        return this;
    }

    public Doc getScope() {
        return scope;
    }

    public MapReduceSettings setScope(Doc scope) {
        this.scope = scope;
        return this;
    }

    public Boolean getJsMode() {
        return jsMode;
    }

    public MapReduceSettings setJsMode(Boolean jsMode) {
        this.jsMode = jsMode;
        return this;
    }

    public Boolean getVerbose() {
        return verbose;
    }

    public MapReduceSettings setVerbose(Boolean verbose) {
        this.verbose = verbose;
        return this;
    }

    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    public MapReduceSettings setBypassDocumentValidation(Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    public Doc getCollation() {
        return collation;
    }

    public MapReduceSettings setCollation(Doc collation) {
        this.collation = collation;
        return this;
    }

    public Doc getWriteConcern() {
        return writeConcern;
    }

    public MapReduceSettings setWriteConcern(Doc writeConcern) {
        this.writeConcern = writeConcern;
        return this;
    }

    @Override
    public String getComment() {
        return comment;
    }

    @Override
    public MapReduceSettings setComment(String comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public Doc asMap(String commandName) {
        Doc m = super.asMap(commandName);
        if (m.containsKey("outColl") && m.containsKey("outConfig")) {
            throw new IllegalArgumentException("Cannot specify out coll and out config!");
        }
        String k = "outColl";
        if (m.containsKey("outConfig")) {
            k = "outConfig";
        }
        m.put("out", m.remove(k));
        return m;
    }
}
