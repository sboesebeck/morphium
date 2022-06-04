package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;

public class MapReduceSettings extends CmdSettings<MapReduceSettings> {
    private String db;
    private String collection;
    private String map;
    private String reduce;
    private String finalize;
    private String out;

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

    @Override
    public String getDb() {
        return db;
    }

    @Override
    public MapReduceSettings setDb(String db) {
        this.db = db;
        return this;
    }

    public String getCollection() {
        return collection;
    }

    public MapReduceSettings setCollection(String collection) {
        this.collection = collection;
        return this;
    }

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

    public String getOut() {
        return out;
    }

    public MapReduceSettings setOut(String out) {
        this.out = out;
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
}
