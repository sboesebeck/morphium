package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;

public class CountCmdSettings extends CmdSettings<CountCmdSettings> {
    private Doc query;
    private Integer limit;
    private Integer skip;
    private Object hint;
    private Doc readConcern;
    private Doc collation;



    public Doc getQuery() {
        return query;
    }

    public CountCmdSettings setQuery(Doc query) {
        this.query = query;
        return this;
    }

    public Integer getLimit() {
        return limit;
    }

    public CountCmdSettings setLimit(Integer limit) {
        this.limit = limit;
        return this;
    }

    public Integer getSkip() {
        return skip;
    }

    public CountCmdSettings setSkip(Integer skip) {
        this.skip = skip;
        return this;
    }

    public Object getHint() {
        return hint;
    }

    public CountCmdSettings setHint(Object hint) {
        this.hint = hint;
        return this;
    }

    public Doc getReadConcern() {
        return readConcern;
    }

    public CountCmdSettings setReadConcern(Doc readConcern) {
        this.readConcern = readConcern;
        return this;
    }

    public Doc getCollation() {
        return collation;
    }

    public CountCmdSettings setCollation(Doc collation) {
        this.collation = collation;
        return this;
    }
}