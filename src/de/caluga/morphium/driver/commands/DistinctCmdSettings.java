package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;

import java.util.List;

public class DistinctCmdSettings extends CmdSettings<DistinctCmdSettings> {
    private String key;
    private Doc query;
    private Doc readConcern;
    private Doc collation;

    public String getKey() {
        return key;
    }

    public DistinctCmdSettings setKey(String key) {
        this.key = key;
        return this;
    }

    public Doc getQuery() {
        return query;
    }

    public DistinctCmdSettings setQuery(Doc query) {
        this.query = query;
        return this;
    }

    public Doc getReadConcern() {
        return readConcern;
    }

    public DistinctCmdSettings setReadConcern(Doc readConcern) {
        this.readConcern = readConcern;
        return this;
    }

    public Doc getCollation() {
        return collation;
    }

    public DistinctCmdSettings setCollation(Doc collation) {
        this.collation = collation;
        return this;
    }
}
