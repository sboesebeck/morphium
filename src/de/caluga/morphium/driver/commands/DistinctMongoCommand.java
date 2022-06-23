package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;

public class DistinctMongoCommand extends MongoCommand<DistinctMongoCommand> {
    private String key;
    private Doc query;
    private Doc readConcern;
    private Doc collation;

    public String getKey() {
        return key;
    }

    public DistinctMongoCommand setKey(String key) {
        this.key = key;
        return this;
    }

    public Doc getQuery() {
        return query;
    }

    public DistinctMongoCommand setQuery(Doc query) {
        this.query = query;
        return this;
    }

    public Doc getReadConcern() {
        return readConcern;
    }

    public DistinctMongoCommand setReadConcern(Doc readConcern) {
        this.readConcern = readConcern;
        return this;
    }

    public Doc getCollation() {
        return collation;
    }

    public DistinctMongoCommand setCollation(Doc collation) {
        this.collation = collation;
        return this;
    }
}
