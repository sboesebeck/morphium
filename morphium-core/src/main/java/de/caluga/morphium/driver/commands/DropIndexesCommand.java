package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.wire.MongoConnection;

/**
 * Command to drop indexes from a collection.
 *
 * Usage:
 * - To drop a single index by name: setIndex("indexName")
 * - To drop all indexes (except _id): setIndex("*")
 */
public class DropIndexesCommand extends WriteMongoCommand<DropIndexesCommand> {
    private Object index; // Can be String (index name or "*") or Map (key specification)

    public DropIndexesCommand(MongoConnection d) {
        super(d);
    }

    @Override
    public String getCommandName() {
        return "dropIndexes";
    }

    /**
     * Get the index to drop.
     * @return index name, "*" for all indexes, or key specification map
     */
    public Object getIndex() {
        return index;
    }

    /**
     * Set the index to drop.
     * @param index index name, "*" to drop all indexes, or key specification map
     * @return this command for chaining
     */
    public DropIndexesCommand setIndex(Object index) {
        this.index = index;
        return this;
    }
}
