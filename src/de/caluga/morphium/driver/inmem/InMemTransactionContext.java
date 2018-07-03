package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.driver.MorphiumTransactionContext;

import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 03.07.18
 * Time: 23:34
 * <p>
 * TODO: Add documentation here
 */
public class InMemTransactionContext extends MorphiumTransactionContext {
    private Map database;

    public Map getDatabase() {
        return database;
    }

    public void setDatabase(Map database) {
        this.database = database;
    }
}
