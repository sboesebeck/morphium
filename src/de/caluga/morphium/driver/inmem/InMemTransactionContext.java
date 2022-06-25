package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.driver.MorphiumTransactionContext;
import de.caluga.morphium.driver.sync.MorphiumTransactionContextImpl;

import java.util.Map;
import java.util.UUID;

/**
 * User: Stephan Bösebeck
 * Date: 03.07.18
 * Time: 23:34
 * <p>
 * TODO: Add documentation here
 */
public class InMemTransactionContext implements MorphiumTransactionContext {
    private Map database;

    public Map getDatabase() {
        return database;
    }

    public void setDatabase(Map database) {
        this.database = database;
    }

    @Override
    public Long getTxnNumber() {
        return null;
    }

    @Override
    public boolean getAutoCommit() {
        return false;
    }

    @Override
    public MorphiumTransactionContextImpl setAutoCommit(boolean autoCommit) {
        return null;
    }

    @Override
    public boolean isStarted() {
        return false;
    }

    @Override
    public MorphiumTransactionContextImpl setStarted(boolean started) {
        return null;
    }

    @Override
    public UUID getLsid() {
        return null;
    }

    @Override
    public MorphiumTransactionContextImpl setLsid(UUID lsid) {
        return null;
    }

    @Override
    public MorphiumTransactionContextImpl setTxnNumber(Long txnNumber) {
        return null;
    }
}
