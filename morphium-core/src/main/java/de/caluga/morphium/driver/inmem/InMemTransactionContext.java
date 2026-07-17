package de.caluga.morphium.driver.inmem;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import de.caluga.morphium.driver.MorphiumTransactionContext;
import de.caluga.morphium.driver.wire.MorphiumTransactionContextImpl;

/**
 * User: Stephan Bösebeck
 * Date: 03.07.18
 * Time: 23:34
 * <p>
 * TODO: Add documentation here
 */
public class InMemTransactionContext implements MorphiumTransactionContext {
    private Map database;

    /**
     * Keys ({@code db + "/" + collection}) of the collections this transaction actually wrote to.
     * On commit only these collections are merged back into the live database, so concurrent
     * non-transactional writes to collections the transaction never touched are preserved.
     */
    private final Set<String> touchedCollections = ConcurrentHashMap.newKeySet();

    public Map getDatabase() {
        return database;
    }

    public void setDatabase(Map database) {
        this.database = database;
    }

    public Set<String> getTouchedCollections() {
        return touchedCollections;
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
