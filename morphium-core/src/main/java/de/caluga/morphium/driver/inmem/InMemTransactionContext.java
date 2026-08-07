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

    /**
     * Keys ({@code db + "/" + collection}) of every collection whose persistent
     * {@link CollectionIndexStore} was actually BUILT (not merely reused) while this
     * transaction was active - a strict superset of {@link #touchedCollections}. A read-only
     * indexed query (see {@code InMemoryDriver#getDataFromIndex}) can lazily build that store
     * from {@code getCollection()}, which resolves against this transaction's private snapshot
     * while one is active - i.e. against structurally-cloned document instances, not the live
     * ones - without ever writing to the collection and therefore without ever calling
     * {@code markCollectionTouched}. A plain reuse of an already-built store can never
     * introduce clones (see {@code InMemoryDriver#getIndexStore}), so only builds are recorded
     * here. On BOTH commit and abort, every collection recorded here (not just the written
     * ones) must have its store invalidated, or a store lazily built from this transaction's
     * clones could keep referencing them after the transaction ends.
     */
    private final Set<String> indexStoreAccessedCollections = ConcurrentHashMap.newKeySet();

    public Map getDatabase() {
        return database;
    }

    public void setDatabase(Map database) {
        this.database = database;
    }

    public Set<String> getTouchedCollections() {
        return touchedCollections;
    }

    public Set<String> getIndexStoreAccessedCollections() {
        return indexStoreAccessedCollections;
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
