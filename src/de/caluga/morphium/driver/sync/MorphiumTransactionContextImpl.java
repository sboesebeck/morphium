package de.caluga.morphium.driver.sync;

import de.caluga.morphium.driver.MorphiumTransactionContext;

import java.util.UUID;

public class MorphiumTransactionContextImpl implements MorphiumTransactionContext {
    private UUID lsid;
    private boolean autoCommit = false;
    private boolean started;

    protected Long txnNumber;

    @Override
    public Long getTxnNumber() {
        return txnNumber;
    }

    @Override
    public MorphiumTransactionContextImpl setTxnNumber(Long txnNumber) {
        this.txnNumber = txnNumber;
        return this;
    }

    @Override
    public boolean getAutoCommit() {
        return autoCommit;
    }

    @Override
    public MorphiumTransactionContextImpl setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
        return this;
    }

    @Override
    public boolean isStarted() {
        return started;
    }

    @Override
    public MorphiumTransactionContextImpl setStarted(boolean started) {
        this.started = started;
        return this;
    }

    @Override
    public UUID getLsid() {
        return lsid;
    }

    @Override
    public MorphiumTransactionContextImpl setLsid(UUID lsid) {
        this.lsid = lsid;
        return this;
    }
}
