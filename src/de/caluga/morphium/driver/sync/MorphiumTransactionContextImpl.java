package de.caluga.morphium.driver.sync;

import de.caluga.morphium.driver.MorphiumTransactionContext;

import java.util.UUID;

public class MorphiumTransactionContextImpl extends MorphiumTransactionContext {
    private UUID lsid;
    private Long txnNumber;
    private boolean autoCommit = false;
    private boolean started;

    public boolean getAutoCommit() {
        return autoCommit;
    }

    public MorphiumTransactionContextImpl setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
        return this;
    }

    public boolean isStarted() {
        return started;
    }

    public MorphiumTransactionContextImpl setStarted(boolean started) {
        this.started = started;
        return this;
    }

    public UUID getLsid() {
        return lsid;
    }

    public MorphiumTransactionContextImpl setLsid(UUID lsid) {
        this.lsid = lsid;
        return this;
    }

    public Long getTxnNumber() {
        return txnNumber;
    }

    public MorphiumTransactionContextImpl setTxnNumber(Long txnNumber) {
        this.txnNumber = txnNumber;
        return this;
    }
}
