package de.caluga.morphium.driver.sync;

import de.caluga.morphium.driver.MorphiumTransactionContext;

public class MorphiumTransactionContextImpl extends MorphiumTransactionContext {
    private String lsid;
    private Long txnNumber;
    private Boolean autoCommit;
    private boolean started;

    public Boolean getAutoCommit() {
        return autoCommit;
    }

    public MorphiumTransactionContextImpl setAutoCommit(Boolean autoCommit) {
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

    public String getLsid() {
        return lsid;
    }

    public MorphiumTransactionContextImpl setLsid(String lsid) {
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
