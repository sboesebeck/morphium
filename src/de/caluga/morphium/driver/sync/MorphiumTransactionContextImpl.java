package de.caluga.morphium.driver.sync;

import de.caluga.morphium.driver.MorphiumTransactionContext;

public class MorphiumTransactionContextImpl extends MorphiumTransactionContext {
    private String lsid;
    private Long txnNumber;
    private Boolean autoCommit;

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
