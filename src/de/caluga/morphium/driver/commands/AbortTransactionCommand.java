package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.Map;
import java.util.UUID;

public class AbortTransactionCommand extends AdminMongoCommand<AbortTransactionCommand>{
    private boolean autocommit=false;
    private UUID lsid;
    private long txnNumber=0;

    public AbortTransactionCommand(MongoConnection d) {
        super(d);
    }

    @Override
    public Map<String, Object> asMap() {
        var m=super.asMap();
        m.put("lsid", Doc.of("id",lsid));
        return m;
    }

    @Override
    public String getCommandName() {
        return "abortTransaction";
    }

    public boolean isAutocommit() {
        return autocommit;
    }

    public AbortTransactionCommand setAutocommit(boolean autocommit) {
        this.autocommit = autocommit;
        return this;
    }

    public UUID getLsid() {
        return lsid;
    }

    public AbortTransactionCommand setLsid(UUID lsid) {
        this.lsid = lsid;
        return this;
    }

    public long getTxnNumber() {
        return txnNumber;
    }

    public AbortTransactionCommand setTxnNumber(long txnNumber) {
        this.txnNumber = txnNumber;
        return this;
    }
}
