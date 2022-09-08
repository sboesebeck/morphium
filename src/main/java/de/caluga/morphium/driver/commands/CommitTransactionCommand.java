package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.Map;
import java.util.UUID;

public class CommitTransactionCommand extends AdminMongoCommand<CommitTransactionCommand>{
    private boolean autocommit=false;
    private UUID lsid;
    private long txnNumber=0;

    public CommitTransactionCommand(MongoConnection d) {
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
        return "commitTransaction";
    }

    public boolean isAutocommit() {
        return autocommit;
    }

    public CommitTransactionCommand setAutocommit(boolean autocommit) {
        this.autocommit = autocommit;
        return this;
    }

    public UUID getLsid() {
        return lsid;
    }

    public CommitTransactionCommand setLsid(UUID lsid) {
        this.lsid = lsid;
        return this;
    }

    public long getTxnNumber() {
        return txnNumber;
    }

    public CommitTransactionCommand setTxnNumber(long txnNumber) {
        this.txnNumber = txnNumber;
        return this;
    }
//Doc.of("commitTransaction", 1, "txnNumber", ctx.getTxnNumber(), "autocommit", false, "lsid", Doc.of("id", ctx.getLsid()), "$db", "admin")
}
