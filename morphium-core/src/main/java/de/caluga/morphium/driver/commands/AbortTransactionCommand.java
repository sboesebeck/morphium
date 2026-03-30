package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.Map;
import java.util.UUID;

/**
 * AbortTransactionCommand
 */
public class AbortTransactionCommand extends AdminMongoCommand<AbortTransactionCommand>{
    private boolean autocommit=false;
    private UUID lsid;
    private long txnNumber=0;

    /**
     * Creates a new AbortTransactionCommand with the given connection.
     * @param d the connection
     */
    public AbortTransactionCommand(MongoConnection d) {
        super(d);
    }

    @Override
    public Map<String, Object> asMap() {
        var m=super.asMap();
        m.put("lsid", Doc.of("id",lsid));
        return m;
    }

    /**
     * Returns the MongoDB command name for this command.
     * @return the command name
     */
    @Override
    public String getCommandName() {
        return "abortTransaction";
    }

    /**
     * Returns the autocommit setting for this transaction.
     * @return the autocommit flag
     */
    public boolean isAutocommit() {
        return autocommit;
    }

    /**
     * Sets the autocommit flag for this transaction.
     * @param autocommit the autocommit flag to set
     * @return the command itself
     */
    public AbortTransactionCommand setAutocommit(boolean autocommit) {
        this.autocommit = autocommit;
        return this;
    }

    /**
     * Returns the logical session ID for this transaction.
     * @return the lsid
     */
    public UUID getLsid() {
        return lsid;
    }

    /**
     * Sets the logical session ID for this transaction.
     * @param lsid the lsid to set
     * @return the command itself
     */
    public AbortTransactionCommand setLsid(UUID lsid) {
        this.lsid = lsid;
        return this;
    }

    /**
     * Returns the transaction number.
     * @return the txnNumber
     */
    public long getTxnNumber() {
        return txnNumber;
    }

    /**
     * Sets the transaction number.
     * @param txnNumber the txnNumber to set
     * @return the command itself
     */
    public AbortTransactionCommand setTxnNumber(long txnNumber) {
        this.txnNumber = txnNumber;
        return this;
    }
}
