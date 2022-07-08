package de.caluga.morphium.driver.mongodb;

import com.mongodb.client.ClientSession;
import de.caluga.morphium.driver.MorphiumTransactionContext;
import de.caluga.morphium.driver.wire.MorphiumTransactionContextImpl;

import java.util.UUID;

/**
 * User: Stephan Bösebeck
 * Date: 03.07.18
 * Time: 22:19
 * <p>
 * TODO: Add documentation here
 */
public class MongoTransactionContext implements MorphiumTransactionContext {
    ClientSession session;

    public ClientSession getSession() {
        return session;
    }

    public void setSession(ClientSession session) {
        this.session = session;
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
