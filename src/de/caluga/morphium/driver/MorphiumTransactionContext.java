package de.caluga.morphium.driver;

import de.caluga.morphium.driver.wire.MorphiumTransactionContextImpl;

import java.util.UUID;

/**
 * User: Stephan Bösebeck
 * Date: 03.07.18
 * Time: 22:03
 * <p>
 * TODO: Add documentation here
 */
public interface MorphiumTransactionContext {

    Long getTxnNumber();

    boolean getAutoCommit();

    MorphiumTransactionContextImpl setAutoCommit(boolean autoCommit);

    boolean isStarted();

    MorphiumTransactionContextImpl setStarted(boolean started);

    UUID getLsid();

    MorphiumTransactionContextImpl setLsid(UUID lsid);

    MorphiumTransactionContextImpl setTxnNumber(Long txnNumber);
}
