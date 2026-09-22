package de.caluga.morphium.driver.commands;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wire.NetworkCallHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class ReadMongoCommand<T extends MongoCommand> extends MongoCommand<T> implements MultiResultCommand, Iterable<Map<String, Object>> {

    private static final Logger log = LoggerFactory.getLogger(ReadMongoCommand.class);

    public ReadMongoCommand(MongoConnection d) {
        super(d);
    }



    @Override
    public Iterator<Map<String, Object>> iterator() {
        return executeIterable(getConnection().getDriver().getDefaultBatchSize());
    }

    public List<Map<String, Object>> execute() throws MorphiumDriverException {
        MongoConnection connection = getConnection();
        if (connection == null) throw new IllegalArgumentException("you need to set the connection!");
        MorphiumDriver driver = connection.getDriver();
        //noinspection unchecked
        return new NetworkCallHelper<List<Map<String, Object>>>().doCall(() -> {
            // If the connection was closed (e.g. due to a transient transaction error on
            // a previous attempt), acquire a fresh connection from the pool.
            MongoConnection activeCon = getConnection();
            if (activeCon == null || !activeCon.isConnected()) {
                // re-borrow for what this read asked for, not for the driver default - behind a
                // mongos the retry would otherwise be routed differently than the first attempt
                ReadPreference rp = getReadPreference();
                if (rp == null && activeCon != null) {
                    rp = activeCon.getEffectiveReadPreference();
                }
                if (rp == null) {
                    rp = driver.getDefaultReadPreference();
                }
                activeCon = driver.getReadConnection(rp);
                setConnection(activeCon);
            }
            List<Map<String, Object>> ret = new ArrayList<>();
            long start = System.currentTimeMillis();
            MorphiumCursor crs = openCursor(activeCon, driver.getDefaultBatchSize());
            while (crs.hasNext()) {
                List<Map<String, Object>> batch = crs.getBatch();
                if (batch.size() == 1 && batch.get(0).containsKey("ok") && batch.get(0).get("ok").equals((double) 0)) {
                    throw new MorphiumDriverException("Error: " + batch.get(0).get("code") + ": " + batch.get(0).get("errmsg"));
                }
                ret.addAll(batch);
                crs.ahead(batch.size());
            }
            setConnection(null); //was released!
            long dur = System.currentTimeMillis() - start;
            setMetaData("duration", dur);
            return ret;
        }, driver.getRetriesOnNetworkError(), driver.getSleepBetweenErrorRetries());
    }

    @Override
    public MorphiumCursor executeIterable(int batchsize) throws MorphiumDriverException {
        MongoConnection connection = getConnection();
        if (connection == null) throw new IllegalArgumentException("you need to set the connection!");
        long start = System.currentTimeMillis();
        MorphiumCursor crs = openCursor(connection, batchsize);
        long dur = System.currentTimeMillis() - start;
        setMetaData("duration", dur);
        return crs;
    }

    /**
     * Sends the command on {@code con} and reads its first reply.
     *
     * <p>A not-primary/stepdown answer ({@link StepDownErrors}) on a read that targets the
     * primary is retried on the re-resolved primary (#393): the driver is asked to refresh its
     * primary from the very node that rejected the read
     * ({@link MorphiumDriver#refreshPrimaryAfterStepDown}), the rejected connection is given
     * back, and after {@code sleepBetweenErrorRetries} (at least 50 ms) the read goes out again
     * on {@link MorphiumDriver#getPrimaryConnection}, at most {@code retriesOnNetworkError}
     * times - then the last error is thrown. A rejected read has not touched the server, so
     * re-sending it is safe; this is what the write path has done for a rejected write all
     * along. A read that asked for a secondary or the nearest node keeps the error: there it is
     * the caller's read preference meeting a node that does not serve it, not a failover. Inside
     * a transaction the error is not retried either - the transaction is bound to the node that
     * just stepped down, and the transaction layer restarts it as a whole.
     *
     * <p>Only the first reply is covered. A getMore of a partially consumed cursor cannot be
     * re-sent to another node - the cursor lives on the node that stepped down, and MongoDB
     * kills it there - and re-running the query from the start would hand the caller documents
     * it has already seen. Such an error still reaches the caller.
     */
    private MorphiumCursor openCursor(MongoConnection con, int batchSize) throws MorphiumDriverException {
        MorphiumDriver driver = con.getDriver();
        int maxRetries = Math.max(0, driver.getRetriesOnNetworkError());
        int retry = 0;

        while (true) {
            setMetaData("server", con.getConnectedTo());

            try {
                int msg = con.sendCommand(this);
                return con.getAnswerFor(msg, batchSize);
            } catch (MorphiumDriverException e) {
                if (retry >= maxRetries || !StepDownErrors.isStepDownError(e) || !targetsPrimary(con)
                    || driver.isTransactionInProgress()) {
                    throw e;
                }

                retry++;
                log.warn("Primary step-down during read ({}) - re-resolving primary, retry {}/{}", e.getMessage(), retry, maxRetries);

                try {
                    driver.refreshPrimaryAfterStepDown(con);
                } catch (Exception ignore) {
                    // the contract says it does not throw; a driver that does must not cost the retry
                }

                try {
                    driver.releaseConnection(con);
                } catch (Exception ignore) {
                }

                setConnection(null);
                con = null;

                while (con == null) {
                    try {
                        Thread.sleep(Math.max(50, driver.getSleepBetweenErrorRetries()));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw e;
                    }

                    try {
                        con = driver.getPrimaryConnection(null);
                    } catch (MorphiumDriverException noPrimary) {
                        // the driver waited its server selection timeout for a primary and found
                        // none: the election is still running. Stays within the same retry budget.
                        if (retry >= maxRetries) {
                            throw noPrimary;
                        }

                        retry++;
                        log.warn("No primary after step-down ({}) - retry {}/{}", noPrimary.getMessage(), retry, maxRetries);
                    }
                }

                setConnection(con);
            }
        }
    }

    /**
     * @return true when this read goes to the primary: by the command's own read preference,
     *         else by the one its connection was handed out for; none at all is sent as
     *         primaryPreferred (see {@code MongoCommand#readPreferenceAsDoc}) and counts as such
     */
    private boolean targetsPrimary(MongoConnection con) {
        ReadPreference rp = getReadPreference();

        if (rp == null) {
            rp = con.getEffectiveReadPreference();
        }

        if (rp == null || rp.getType() == null) {
            return true;
        }

        return rp.getType() == ReadPreferenceType.PRIMARY || rp.getType() == ReadPreferenceType.PRIMARY_PREFERRED;
    }

}
