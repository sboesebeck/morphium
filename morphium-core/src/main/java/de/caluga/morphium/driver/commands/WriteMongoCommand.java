package de.caluga.morphium.driver.commands;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumDriverNetworkException;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class WriteMongoCommand<T extends MongoCommand> extends MongoCommand<T> {
    private Map<String, Object> writeConcern;
    private Boolean bypassDocumentValidation;

    public WriteMongoCommand(MongoConnection d) {
        super(d);
    }

    public Map<String, Object> getWriteConcern() {
        return writeConcern;
    }

    @SuppressWarnings("unchecked")
    public T setWriteConcern(Map<String, Object> writeConcern) {
        this.writeConcern = writeConcern;
        return (T) this;
    }

    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    public WriteMongoCommand<T> setBypassDocumentValidation(Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    public Map<String, Object> execute() throws MorphiumDriverException {
        if (!getConnection().isConnected()) {
            throw new MorphiumDriverException("Not connected");
        }

        Logger log = LoggerFactory.getLogger(WriteMongoCommand.class);
        int attempts = 0;
        MorphiumDriver drv = getConnection().getDriver();
        int maxAttempts = Math.max(0, drv.getRetriesOnNetworkError()) + 5;

        while (true) {
            MongoConnection con = getConnection();
            // If the connection was closed (e.g. corrupt stream on previous attempt), get a fresh one
            if (!con.isConnected()) {
                con = drv.getPrimaryConnection(null);
                setConnection(con);
            }
            //noinspection unchecked
            setMetaData("server", con.getConnectedTo());
            long start = System.currentTimeMillis();
            int msg = con.sendCommand(this);

            try {
                var crs = con.readSingleAnswer(msg);
                if (crs == null) {
                    // No reply: either the connection was closed while waiting (dead host
                    // evicted during failover) or the reply did not arrive within the
                    // timeout. Either way the connection is in an undefined state (a late
                    // reply would corrupt the next command) - close it and retry on a
                    // fresh one, like any other network error.
                    try {
                        con.close();
                    } catch (Exception ignore) {
                    }
                    throw new MorphiumDriverNetworkException("No reply for write request (connection closed or timeout)");
                }
                long dur = System.currentTimeMillis() - start;
                setMetaData("duration", dur);

                // "database is in the process of being dropped" is a transient state on real MongoDB
                // (especially in parallel test runs that drop DBs a lot). Retry these writes with backoff.
                if (isDbBeingDroppedWriteError(crs) && attempts++ < maxAttempts) {
                    try {
                        Thread.sleep(Math.max(50, drv.getSleepBetweenErrorRetries()));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return crs;
                    }
                    continue;
                }

                return crs;
            } catch (MorphiumDriverException e) {
                String errMsg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
                if (isStepDownError(e, errMsg)) {
                    // Primary stepped down / is shutting down: the write was rejected, so
                    // retrying on the newly elected primary is safe.
                    if (attempts++ >= maxAttempts) {
                        throw e;
                    }
                    log.warn("Primary step-down ({}) - waiting for failover, retry {}/{}", e.getMessage(), attempts, maxAttempts);
                    try {
                        drv.releaseConnection(getConnection());
                    } catch (Exception ignore) {
                    }
                    try {
                        Thread.sleep(Math.max(50, drv.getHeartbeatFrequency()));
                    } catch (InterruptedException e1) {
                        Thread.currentThread().interrupt();
                        throw e;
                    }
                    setConnection(drv.getPrimaryConnection(null));
                    continue;
                } else if (e instanceof MorphiumDriverNetworkException
                    && e.getMongoCode() instanceof Number mc && mc.intValue() == 251
                    && attempts++ < maxAttempts) {
                    // Error 251 (NoSuchTransaction): connection had a poisoned server-side session.
                    // The connection was already closed by checkForError/readSingleAnswer.
                    // Get a fresh connection and retry.
                    log.warn("Transient transaction error (code 251) — retrying with new connection");
                    try {
                        drv.releaseConnection(getConnection());
                    } catch (Exception ignore) {
                    }
                    setConnection(drv.getPrimaryConnection(null));
                    continue;
                } else if ((errMsg.contains("error: 215") || errMsg.contains("being dropped") || errMsg.contains("process of being dropped"))
                    && attempts++ < maxAttempts) {
                    // Transient on real MongoDB when DB/collection is being dropped concurrently.
                    try {
                        Thread.sleep(Math.max(50, drv.getSleepBetweenErrorRetries()));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw e;
                    }
                    // refresh connection in case the underlying server got stepped down / closed
                    try {
                        drv.releaseConnection(getConnection());
                    } catch (Exception ignore) {
                    }
                    setConnection(drv.getPrimaryConnection(null));
                    continue;
                } else if (e.getMongoCode() instanceof Number mc && mc.intValue() == 112) {
                    // Error 112 (WriteConflict): transient error — either a real write conflict
                    // between concurrent sessions OR the WiredTiger storage engine evicted the
                    // pinned transaction due to cache pressure ("-31800: oldest pinned transaction
                    // ID rolled back for eviction").
                    // Inside a transaction, the server has already aborted it — retrying the
                    // individual command is futile. Propagate immediately so the caller (e.g.
                    // MorphiumTransactionalInterceptor) can restart the entire transaction.
                    if (drv.isTransactionInProgress()) {
                        log.warn("WriteConflict (code 112) inside transaction — propagating to transaction layer");
                        throw e;
                    }
                    if (attempts++ < maxAttempts) {
                        log.warn("Transient WriteConflict (code 112) — retrying (attempt {}/{})", attempts, maxAttempts);
                        try {
                            Thread.sleep(Math.max(50, drv.getSleepBetweenErrorRetries()));
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            throw e;
                        }
                        continue;
                    }
                    throw e;
                } else if (e instanceof MorphiumDriverNetworkException && attempts++ < maxAttempts) {
                    // Connection to the primary died mid-write (e.g. primary crash / broken pipe).
                    // Retrying on the re-resolved primary gives at-least-once semantics, matching
                    // MongoDB's retryWrites behavior. Without this, every in-flight write during
                    // a failover is lost even though retriesOnNetworkError is configured.
                    log.warn("Network error during write ({}) - re-resolving primary, retry {}/{}", e.getMessage(), attempts, maxAttempts);
                    try {
                        drv.releaseConnection(getConnection());
                    } catch (Exception ignore) {
                    }
                    try {
                        Thread.sleep(Math.max(50, drv.getSleepBetweenErrorRetries()));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw e;
                    }
                    setConnection(drv.getPrimaryConnection(null));
                    continue;
                } else {
                    throw e;
                }
            }
        }

    }

    /**
     * True for errors indicating the node is not (or no longer) the primary and the
     * write was rejected: NotWritablePrimary(10107), PrimarySteppedDown(189),
     * ShutdownInProgress(91), InterruptedAtShutdown(11600),
     * InterruptedDueToReplStateChange(11602), NotPrimaryNoSecondaryOk(13435).
     */
    private boolean isStepDownError(MorphiumDriverException e, String lowerCaseMsg) {
        if (e.getMongoCode() instanceof Number mc) {
            int code = mc.intValue();
            if (code == 10107 || code == 189 || code == 91 || code == 11600 || code == 11602 || code == 13435) {
                return true;
            }
        }
        return lowerCaseMsg.contains("not primary") || lowerCaseMsg.contains("not master");
    }

    @SuppressWarnings("unchecked")
    private boolean isDbBeingDroppedWriteError(Map<String, Object> crs) {
        if (crs == null) {
            return false;
        }
        Object we = crs.get("writeErrors");
        if (!(we instanceof List) || ((List<?>) we).isEmpty()) {
            return false;
        }

        for (Object o : (List<?>) we) {
            if (!(o instanceof Map)) {
                return false;
            }
            Map<String, Object> err = (Map<String, Object>) o;
            Object codeObj = err.get("code");
            int code = (codeObj instanceof Number) ? ((Number) codeObj).intValue() : -1;
            if (code != 215) {
                return false;
            }
            Object msgObj = err.get("errmsg");
            String msg = msgObj == null ? "" : msgObj.toString().toLowerCase();
            if (!msg.contains("being dropped") && !msg.contains("process of being dropped")) {
                return false;
            }
        }
        return true;
    }
}
