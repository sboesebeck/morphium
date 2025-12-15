package de.caluga.morphium.driver.commands;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriverException;
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
            //noinspection unchecked
            setMetaData("server", con.getConnectedTo());
            long start = System.currentTimeMillis();
            int msg = con.sendCommand(this);

            try {
                var crs = con.readSingleAnswer(msg);
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
                if ("not primary".equals(e.getMessage())) {
                    drv.releaseConnection(getConnection());
                    log.warn("node no primary anymore - waiting for failover");
                    try {
                        Thread.sleep(drv.getHeartbeatFrequency() * 2L);
                    } catch (InterruptedException e1) {
                        Thread.currentThread().interrupt();
                        throw e;
                    }
                    log.warn("Retrying... (recursive)");
                    setConnection(drv.getPrimaryConnection(null));
                    // retrying
                    return execute();
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
                } else {
                    throw e;
                }
            }
        }

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
