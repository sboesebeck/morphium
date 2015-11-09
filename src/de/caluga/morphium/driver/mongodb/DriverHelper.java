package de.caluga.morphium.driver.mongodb;/**
 * Created by stephan on 09.11.15.
 */

import com.mongodb.*;
import de.caluga.morphium.Logger;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumDriverNetworkException;
import de.caluga.morphium.driver.MorphiumDriverOperation;

import java.util.Map;

/**
 * TODO: Add Documentation here
 **/
public class DriverHelper {
    Logger logger = new Logger(DriverHelper.class);


    public Map<String, Object> doCall(MorphiumDriverOperation r, int maxRetry, int sleep) throws MorphiumDriverException {
        for (int i = 0; i < maxRetry; i++) {
            try {
                return r.execute();
            } catch (Exception e) {
                handleNetworkError(maxRetry, i, sleep, e);
            }
        }
        return null;
    }


    private void handleNetworkError(int max, int i, int sleep, Throwable e) throws MorphiumDriverException {
        logger.info("Handling network error..." + e.getClass().getName());
        if (e.getClass().getName().equals("javax.validation.ConstraintViolationException")) {
            throw (new MorphiumDriverException("Validation error", e));
        }
        if (e instanceof DuplicateKeyException) {
            throw new MorphiumDriverException("Duplicate Key", e);
        }
        if (e instanceof MongoExecutionTimeoutException || e instanceof MongoTimeoutException || e instanceof MongoSocketReadTimeoutException
                || e instanceof MongoWaitQueueFullException
                || e instanceof MongoWriteConcernException
                || e instanceof MongoSocketReadException
                || e instanceof MongoSocketOpenException
                || e instanceof MongoSocketClosedException
                || e instanceof MongoSocketException
                || e instanceof MongoNotPrimaryException
                || e instanceof MongoInterruptedException
                || e.getMessage() != null && (e.getMessage().equals("can't find a master")
                || e.getMessage().startsWith("No replica set members available in")
                || e.getMessage().equals("not talking to master and retries used up"))
                || (e instanceof WriteConcernException && e.getMessage() != null && e.getMessage().contains("not master"))
                || e instanceof MongoException) {
            if (i + 1 < max) {
                logger.warn("Retry because of network error: " + e.getMessage());
                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException ignored) {
                }

            } else {
                logger.info("no retries left - re-throwing exception");
                throw (new MorphiumDriverNetworkException("Network error error", e));
            }
        } else {
            throw (new MorphiumDriverException("internal error", e));
        }
    }
}
