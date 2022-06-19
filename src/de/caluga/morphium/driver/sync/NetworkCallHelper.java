package de.caluga.morphium.driver.sync;/**
 * Created by stephan on 09.11.15.
 */


import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumDriverNetworkException;
import de.caluga.morphium.driver.MorphiumDriverOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Mongodb throws errors on failover to the client, so we need to repeat all network calls several times
 * this class capsulates these calls.
 **/
@SuppressWarnings("WeakerAccess")
public class NetworkCallHelper {
    private final Logger logger = LoggerFactory.getLogger(NetworkCallHelper.class);


    public Map<String, Object> doCall(MorphiumDriverOperation r, int maxRetry, int sleep) throws MorphiumDriverException {
        for (int i = 0; i < maxRetry; i++) {
            try {
                return (Map<String, Object>) r.execute();
            } catch (Exception e) {
                handleNetworkError(maxRetry, i, sleep, e);
            }
        }
        return null;
    }


    private void handleNetworkError(int max, int i, int sleep, Throwable e) throws MorphiumDriverException {
        logger.info("Handling network error..." + e.getClass().getName());

        if (e instanceof MorphiumDriverNetworkException) {
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
