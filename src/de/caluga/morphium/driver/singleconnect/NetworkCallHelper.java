package de.caluga.morphium.driver.singleconnect;/**
 * Created by stephan on 09.11.15.
 */

import de.caluga.morphium.Logger;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumDriverNetworkException;
import de.caluga.morphium.driver.MorphiumDriverOperation;

import java.util.Map;

/**
 * Mongodb throws errors on failover to the client, so we need to repeat all network calls several times
 * this class capsulates these calls.
 **/
@SuppressWarnings("WeakerAccess")
public class NetworkCallHelper {
    private final Logger logger = new Logger(NetworkCallHelper.class);


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
