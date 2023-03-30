package de.caluga.morphium.driver.wire;
/**
 * Created by stephan on 09.11.15.
 */


import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumDriverNetworkException;
import de.caluga.morphium.driver.MorphiumDriverOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mongodb throws errors on failover to the client, so we need to repeat all network calls several times
 * this class capsulates these calls.
 **/
@SuppressWarnings("WeakerAccess")
public class NetworkCallHelper<T> {
    private final Logger logger = LoggerFactory.getLogger(NetworkCallHelper.class);


    public T doCall(MorphiumDriverOperation r, int maxRetry, int sleep, ErrorCallback err) throws MorphiumDriverException {
        if (maxRetry == 0) {
            logger.error("MaxRetry set to 0?!?!?! Does not make sense! defaulting to 1");
            maxRetry = 1;
        }

        for (int i = 0; i < maxRetry; i++) {
            try {
                return (T) r.execute();
            } catch (Exception e) {
                handleNetworkError(maxRetry, i, sleep, e,err);
            }
        }

        return null;
    }

    public T doCall(MorphiumDriverOperation r, int maxRetry, int sleep) throws MorphiumDriverException {
        return doCall(r,maxRetry,sleep,null);
    }


    private void handleNetworkError(int max, int i, int sleep, Throwable e, ErrorCallback err) throws MorphiumDriverException {
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

                if (err != null) {
                    try {
                        err.onError(e);
                    } catch (Exception e1) {
                        throw new MorphiumDriverException("error callback failed",e1);
                    }
                }

                throw(new MorphiumDriverNetworkException("Network error error", e));
            }
        } else if (e instanceof MorphiumDriverException) {
            throw((MorphiumDriverException) e);
        } else {
            throw(new MorphiumDriverException("internal error", e));
        }
    }

    public interface ErrorCallback {
        public void onError(Throwable e) throws Exception;
    }

}
