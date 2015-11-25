package de.caluga.morphium.driver.mongodb;/**
 * Created by stephan on 09.11.15.
 */

import com.mongodb.*;
import de.caluga.morphium.Logger;
import de.caluga.morphium.MorphiumReference;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumDriverNetworkException;
import de.caluga.morphium.driver.MorphiumDriverOperation;
import de.caluga.morphium.driver.bson.MorphiumId;
import org.bson.types.ObjectId;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
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
        if (e instanceof MongoExecutionTimeoutException
                || e instanceof MongoTimeoutException
                || e instanceof MongoSocketReadTimeoutException
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

    public void replaceMorphiumIdByObjectId(Object in) {
        if (in == null) return;
        if (in instanceof Map) {
            Map<String, Object> m = (Map) in;
            Map<String, Object> toSet = new HashMap<>();
            try {
                for (Map.Entry e : m.entrySet()) {
                    if (e.getValue() instanceof MorphiumId) {
                        toSet.put((String) e.getKey(), new ObjectId(((MorphiumId) e.getValue()).toString()));

                    } else if (e.getValue() instanceof MorphiumReference) {
                        toSet.put((String) e.getKey(), new ObjectId(((MorphiumReference) e.getValue()).getId().toString()));
                    } else if (e.getValue() instanceof Collection) {
                        for (Object o : (Collection) e.getValue()) {
                            if (o == null) continue;
                            if (o instanceof Map) {
                                replaceMorphiumIdByObjectId((Map) o);
                            } else if (o instanceof List) {
                                replaceMorphiumIdByObjectId(o);
                            } else if (o.getClass().isArray()) {
                                replaceMorphiumIdByObjectId(o);
                            }
                        }
                    } else {
                        replaceMorphiumIdByObjectId(e.getValue());
                    }
                }
                for (Map.Entry<String, Object> e : toSet.entrySet()) {
                    ((Map) in).put(e.getKey(), e.getValue());
                }

            } catch (Exception e) {
                logger.fatal("Error replacing mongoid", e);
                //TODO: Implement Handling
//                throw new RuntimeException(e);
            }
        } else if (in instanceof Collection) {
            Collection c = (Collection) in;
            c.forEach(this::replaceMorphiumIdByObjectId);
        } else if (in.getClass().isArray()) {

            for (int i = 0; i < Array.getLength(in); i++) {
                replaceMorphiumIdByObjectId(Array.get(in, i));
            }
        }
    }
}
