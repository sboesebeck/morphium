package de.caluga.morphium.driver.mongodb;/**
 * Created by stephan on 09.11.15.
 */

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
 * helper class
 */
@SuppressWarnings("WeakerAccess")
public class DriverHelper {
    //Logger logger = new Logger(DriverHelper.class);


    public static Map<String, Object> doCall(MorphiumDriverOperation r, int maxRetry, int sleep) throws MorphiumDriverException {
        for (int i = 0; i < maxRetry; i++) {
            try {
                return r.execute();
            } catch (Exception e) {
                handleNetworkError(maxRetry, i, sleep, e);
            }
        }
        return null;
    }


    private static void handleNetworkError(int max, int i, int sleep, Throwable e) throws MorphiumDriverException {
        new Logger(DriverHelper.class).info("Handling network error..." + e.getClass().getName());
        if (e.getClass().getName().equals("javax.validation.ConstraintViolationException")) {
            throw (new MorphiumDriverException("Validation error", e));
        }
        if (e.getClass().getName().contains("DuplicateKeyException")) {
            throw new MorphiumDriverException("Duplicate Key", e);
        }
        if (e.getClass().getName().contains("MongoExecutionTimeoutException")
                || e.getClass().getName().contains("MorphiumDriverNetworkException")
                || e.getClass().getName().contains("MongoTimeoutException")
                || e.getClass().getName().contains("MongoSocketReadTimeoutException")
                || e.getClass().getName().contains("MongoWaitQueueFullException")
                || e.getClass().getName().contains("MongoWriteConcernException")
                || e.getClass().getName().contains("MongoSocketReadException")
                || e.getClass().getName().contains("MongoSocketOpenException")
                || e.getClass().getName().contains("MongoSocketClosedException")
                || e.getClass().getName().contains("MongoSocketException")
                || e.getClass().getName().contains("MongoNotPrimaryException")
                || e.getClass().getName().contains("MongoInterruptedException")
                || e.getMessage() != null && (e.getMessage().equals("can't find a master")
                || e.getMessage().startsWith("No replica set members available in")
                || e.getMessage().equals("not talking to master and retries used up"))
                || (e.getClass().getName().contains("WriteConcernException") && e.getMessage() != null && e.getMessage().contains("not master"))
                || e.getClass().getName().contains("MongoException")) {
            if (i + 1 < max) {
                new Logger(DriverHelper.class).warn("Retry because of network error: " + e.getMessage());
                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException ignored) {
                }

            } else {
                new Logger(DriverHelper.class).info("no retries left - re-throwing exception");
                throw (new MorphiumDriverNetworkException("Network error error", e));
            }
        } else {
            throw (new MorphiumDriverException("internal error", e));
        }
    }

    public static void replaceMorphiumIdByObjectId(Object in) {
        if (in == null) {
            return;
        }
        if (in instanceof Map) {
            @SuppressWarnings("unchecked") Map<String, Object> m = (Map) in;
            Map<String, Object> toSet = new HashMap<>();
            try {
                for (Map.Entry e : m.entrySet()) {
                    if (e.getValue() instanceof MorphiumId) {
                        toSet.put((String) e.getKey(), new ObjectId(e.getValue().toString()));

                    } else if (e.getValue() instanceof MorphiumReference) {
                        toSet.put((String) e.getKey(), new ObjectId(((MorphiumReference) e.getValue()).getId().toString()));
                    } else if (e.getValue() instanceof Collection) {
                        for (Object o : (Collection) e.getValue()) {
                            if (o == null) {
                                continue;
                            }
                            if (o instanceof Map
                                    || o instanceof List
                                    || o.getClass().isArray()) {
                                replaceMorphiumIdByObjectId(o);
                            }
                        }
                    } else {
                        replaceMorphiumIdByObjectId(e.getValue());
                    }
                }
                for (Map.Entry<String, Object> e : toSet.entrySet()) {
                    //noinspection unchecked
                    ((Map) in).put(e.getKey(), e.getValue());
                }

            } catch (Exception e) {
                new Logger(DriverHelper.class).fatal("Error replacing mongoid", e);
                //                throw new RuntimeException(e);
            }
        } else if (in instanceof Collection) {
            Collection c = (Collection) in;
            //noinspection unchecked
            c.forEach(DriverHelper::replaceMorphiumIdByObjectId);
        } else if (in.getClass().isArray()) {

            for (int i = 0; i < Array.getLength(in); i++) {
                replaceMorphiumIdByObjectId(Array.get(in, i));
            }
        }
    }
}
