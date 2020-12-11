package de.caluga.morphium.driver.mongodb;/**
 * Created by stephan on 09.11.15.
 */

import de.caluga.morphium.MorphiumReference;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumDriverNetworkException;
import de.caluga.morphium.driver.MorphiumDriverOperation;
import de.caluga.morphium.driver.MorphiumId;
import org.bson.types.ObjectId;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.util.*;

/**
 * helper class
 */
@SuppressWarnings("WeakerAccess")
public class DriverHelper {
    //Logger logger = LoggerFactory.getLogger(DriverHelper.class);


    public static <V> V doCall(MorphiumDriverOperation<V> r, int maxRetry, int sleep) throws MorphiumDriverException {
        Exception lastException = null;
        for (int i = 0; i < maxRetry; i++) {
            try {
                V ret = r.execute();
                if (i > 0) {
                    if (lastException == null) {
                        LoggerFactory.getLogger(DriverHelper.class).debug("recovered from error without exception");
                    } else {
                        LoggerFactory.getLogger(DriverHelper.class).debug("recovered from error: " + lastException.getMessage());
                    }
                }
                return ret;
            } catch (IllegalStateException e1){
                //should be open...
            } catch (Exception e) {
                lastException = e;
                handleNetworkError(maxRetry, i, sleep, e);
            }
        }
        return null;
    }


    private static void handleNetworkError(int max, int i, int sleep, Throwable e) throws MorphiumDriverException {
        LoggerFactory.getLogger(DriverHelper.class).debug("Handling network error..." + e.getClass().getName());
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
                || e.getClass().getName().contains("MongoNodeIsRecoveringException")
                || e.getMessage() != null && (e.getMessage().equals("can't find a master")
                || e.getMessage().startsWith("No replica set members available in")
                || e.getMessage().equals("not talking to master and retries used up"))
                || (e.getClass().getName().contains("WriteConcernException") && e.getMessage() != null && e.getMessage().contains("not master"))
                || e.getClass().getName().contains("MongoException")) {
            if (i + 1 < max) {
                LoggerFactory.getLogger(DriverHelper.class).warn("Retry because of network error: " + e.getMessage());
                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException ignored) {
                }

            } else {
                LoggerFactory.getLogger(DriverHelper.class).info("no retries left - re-throwing exception");
                throw (new MorphiumDriverNetworkException("Network error error: " + e.getMessage(), e));
            }
        } else {
            throw (new MorphiumDriverException("internal error: " + e.getMessage(), e));
        }
    }

    public static void replaceBsonValues(List<Map<String, Object>> in) {
        if (in == null || in.isEmpty()) {
            return;
        }
        for (Map<String, Object> map : in) {
            replaceMorphiumIdByObjectId(map);
        }
    }

    public static void replaceBsonValues(Map<String, Object> m) {
        if (m == null || m.isEmpty()) {
            return;
        }
        try {
            for (Map.Entry<String, Object> e : m.entrySet()) {
                Object value = e.getValue();
                if (value instanceof MorphiumId) {
                    e.setValue(new ObjectId(value.toString()));
                } else if (value instanceof MorphiumReference) {
                    e.setValue(new ObjectId(((MorphiumReference) value).getId().toString()));
                } else if (value instanceof List) {
                    replaceMorphiumIdByObjectIdInList((List<Object>) value);
                } else if (value != null && value.getClass().isArray()) {
                    replaceMorphiumIdByObjectIdInArray(value);
                } else if (value instanceof Map) {
                    replaceMorphiumIdByObjectId((Map<String, Object>) value);
                } else if (value instanceof Collection) {
                    e.setValue(replaceMorphiumIdByObjectIdInCollection((Collection<Object>) value));
                }
            }
        } catch (Exception e) {
            LoggerFactory.getLogger(DriverHelper.class).error("Error replacing mongoid", e);
            // throw new RuntimeException(e);
        }
    }

    public static void replaceBsonValuesIdInList(List<Object> in) {
        if (in == null || in.isEmpty()) {
            return;
        }
        for (int i = 0; i < in.size(); i++) {
            Object element = in.get(i);
            if (element instanceof MorphiumId) {
                in.set(i, new ObjectId(element.toString()));
            } else if (element instanceof MorphiumReference) {
                in.set(i, new ObjectId(((MorphiumReference) element).getId().toString()));
            } else if (element instanceof List) {
                replaceMorphiumIdByObjectIdInList((List<Object>) element);
            } else if (element != null && element.getClass().isArray()) {
                replaceMorphiumIdByObjectIdInArray(element);
            } else if (element instanceof Map) {
                replaceMorphiumIdByObjectId((Map<String, Object>) element);
            } else if (element instanceof Collection) {
                in.set(i, replaceMorphiumIdByObjectIdInCollection((Collection<Object>) element));
            }
        }
    }

    public static void replaceBsonValuesInArray(Object o) {
        if (o == null) {
            return;
        }
        for (int i = 0; i < Array.getLength(o); i++) {
            Object arrayElement = Array.get(o, i);
            if (arrayElement instanceof MorphiumId) {
                Array.set(o, i, new ObjectId(arrayElement.toString()));
            } else if (arrayElement instanceof MorphiumReference) {
                Array.set(o, i, new ObjectId(((MorphiumReference) arrayElement).getId().toString()));
            } else if (arrayElement instanceof List) {
                replaceMorphiumIdByObjectIdInList((List<Object>) arrayElement);
            } else if (arrayElement != null && arrayElement.getClass().isArray()) {
                replaceMorphiumIdByObjectIdInArray(arrayElement);
            } else if (arrayElement instanceof Map) {
                replaceMorphiumIdByObjectId((Map<String, Object>) arrayElement);
            } else if (arrayElement instanceof Collection) {
                Array.set(o, i, replaceMorphiumIdByObjectIdInCollection((Collection<Object>) arrayElement));
            }
        }
    }

    public static Collection<Object> replaceBsonValuesInCollection(Collection<Object> collection) {
        boolean needToReplace = false;
        for(Object iv : collection) {
            if (iv instanceof MorphiumId || iv instanceof MorphiumReference) {
                needToReplace = true;
                break;
            }
        }
        if(needToReplace) {
            ArrayList<Object> copyOfIterable = new ArrayList<>(collection);
            replaceMorphiumIdByObjectIdInList(copyOfIterable);
            return copyOfIterable;
        }
        return collection;
    }

    
    
    public static void replaceMorphiumIdByObjectId(List<Map<String, Object>> in) {
        if (in == null || in.isEmpty()) {
            return;
        }
        for (Map<String, Object> map : in) {
            replaceMorphiumIdByObjectId(map);
        }
    }

    public static void replaceMorphiumIdByObjectId(Map<String, Object> m) {
        if (m == null || m.isEmpty()) {
            return;
        }
        try {
            for (Map.Entry<String, Object> e : m.entrySet()) {
                Object value = e.getValue();
                if (value instanceof MorphiumId) {
                    e.setValue(new ObjectId(value.toString()));
                } else if (value instanceof MorphiumReference) {
                    e.setValue(new ObjectId(((MorphiumReference) value).getId().toString()));
                } else if (value instanceof List) {
                    replaceMorphiumIdByObjectIdInList((List<Object>) value);
                } else if (value != null && value.getClass().isArray()) {
                    replaceMorphiumIdByObjectIdInArray(value);
                } else if (value instanceof Map) {
                    replaceMorphiumIdByObjectId((Map<String, Object>) value);
                } else if (value instanceof Collection) {
                    e.setValue(replaceMorphiumIdByObjectIdInCollection((Collection<Object>) value));
                }
            }
        } catch (Exception e) {
            LoggerFactory.getLogger(DriverHelper.class).error("Error replacing mongoid", e);
            // throw new RuntimeException(e);
        }
    }

    public static void replaceMorphiumIdByObjectIdInList(List<Object> in) {
        if (in == null || in.isEmpty()) {
            return;
        }
        for (int i = 0; i < in.size(); i++) {
            Object element = in.get(i);
            if (element instanceof MorphiumId) {
                in.set(i, new ObjectId(element.toString()));
            } else if (element instanceof MorphiumReference) {
                in.set(i, new ObjectId(((MorphiumReference) element).getId().toString()));
            } else if (element instanceof List) {
                replaceMorphiumIdByObjectIdInList((List<Object>) element);
            } else if (element != null && element.getClass().isArray()) {
                replaceMorphiumIdByObjectIdInArray(element);
            } else if (element instanceof Map) {
                replaceMorphiumIdByObjectId((Map<String, Object>) element);
            } else if (element instanceof Collection) {
                in.set(i, replaceMorphiumIdByObjectIdInCollection((Collection<Object>) element));
            }
        }
    }

    public static void replaceMorphiumIdByObjectIdInArray(Object o) {
        if (o == null) {
            return;
        }
        for (int i = 0; i < Array.getLength(o); i++) {
            Object arrayElement = Array.get(o, i);
            if (arrayElement instanceof MorphiumId) {
                Array.set(o, i, new ObjectId(arrayElement.toString()));
            } else if (arrayElement instanceof MorphiumReference) {
                Array.set(o, i, new ObjectId(((MorphiumReference) arrayElement).getId().toString()));
            } else if (arrayElement instanceof List) {
                replaceMorphiumIdByObjectIdInList((List<Object>) arrayElement);
            } else if (arrayElement != null && arrayElement.getClass().isArray()) {
                replaceMorphiumIdByObjectIdInArray(arrayElement);
            } else if (arrayElement instanceof Map) {
                replaceMorphiumIdByObjectId((Map<String, Object>) arrayElement);
            } else if (arrayElement instanceof Collection) {
                Array.set(o, i, replaceMorphiumIdByObjectIdInCollection((Collection<Object>) arrayElement));
            }
        }
    }

    public static Collection<Object> replaceMorphiumIdByObjectIdInCollection(Collection<Object> collection) {
        boolean needToReplace = false;
        for(Object iv : collection) {
            if (iv instanceof MorphiumId || iv instanceof MorphiumReference) {
                needToReplace = true;
                break;
            }
        }
        if(needToReplace) {
            ArrayList<Object> copyOfIterable = new ArrayList<>(collection);
            replaceMorphiumIdByObjectIdInList(copyOfIterable);
            return copyOfIterable;
        }
        return collection;
    }

}
