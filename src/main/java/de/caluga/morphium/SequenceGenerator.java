package de.caluga.morphium;

import de.caluga.morphium.Sequence.SeqLock;
import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * User: Stephan Bösebeck
 * Date: 24.07.12
 * Time: 21:36
 * <p>
 * Generate a new unique sequence number. Uses Sequence to store the current
 * value. Process is as follows:
 * <ul>
 * <li>lock the entry in sequence collection with my own UUID (rather unique)
 * using atomar $set operation</li>
 * <li>read my locked entries (should only be one). If one is found, I have a
 * valid lock. If not found, somebody
 * locked over. Wait a while and try again
 * </li>
 * <li>increase the number by one (or the given increment) on the locked
 * element, get this value</li>
 * <li>remove the lock (atomar using $set)</li>
 * <li>return current value</li>
 * </ul>
 */

@SuppressWarnings({"UnusedDeclaration", "BusyWait"})
public class SequenceGenerator {
    private static final Logger log = LoggerFactory.getLogger(SequenceGenerator.class);
    // Keep in sync with {@link Sequence.SeqLock#lockedAt} TTL index (expireAfterSeconds:30).
    private static final long LOCK_EXPIRE_MILLIS = TimeUnit.SECONDS.toMillis(30);
    private static final long LOCK_EXPIRE_GRACE_MILLIS = TimeUnit.SECONDS.toMillis(5);
    private int inc;
    private long startValue;
    private Morphium morphium;
    private String id;
    private String name;

    public SequenceGenerator(Morphium m, String n) {
        this(m, n, 1, 1);
    }

    public SequenceGenerator(Morphium mrph, String name, int inc, long startValue) {
        this.inc = inc;

        if (inc == 0) {
            throw new IllegalArgumentException("Cannot use increment value 0!");
        }

        this.name = name;
        this.startValue = startValue;
        this.morphium = mrph;
        id = UUID.randomUUID().toString();

        String db = morphium.getConfig().connectionSettings().getDatabase();

        log.info("SequenceGenerator init: db={}, name={}, startValue={}, inc={}, thread={}",
            db, name, startValue, inc, Thread.currentThread().getName());

        // Always try to insert the sequence - let duplicate detection handle the race condition
        // This ensures atomicity: only ONE thread will successfully create the sequence
        try {
            Sequence s = new Sequence();
            s.setCurrentValue(startValue - inc); // making sure first value will be startValue!
            s.setName(name);
            // Use insert() instead of storeNoCache() to ensure duplicate detection
            // storeNoCache() uses upsert which would silently overwrite
            morphium.insert(s);
            log.info("Sequence '{}' inserted successfully in db '{}' with initial value={}, thread={}",
                name, db, (startValue - inc), Thread.currentThread().getName());
        } catch (Exception e) {
            // Ignore duplicate key errors - another thread already created it
            // This is expected when multiple threads try to initialize the same sequence
            log.info("Sequence '{}' already exists in db '{}' (this is normal in concurrent scenarios), thread={}: {}",
                name, db, Thread.currentThread().getName(), e.getMessage());
            if (log.isDebugEnabled()) {
                log.debug("Full exception:", e);
            }
        }

        morphium.ensureIndicesFor(Sequence.class);
        morphium.ensureIndicesFor(SeqLock.class);
    }

    public long getCurrentValue() {
        Sequence s = morphium.createQueryFor(Sequence.class)
            .setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY)
            .f("_id").eq(name).get();

        if (s == null) {
            log.warn("getCurrentValue() - Sequence '{}' is NULL, calling getNextValue(), thread={}",
                name, Thread.currentThread().getName());
            return getNextValue();
        }

        if (s.getCurrentValue() == null) {
            log.warn("getCurrentValue() - Sequence '{}' exists but currentValue is NULL, calling getNextValue(), thread={}",
                name, Thread.currentThread().getName());
            return getNextValue();
        }

        return s.getCurrentValue();
    }

    public long getNextValue() {
        long start = System.currentTimeMillis();
        SeqLock lock = new SeqLock();
        lock.setName(name);
        lock.setLockedBy(id);

        while (true) {
            if (System.currentTimeMillis() - start > 100_000) {
                throw new RuntimeException(String.format("Getting lock on seqence %s failed!", name));
            }

            try {
                lock.setLockedAt(new Date());
                morphium.insert(lock);
                break;
            } catch (Exception e) {
                // lock failed
                // waiting for it to be released (or for a stale lock to be cleared)
                try {
                    SeqLock existingLock = morphium.createQueryFor(SeqLock.class)
                        .setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY)
                        .f("_id").eq(name).get();

                    if (existingLock != null && existingLock.getLockedAt() != null) {
                        long age = System.currentTimeMillis() - existingLock.getLockedAt().getTime();

                        // MongoDB's TTL monitor only runs periodically and may take >expireAfterSeconds to delete.
                        // To keep sequence acquisition deterministic (and avoid test flakiness), proactively clear
                        // stale locks once they are beyond the configured TTL (+ grace).
                        if (age > LOCK_EXPIRE_MILLIS + LOCK_EXPIRE_GRACE_MILLIS) {
                            morphium.delete(
                                morphium.createQueryFor(SeqLock.class)
                                    .setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY)
                                    .f("_id").eq(name)
                                    .f("lockedBy").eq(existingLock.getLockedBy())
                                    .f("lockedAt").eq(existingLock.getLockedAt())
                            );
                        }
                    }
                } catch (Exception ignored) {
                    // ignore all errors during stale-lock cleanup; we will retry acquiring the lock
                }

                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos((long)(100 * Math.random() + 10)));
                // try {
                //     Thread.sleep((long)(100 * Math.random() + 10));
                // } catch (InterruptedException ignored) {
                // }
            }

            // MongoConnection con = null;
            //
            // try {
            // con = morphium.getDriver().getPrimaryConnection(null);
            // long st = System.currentTimeMillis();
            // InsertMongoCommand ins = new InsertMongoCommand(con);
            // ins.setDb(morphium.getDatabase());
            // ins.setColl(morphium.getMapper().getCollectionName(SeqLock.class));
            // ins.setDocuments(Arrays.asList(morphium.getMapper().serialize(lock)));
            // ins.executeAsync();
            // // log.info(String.format("insert lock: %s ms", System.currentTimeMillis() -
            // st));
            // break;
            // } catch (Exception e) {
            // // lock failed
            // // waiting for it to be released
            // try {
            // Thread.sleep((long)(100 * Math.random() + 10));
            // } catch (InterruptedException ignored) {
            // }
            // } finally {
            // if (con != null) {
            // con.release();
            // }
            // }
        }

        // long st = System.currentTimeMillis();
        try {
            Query<Sequence> seq = morphium.createQueryFor(Sequence.class)
                .setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY)
                .f("_id").eq(name);
            var val = seq.get();//.getCurrentValue();
            int count = 0;
            while (val == null || val.getCurrentValue() == null) {

                //cannot be - retry
                if (count++ > morphium.getConfig().connectionSettings().getRetriesOnNetworkError()) {
                    throw new RuntimeException("Could not read from Sequence");
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(
                        morphium.getConfig().connectionSettings().getSleepBetweenNetworkErrorRetries()));

                val = seq.get();
            }
            morphium.inc(val, "current_value", inc);
            // log.info(String.format("inc: %s ms", System.currentTimeMillis() - st));
            // WARNING: morphium.inc() updates the local object by adding to its current value,
            // but in concurrent scenarios the local value may be stale. Re-read from DB to get correct value.

            // Retry reading the sequence with backoff in case of race conditions
            count = 0;
            while (true) {
                val = seq.get();
                if (val != null && val.getCurrentValue() != null) {
                    // Under very high concurrency (especially with in-memory drivers), we may briefly
                    // observe an incomplete/old read. Never return values below the configured startValue.
                    if (val.getCurrentValue() >= startValue) {
                        break;
                    }
                }

                // Sequence disappeared - retry with backoff
                if (count++ > morphium.getConfig().connectionSettings().getRetriesOnNetworkError()) {
                    throw new RuntimeException("Sequence disappeared after increment!");
                }

                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(
                        morphium.getConfig().connectionSettings().getSleepBetweenNetworkErrorRetries()));
            }

            return val.getCurrentValue();
        } finally {
            // Always delete the lock, even if an exception occurs
            morphium.delete(lock);
        }
    }

    public int getInc() {
        return inc;
    }

    public void setInc(int inc) {
        this.inc = inc;
    }

    public long getStartValue() {
        return startValue;
    }

    public void setStartValue(long startValue) {
        this.startValue = startValue;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Morphium getMorphium() {
        return morphium;
    }

    public void setMorphium(Morphium morphium) {
        this.morphium = morphium;
    }

}
