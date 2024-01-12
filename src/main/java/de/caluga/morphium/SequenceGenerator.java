package de.caluga.morphium;

import de.caluga.morphium.Sequence.SeqLock;
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

/**
 * User: Stephan Bösebeck
 * Date: 24.07.12
 * Time: 21:36
 * <p/>
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

        try {
            if (!morphium.getDriver().exists(morphium.getConfig().getDatabase(), morphium.getMapper().getCollectionName(Sequence.class))
             || morphium.createQueryFor(Sequence.class).f("_id").eq(name).countAll() == 0) {
                // sequence does not exist yet
                if (log.isDebugEnabled()) {
                    log.debug("Sequence does not exist yet... inserting");
                }

                Sequence s = new Sequence();
                s.setCurrentValue(startValue - inc); // making sure first value will be startValue!
                s.setName(name);
                morphium.storeNoCache(s);
            }

            morphium.ensureIndicesFor(Sequence.class);
            morphium.ensureIndicesFor(SeqLock.class);
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }

    public long getCurrentValue() {
        Sequence s = morphium.createQueryFor(Sequence.class).f("_id").eq(name).get();

        if (s == null || s.getCurrentValue() == null) {
            // new sequence - get default
            return getNextValue();
        }

        return s.getCurrentValue();
    }

    public long getNextValue() {
        long start = System.currentTimeMillis();
        SeqLock lock = new SeqLock();
        lock.setName(name);
        lock.setLockedAt(new Date());
        lock.setLockedBy(id);

        while (true) {
            if (System.currentTimeMillis() - start > 100000) {
                throw new RuntimeException(String.format("Getting lock on seqence %s failed!", name));
            }

            try {
                morphium.insert(lock);
                break;
            } catch (Exception e) {
                // lock failed
                // waiting for it to be released
                try {
                    Thread.sleep((long)(100 * Math.random() + 10));
                } catch (InterruptedException ignored) {
                }
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
        Query<Sequence> seq = morphium.createQueryFor(Sequence.class).f("_id").eq(name);
        var val = seq.get();//.getCurrentValue();
        morphium.inc(val, "current_value", inc);
        morphium.delete(lock);
        // log.info(String.format("inc: %s ms", System.currentTimeMillis() - st));
        return val.getCurrentValue();
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
