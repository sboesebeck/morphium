package de.caluga.morphium;

import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * User: Stephan Bösebeck
 * Date: 24.07.12
 * Time: 21:36
 * <p/>
 * TODO: Add documentation here
 */
public class SequenceGenerator {
    private int inc;
    private long startValue;
    private Morphium morphium;
    private String id;
    private String name;

    private static Logger log = Logger.getLogger(SequenceGenerator.class);

    public SequenceGenerator() {
        this(MorphiumSingleton.get(), "seq", 1, 1);
    }

    public SequenceGenerator(Morphium m, String n) {
        this(m, n, 1, 1);
    }

    public SequenceGenerator(Morphium mrph, String name, int inc, long startValue) {
        this.inc = inc;
        this.name = name;
        this.startValue = startValue;
        this.morphium = mrph;
        id = UUID.randomUUID().toString();
    }

    public long getNextValue() {
        if (!morphium.getDatabase().collectionExists(morphium.getConfig().getMapper().getCollectionName(Sequence.class))) {
            //sequence does not exist yet
            Query<Sequence> seq = morphium.createQueryFor(Sequence.class);
            seq.f(Sequence.PROPERTYNAME_NAME).eq(name);

            Map<String, Object> values = new HashMap<String, Object>();
            values.put("locked_by", id);
            morphium.set(seq, values, true, false);
            morphium.ensureIndicesFor(Sequence.class);
            //inserted
        }

        Query<Sequence> seq = morphium.createQueryFor(Sequence.class);
        seq.f(Sequence.PROPERTYNAME_NAME).eq(name).f(Sequence.PROPERTYNAME_LOCKED_BY).eq(null);

        Map<String, Object> values = new HashMap<String, Object>();
        values.put("locked_by", id);
        morphium.set(seq, values, false, false);

        seq = morphium.createQueryFor(Sequence.class);
        seq.f(Sequence.PROPERTYNAME_NAME).eq(name).f(Sequence.PROPERTYNAME_LOCKED_BY).eq(id);

        if (seq.countAll() == 0) {
            //locking failed... wait a moment, try again
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
            return getNextValue();
        }
        if (seq.countAll() > 1) {
            log.error("sequence name / locked by not unique??? - using first");
        }

        values.clear();
        morphium.inc(seq, Sequence.PROPERTYNAME_CURRENT_VALUE, inc);

        Sequence s = seq.get();
        s.setLockedBy(null);
        morphium.store(s);

        return s.getCurrentValue();

    }

}
