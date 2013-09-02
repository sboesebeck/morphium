package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.Sequence;
import de.caluga.morphium.SequenceGenerator;
import org.junit.Test;

import java.util.Collections;
import java.util.Vector;

/**
 * User: Stephan Bösebeck
 * Date: 25.07.12
 * Time: 08:09
 * <p/>
 */
public class SequenceTest extends MongoTest {
    @Test
    public void singleSequenceTest() throws Exception {
        MorphiumSingleton.get().dropCollection(Sequence.class);
        SequenceGenerator sg = new SequenceGenerator(MorphiumSingleton.get(), "tstseq", 1, 1);
        long v = sg.getNextValue();
        assert (v == 1) : "Value wrong: " + v;
        v = sg.getNextValue();
        assert (v == 2);
    }

    @Test
    public void multiSequenceTest() throws Exception {
        MorphiumSingleton.get().dropCollection(Sequence.class);
        SequenceGenerator sg1 = new SequenceGenerator(MorphiumSingleton.get(), "tstseq1", 1, 1);
        SequenceGenerator sg2 = new SequenceGenerator(MorphiumSingleton.get(), "tstseq2", 1, 1);
        long v = sg1.getNextValue();
        assert (v == 1) : "Value wrong: " + v;
        v = sg2.getNextValue();
        v = sg2.getNextValue();
        assert (v == 2) : "Value wrong: " + v;

        v = sg1.getNextValue();
        assert (v == 2);
        v = sg2.getNextValue();
        v = sg2.getNextValue();
        assert (v == 4);
    }

    @Test
    public void errorLockedSequenceTest() throws Exception {
        MorphiumSingleton.get().dropCollection(Sequence.class);
        SequenceGenerator sg = new SequenceGenerator(MorphiumSingleton.get(), "test", 1, 1);
        sg.getNextValue(); //initializing

        Sequence s = MorphiumSingleton.get().createQueryFor(Sequence.class).f("name").eq("test").get();
        s.setLockedBy("noone");
        MorphiumSingleton.get().store(s);
        waitForWrites();
        //now sequence is blocked by someone else... waiting 30s
        long v = sg.getNextValue();
        log.info("Got next Value: " + v);
        assert (v == 2);


    }

    @Test
    public void massiveMultiSequenceTest() throws Exception {
        MorphiumSingleton.get().dropCollection(Sequence.class);
        Vector<SequenceGenerator> gens = new Vector<SequenceGenerator>();
        //creating lots of sequences
        for (int i = 0; i < 10; i++) {
            SequenceGenerator sg1 = new SequenceGenerator(MorphiumSingleton.get(), "tstseq_" + i, i % 3 + 1, i);
            gens.add(sg1);
        }

        log.info("getting values...");
        for (int i = 0; i < 200; i++) {
            log.info("" + i + "/200");
            int r = (int) (Math.random() * gens.size());
            SequenceGenerator g = gens.get(r);
            long v = g.getCurrentValue();
            long v2 = g.getNextValue();
            assert (v2 == v + g.getInc()) : "incremented wrong?";
        }
        log.info("done");
    }

    @Test
    public void massiveParallelSingleSequenceTest() throws Exception {
        MorphiumSingleton.get().dropCollection(Sequence.class);
        final SequenceGenerator sg1 = new SequenceGenerator(MorphiumSingleton.get(), "tstseq", 1, 0);
        Vector<Thread> thr = new Vector<Thread>();
        final Vector<Long> data = new Vector<Long>();
        for (int i = 0; i < 10; i++) {
            Thread t = new Thread() {
                public void run() {
                    for (int i = 0; i < 25; i++) {
                        long nv = sg1.getNextValue();
                        assert (!data.contains(Long.valueOf(nv))) : "Value already stored? Value: " + nv;
                        data.add(Long.valueOf(nv));
                        try {
                            sleep(10);
                        } catch (InterruptedException e) {
                        }
                    }
                }
            };
            t.start();
            thr.add(t);
        }
        log.info("Waiting for threads to finish");
        for (Thread t : thr) {
            t.join();
        }
        long last = -1;
        Collections.sort(data);
        for (Long l : data) {
            assert (last == l - 1);
            last = l;
        }
        log.info("done");
    }

    @Test
    public void massiveParallelMultiSequenceTest() throws Exception {
        MorphiumSingleton.get().dropCollection(Sequence.class);
        Vector<SequenceGenerator> gens = new Vector<SequenceGenerator>();
        //creating lots of sequences
        for (int i = 0; i < 20; i++) {
            SequenceGenerator sg1 = new SequenceGenerator(MorphiumSingleton.get(), "tstseq_" + i, i % 3 + 1, i);
            gens.add(sg1);
        }

        Vector<Thread> thr = new Vector<Thread>();
        for (final SequenceGenerator g : gens) {
            Thread t = new Thread() {
                public void run() {
                    double max = Math.random() * 100;
                    for (int i = 0; i < max; i++) {
                        long cv = g.getCurrentValue();
                        long nv = g.getNextValue();
                        assert (nv == cv + g.getInc());
                        try {
                            sleep(10);
                        } catch (InterruptedException e) {
                        }
                    }
                }
            };
            thr.add(t);
            log.info("started thread for seqence " + g.getName());
            t.start();
        }

        //joining threads
        log.info("Waiting for threads to finish");
        for (Thread t : thr) {
            t.join();
        }
        log.info("done!");
    }

}
