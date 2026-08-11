package de.caluga.test.poppydb;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.poppydb.PoppyDB;
import de.caluga.test.mongo.suite.data.UncachedObject;

/**
 * Probe (temporary): write cost against a PoppyDB replica set vs a single node, both started
 * IN THIS JVM on 127.0.0.1 - no network, no VPN, no shared test infrastructure. Compares
 * individual store() against a batched storeList of the same document count.
 */
@Tag("manual")
public class LocalRsWriteProbe {

    private final Logger log = LoggerFactory.getLogger(LocalRsWriteProbe.class);
    private static final int N = Integer.getInteger("probe.n", 300);

    @Test
    public void probe() throws Exception {
        PoppyDB s1 = new PoppyDB(16116, "127.0.0.1", 1000, 60);
        PoppyDB s2 = new PoppyDB(16117, "127.0.0.1", 1000, 60);
        PoppyDB s3 = new PoppyDB(16118, "127.0.0.1", 1000, 60);
        PoppyDB single = new PoppyDB(16119, "127.0.0.1", 1000, 60);
        var rs = List.of(s1, s2, s3);

        try {
            for (var s : rs) {
                s.configureReplicaSet("rs_probe",
                    List.of("127.0.0.1:16116", "127.0.0.1:16117", "127.0.0.1:16118"), null, true, null);
            }
            for (var s : rs) {
                s.start();
            }
            single.start();

            AtomicReference<PoppyDB> primary = new AtomicReference<>();
            long deadline = System.currentTimeMillis() + 20000;
            while (System.currentTimeMillis() < deadline && primary.get() == null) {
                for (var s : rs) {
                    if (s.isPrimary()) {
                        primary.set(s);
                    }
                }
                Thread.sleep(100);
            }
            log.info("Primary: " + (primary.get() == null ? "KEINER" : primary.get().getPort()));

            measure("RS (3 Knoten, in-process)", List.of("127.0.0.1:16116", "127.0.0.1:16117", "127.0.0.1:16118"));
            measure("Single (1 Knoten, in-process)", List.of("127.0.0.1:16119"));
        } finally {
            for (var s : rs) {
                try {
                    s.shutdown();
                } catch (Exception ignored) {
                }
            }
            try {
                single.shutdown();
            } catch (Exception ignored) {
            }
        }
    }

    private void measure(String label, List<String> hosts) throws Exception {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.connectionSettings().setDatabase("probe");
        cfg.driverSettings().setDriverName(PooledDriver.driverName);
        for (String h : hosts) {
            cfg.clusterSettings().addHostToSeed(h.split(":")[0], Integer.parseInt(h.split(":")[1]));
        }
        Morphium m = new Morphium(cfg);

        try {
            m.dropCollection(UncachedObject.class, "p_single", null);
            m.dropCollection(UncachedObject.class, "p_bulk", null);
            Thread.sleep(300);

            List<Long> us = new ArrayList<>(N);
            for (int i = 0; i < N; i++) {
                UncachedObject o = new UncachedObject();
                o.setCounter(i);
                o.setStrValue("v");
                long t0 = System.nanoTime();
                m.store(o, "p_single", null);
                us.add((System.nanoTime() - t0) / 1000);
            }
            List<Long> sorted = new ArrayList<>(us);
            Collections.sort(sorted);
            long sum = 0;
            for (long v : us) {
                sum += v;
            }

            List<UncachedObject> lst = new ArrayList<>();
            for (int i = 0; i < N; i++) {
                UncachedObject o = new UncachedObject();
                o.setCounter(i);
                o.setStrValue("v");
                lst.add(o);
            }
            long t0 = System.nanoTime();
            m.storeList(lst, "p_bulk");
            long bulkMs = (System.nanoTime() - t0) / 1_000_000;

            System.out.println(String.format(
                "PROBE %-30s einzeln: avg=%.2f ms p50=%.2f p90=%.2f -> %.0f docs/s   |   storeList(%d): %d ms -> %.0f docs/s",
                label, sum / 1000.0 / N, sorted.get(N / 2) / 1000.0, sorted.get((int)(N * 0.9)) / 1000.0,
                N * 1_000_000.0 / sum, N, bulkMs, N * 1000.0 / Math.max(1, bulkMs)));
        } finally {
            m.close();
        }
    }
}
