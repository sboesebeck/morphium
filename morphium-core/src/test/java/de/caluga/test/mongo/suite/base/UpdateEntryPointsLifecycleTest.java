package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.LastChange;
import de.caluga.morphium.annotations.Version;
import de.caluga.morphium.annotations.lifecycle.Lifecycle;
import de.caluga.morphium.annotations.lifecycle.PostStore;
import de.caluga.morphium.annotations.lifecycle.PreStore;
import de.caluga.morphium.query.ReturnDocument;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MOR-027: what each entry point of an update does with the lifecycle of an entity. The page {@code docs/howtos/update-pitfalls.md} states this table; the test pins it.
 * {@code store()} calls {@code @PreStore} and {@code @PostStore}; the update through a query has no entity instance and calls neither. The annotations that describe the document
 * ({@code @Version}, {@code @LastChange}) are maintained by the builder and by {@code store()}; {@code Query.set} and {@code findOneAndUpdate(Map)} set {@code @LastChange} only for a class that
 * carries the annotation on the type as well, and do not touch the version (MOR-071).
 */
@Tag("core")
public class UpdateEntryPointsLifecycleTest extends MultiDriverTestBase {
    static final AtomicInteger PRE = new AtomicInteger();
    static final AtomicInteger POST = new AtomicInteger();

    @Entity
    @Lifecycle
    public static class Doc {
        @Id
        public String id;
        public String name;
        public int hits;
        @Version
        public Long version;
        @LastChange
        public long changedAt;

        @PreStore
        public void pre() {
            PRE.incrementAndGet();
        }

        @PostStore
        public void post() {
            POST.incrementAndGet();
        }
    }

    private Doc fresh(Morphium m) {
        m.dropCollection(Doc.class);
        Doc d = new Doc();
        d.id = "d1";
        d.name = "a";
        m.store(d);
        PRE.set(0);
        POST.set(0);
        return d;
    }

    private Doc load(Morphium m) {
        return m.createQueryFor(Doc.class).f("id").eq("d1").get();
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void storeCallsTheCallbacksAndBumpsTheVersion(Morphium m) throws Exception {
        try (m) {
            Doc d = fresh(m);
            long v = load(m).version;
            d = load(m);
            d.name = "b";
            m.store(d);
            assertTrue(PRE.get() >= 1, "@PreStore is called by store()");
            assertTrue(POST.get() >= 1, "@PostStore is called by store()");
            assertEquals(v + 1, load(m).version);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theBuilderCallsNoCallbackButMaintainsTheVersionAndTheLastChange(Morphium m) throws Exception {
        try (m) {
            fresh(m);
            Doc before = load(m);
            Thread.sleep(5);
            m.createQueryFor(Doc.class).f("id").eq("d1").modify().set("name", "b").findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(0, PRE.get());
            assertEquals(0, POST.get());
            assertEquals(before.version + 1, load(m).version);
            assertTrue(load(m).changedAt > before.changedAt);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void findOneAndUpdateOfAMapCallsNoCallback(Morphium m) throws Exception {
        try (m) {
            fresh(m);
            Doc before = load(m);
            Thread.sleep(5);
            m.createQueryFor(Doc.class).f("id").eq("d1").findOneAndUpdate(Map.of("$set", Map.of("name", "b")), false, true);
            assertEquals(0, PRE.get());
            assertEquals(0, POST.get());
            // PINNED DEVIATION MOR-071: the raw map path maintains neither the version nor a last change that is only declared on the field
            assertEquals(before.version, load(m).version);
            assertEquals(before.changedAt, load(m).changedAt);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void querySetCallsNoCallback(Morphium m) throws Exception {
        try (m) {
            fresh(m);
            Doc before = load(m);
            Thread.sleep(5);
            m.createQueryFor(Doc.class).f("id").eq("d1").set("name", "b");
            assertEquals(0, PRE.get());
            assertEquals(0, POST.get());
            // PINNED DEVIATION MOR-071, as above
            assertEquals(before.version, load(m).version);
            assertEquals(before.changedAt, load(m).changedAt);
        }
    }

    @Entity
    @LastChange
    public static class TypeLevel {
        @Id
        public String id;
        public String name;
        @LastChange
        public long changedAt;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void querySetSetsTheLastChangeWhenTheClassCarriesTheAnnotationToo(Morphium m) throws Exception {
        try (m) {
            m.dropCollection(TypeLevel.class);
            TypeLevel t = new TypeLevel();
            t.id = "t1";
            t.name = "a";
            m.store(t);
            long before = m.createQueryFor(TypeLevel.class).f("id").eq("t1").get().changedAt;
            Thread.sleep(5);
            m.createQueryFor(TypeLevel.class).f("id").eq("t1").set("name", "b");
            assertTrue(m.createQueryFor(TypeLevel.class).f("id").eq("t1").get().changedAt > before, "@LastChange on the class and on the field: Query.set sets it");
        }
    }
}
