package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumAccessVetoException;
import de.caluga.morphium.MorphiumStorageListener;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("core")
public class MorphiumTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testListDatabases(Morphium morphium) throws Exception  {
        createUncachedObjects(morphium, 1);
        TestUtils.waitForConditionToBecomeTrue(5000, "Collection not listed",
                () -> morphium.listCollections().contains(morphium.getMapper().getCollectionName(UncachedObject.class)));
        assertFalse(morphium.listDatabases().isEmpty());
        assertTrue(morphium.listDatabases().contains(morphium.getConfig().connectionSettings().getDatabase()));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testStorageListener(Morphium morphium) throws Exception  {
        AtomicInteger preStore = new AtomicInteger(0);

        AtomicInteger postStore = new AtomicInteger(0);

        AtomicInteger preRemove = new AtomicInteger(0);

        AtomicInteger postRemove = new AtomicInteger(0);

        AtomicInteger postDrop = new AtomicInteger(0);
        AtomicInteger preDrop = new AtomicInteger(0);
        AtomicInteger postLoad = new AtomicInteger(0);

        AtomicInteger preUpdate = new AtomicInteger(0);
        AtomicInteger postUpdate = new AtomicInteger(0);
        MorphiumStorageListener lst = new MorphiumStorageListener() {
            @Override
            public void preStore(Morphium m, Object r, boolean isNew) throws MorphiumAccessVetoException {
                preStore.incrementAndGet();
            }

            @Override
            public void preStore(Morphium m, Map isNew) throws MorphiumAccessVetoException {
                preStore.incrementAndGet();
            }

            @Override
            public void postStore(Morphium m, Object r, boolean isNew) {
                postStore.incrementAndGet();
            }

            @Override
            public void postStore(Morphium m, Map isNew) {
                postStore.incrementAndGet();
            }

            @Override
            public void preRemove(Morphium m, Query q) throws MorphiumAccessVetoException {
                preRemove.incrementAndGet();
            }

            @Override
            public void preRemove(Morphium m, Object r) throws MorphiumAccessVetoException {
                preRemove.incrementAndGet();
            }

            @Override
            public void postRemove(Morphium m, Object r) {
                postRemove.incrementAndGet();
            }

            @Override
            public void postRemove(Morphium m, List lst) {
                postRemove.incrementAndGet();
            }

            @Override
            public void postDrop(Morphium m, Class cls) {
                postDrop.incrementAndGet();
            }

            @Override
            public void preDrop(Morphium m, Class cls) throws MorphiumAccessVetoException {
                preDrop.incrementAndGet();
            }

            @Override
            public void postRemove(Morphium m, Query q) {
                postRemove.incrementAndGet();
            }

            @Override
            public void postLoad(Morphium m, Object o) {
                postLoad.incrementAndGet();
            }

            @Override
            public void postLoad(Morphium m, List o) {
                postLoad.incrementAndGet();
            }

            @Override
            public void preUpdate(Morphium m, Class cls, Enum updateType) throws MorphiumAccessVetoException {
                preUpdate.incrementAndGet();
            }

            @Override
            public void postUpdate(Morphium m, Class cls, Enum updateType) {
                postUpdate.incrementAndGet();
            }
        };

        morphium.addListener(lst);

        UncachedObject uc = new UncachedObject("value", 12);
        morphium.store(uc);
        TestUtils.waitForConditionToBecomeTrue(5000, "Store listeners not called",
                () -> postStore.get() == 1);
        assertEquals(1, preStore.get());

        morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).get();
        assertTrue((postLoad.get() == 1));

        postLoad.set(0);
        Thread.sleep(500);
        morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).asList();
        assertTrue((postLoad.get() == 2)); //one for each element, one for the whole list - two listeners!

        morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).delete();
        assertTrue((preRemove.get() == 1));
        assertTrue((postRemove.get() == 1));


        morphium.dropCollection(UncachedObject.class);
        assertTrue((preDrop.get() == 1));
        assertTrue((postDrop.get() == 1));

        morphium.removeListener(lst);
        preStore.set(0);
        uc = new UncachedObject("value", 12);
        morphium.store(uc);
        Thread.sleep(50);
        assertTrue((preStore.get() == 0));
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testUnset(Morphium morphium) throws Exception  {
        UncachedObject uc = new UncachedObject("val", 123);
        morphium.store(uc);
        TestUtils.waitForConditionToBecomeTrue(5000, "Object not stored",
                () -> morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).countAll() == 1);
        morphium.unsetInEntity(uc, UncachedObject.Fields.strValue);
        TestUtils.waitForConditionToBecomeTrue(5000, "Unset not persisted", () -> {
            morphium.reread(uc);
            return uc.getStrValue() == null;
        });
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testSet(Morphium morphium) throws Exception  {
        UncachedObject uc = new UncachedObject("val", 123);
        morphium.store(uc);
        TestUtils.waitForConditionToBecomeTrue(5000, "Object not stored",
                () -> morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).countAll() == 1);
        morphium.setInEntity(uc, UncachedObject.Fields.strValue, "other");
        assertTrue((uc.getStrValue().equals("other")));
        TestUtils.waitForConditionToBecomeTrue(5000, "Set not persisted", () -> {
            morphium.reread(uc);
            return "other".equals(uc.getStrValue());
        });
    }


}
