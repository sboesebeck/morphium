package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.query.Query;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 28.05.12
 * Time: 17:17
 * <p/>
 * TODO: Add documentation here
 */
public class ListTests extends MongoTest {

    @Test
    public void simpleListTest() throws Exception {
        ListContainer lst = new ListContainer();
        int count = 2;

        for (int i = 0; i < count; i++) {
            EmbeddedObject eo = new EmbeddedObject();
            eo.setName("Embedded");
            eo.setValue("" + i);
            eo.setTest(i);
            lst.addEmbedded(eo);
        }

        for (int i = 0; i < count; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i);
            uc.setValue("A value - uncached!");
            //references should be stored automatically...
            lst.addRef(uc);
        }


        for (int i = 0; i < count; i++) {
            lst.addLong(i);
        }
        for (int i = 0; i < count; i++) {
            lst.addString("Value " + i);
        }

        MorphiumSingleton.get().store(lst);

        Query<ListContainer> q = MorphiumSingleton.get().createQueryFor(ListContainer.class).f("id").eq(lst.getId());
        q.setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY);
        ListContainer lst2 = q.get();
        assert (lst2 != null) : "Error - not found?";

        assert (lst2.getEmbeddedObjectList() != null) : "Embedded list null?";
        assert (lst2.getLongList() != null) : "Long list null?";
        assert (lst2.getRefList() != null) : "Ref list null?";
        assert (lst2.getStringList() != null) : "String list null?";

        for (int i = 0; i < count; i++) {

            assert (lst2.getEmbeddedObjectList().get(i).equals(lst.getEmbeddedObjectList().get(i))) : "Embedded objects list differ? - " + i;
            assert (lst2.getLongList().get(i).equals(lst.getLongList().get(i))) : "long list differ? - " + i;
            assert (lst2.getStringList().get(i).equals(lst.getStringList().get(i))) : "string list differ? - " + i;
            assert (lst2.getRefList().get(i).equals(lst.getRefList().get(i))) : "reference list differ? - " + i;
        }

    }

    @Test
    public void nullValueListTest() throws Exception {
        ListContainer lst = new ListContainer();
        int count = 2;

        for (int i = 0; i < count; i++) {
            EmbeddedObject eo = new EmbeddedObject();
            eo.setName("Embedded");
            eo.setValue("" + i);
            eo.setTest(i);
            lst.addEmbedded(eo);
        }
        lst.addEmbedded(null);

        for (int i = 0; i < count; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i);
            uc.setValue("A value - uncached!");
            //references should be stored automatically...
            lst.addRef(uc);
        }
        lst.addRef(null);


        for (int i = 0; i < count; i++) {
            lst.addLong(i);
        }

        for (int i = 0; i < count; i++) {
            lst.addString("Value " + i);
        }
        lst.addString(null);

        MorphiumSingleton.get().store(lst);

        Query q = MorphiumSingleton.get().createQueryFor(ListContainer.class).f("id").eq(lst.getId());
        q.setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY);
        ListContainer lst2 = (ListContainer) q.get();
        assert (lst2.getStringList().get(count) == null);
        assert (lst2.getRefList().get(count) == null);
        assert (lst2.getEmbeddedObjectList().get(count) == null);

    }
}
