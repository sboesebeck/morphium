package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.Query;
import org.junit.Test;

import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 09.05.12
 * Time: 10:46
 * <p/>
 * TODO: Add documentation here
 */
public class UpdateTest extends MongoTest {
    @Test
    public void incTest() throws Exception {
        for (int i = 1; i <= 50; i++) {
            UncachedObject o = new UncachedObject();
            o.setCounter(i);
            o.setValue("Uncached " + i);
            MorphiumSingleton.get().store(o);
        }

        Query<UncachedObject> q=MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        q=q.f("value").eq("Uncached "+5);
        UncachedObject uc=q.get();
        MorphiumSingleton.get().inc(uc,"counter",1);

        assert(uc.getCounter()==6):"Counter is not correct: "+uc.getCounter();

        //inc without object - single update, no upsert
        q=MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        q=q.f("counter").gte(10).f("counter").lte(25).order("-counter");
        MorphiumSingleton.get().inc(UncachedObject.class,q,"counter",100);

        uc=q.get();
        assert(uc.getCounter()==11):"Counter is wrong: "+uc.getCounter();

        //inc without object directly in DB - multiple update
        q=MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        q=q.f("counter").gt(10).f("counter").lte(25);
        MorphiumSingleton.get().inc(UncachedObject.class,q,"counter",100,false,true);

        q=MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        q=q.f("counter").gt(110).f("counter").lte(125);
        List<UncachedObject> lst=q.asList(); //read the data after update
        for (UncachedObject u:lst) {
            assert(u.getCounter()>110 && u.getCounter()<=125 && u.getValue().equals("Uncached "+(u.getCounter()-100))):"Counter wrong: "+u.getCounter();
        }

    }

    @Test
    public void decTest() throws Exception {
        for (int i = 1; i <= 50; i++) {
            UncachedObject o = new UncachedObject();
            o.setCounter(i);
            o.setValue("Uncached " + i);
            MorphiumSingleton.get().store(o);
        }

        Query<UncachedObject> q=MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        q=q.f("value").eq("Uncached "+5);
        UncachedObject uc=q.get();
        MorphiumSingleton.get().dec(uc,"counter",1);

        assert(uc.getCounter()==4):"Counter is not correct: "+uc.getCounter();

        //inc without object - single update, no upsert
        q=MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        q=q.f("counter").gte(40).f("counter").lte(55).order("-counter");
        MorphiumSingleton.get().dec(UncachedObject.class,q,"counter",40);

        uc=q.get();
        assert(uc.getCounter()==41):"Counter is wrong: "+uc.getCounter();

        //inc without object directly in DB - multiple update
        q=MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        q=q.f("counter").gt(40).f("counter").lte(55);
        MorphiumSingleton.get().dec(UncachedObject.class,q,"counter",40,false,true);

        q=MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        q=q.f("counter").gt(0).f("counter").lte(55);
        List<UncachedObject> lst=q.asList(); //read the data after update
        for (UncachedObject u:lst) {
            assert(u.getCounter()>0 && u.getCounter()<=55 ):"Counter wrong: "+u.getCounter();
//            assert(u.getValue().equals("Uncached "+(u.getCounter()-40))):"Value wrong: Counter: "+u.getCounter()+" Value;: "+u.getValue();
        }

    }

    @Test
    public void setTest() throws Exception {
        for (int i = 1; i <= 50; i++) {
            UncachedObject o = new UncachedObject();
            o.setCounter(i);
            o.setValue("Uncached " + i);
            MorphiumSingleton.get().store(o);
        }

        Query<UncachedObject> q=MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        q=q.f("value").eq("unexistent");
        MorphiumSingleton.get().set(UncachedObject.class,q,"counter",999,true,false);
        UncachedObject uc=q.get(); //should now work

        assert(uc!=null):"Not found?!?!?";
        assert(uc.getValue().equals("unexistent")):"Value wrong: "+uc.getValue();
    }
}
