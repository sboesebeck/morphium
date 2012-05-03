package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 03.05.12
 * Time: 23:31
 * <p/>
 *
 */
public class UpdateTests extends MongoTest{
    @Test
    public void incTest() throws Exception {
        UncachedObject uc=new UncachedObject();
        uc.setCounter(5);
        uc.setValue("unchanged");
        MorphiumSingleton.get().store(uc);
        assert(uc.getMongoId()!=null):"MongoID is NULL???";
        log.info("We got ID: "+uc.getMongoId().toString());
        MorphiumSingleton.get().inc(uc,"counter",2);
        assert(uc.getCounter()==7):"Counter is not 7, it's "+uc.getCounter();

        UncachedObject uc2= (UncachedObject) MorphiumSingleton.get().createQueryFor(UncachedObject.class).f("mongo_id").eq(uc.getMongoId()).get();
        assert(uc2.getMongoId().equals(uc.getMongoId())):"Ids different?";
        assert(uc2.getCounter()==uc.getCounter()):"Counters differ? uc1:"+uc.getCounter()+" uc2:"+uc2.getCounter();
    }

    @Test
    public void decTest() throws Exception {
        UncachedObject uc=new UncachedObject();
        uc.setCounter(5);
        uc.setValue("unchanged");
        MorphiumSingleton.get().store(uc);
        assert(uc.getMongoId()!=null):"MongoID is NULL???";
        log.info("We got ID: "+uc.getMongoId().toString());
        MorphiumSingleton.get().dec(uc,"counter",2);
        assert(uc.getCounter()==3):"Counter is not 3, it's "+uc.getCounter();

        UncachedObject uc2= (UncachedObject) MorphiumSingleton.get().createQueryFor(UncachedObject.class).f("mongo_id").eq(uc.getMongoId()).get();
        assert(uc2.getMongoId().equals(uc.getMongoId())):"Ids different?";
        assert(uc2.getCounter()==uc.getCounter()):"Counters differ? uc1:"+uc.getCounter()+" uc2:"+uc2.getCounter();
    }


    @Test
    public void setTest() throws Exception {
        UncachedObject uc=new UncachedObject();
        uc.setCounter(5);
        uc.setValue("unchanged");
        MorphiumSingleton.get().store(uc);
        assert(uc.getMongoId()!=null):"MongoID is NULL???";
        log.info("We got ID: "+uc.getMongoId().toString());
        MorphiumSingleton.get().set(uc,"counter",2);
        assert(uc.getCounter()==2):"Counter is not 2, it's "+uc.getCounter();

        UncachedObject uc2= (UncachedObject) MorphiumSingleton.get().createQueryFor(UncachedObject.class).f("mongo_id").eq(uc.getMongoId()).get();
        assert(uc2.getMongoId().equals(uc.getMongoId())):"Ids different?";
        assert(uc2.getCounter()==uc.getCounter()):"Counters differ? uc1:"+uc.getCounter()+" uc2:"+uc2.getCounter();
    }
}
