package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 04.05.12
 * Time: 12:44
 * <p/>
 * TODO: Add documentation here
 */
public class EnumTest extends MongoTest {

    @Test
    public void enumTest() throws Exception {
        EnumEntity ent=new EnumEntity();
        ent.setTst(TestEnum.TEST1);
        ent.setValue("ein Test");
        MorphiumSingleton.get().store(ent);

        ent=(EnumEntity)MorphiumSingleton.get().createQueryFor(EnumEntity.class).f("value").eq("ein Test").get();
        assert(ent.getTst()!=null):"Enum is null!";
        assert(ent.getTst().equals(TestEnum.TEST1)):"Enum error!";

        ent=(EnumEntity)MorphiumSingleton.get().createQueryFor(EnumEntity.class).f("tst").eq(TestEnum.TEST1).get();
        assert(ent.getTst()!=null):"Enum is null!";
        assert(ent.getTst().equals(TestEnum.TEST1)):"Enum error!";

    }

}
