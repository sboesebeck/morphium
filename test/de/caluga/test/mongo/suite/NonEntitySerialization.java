package de.caluga.test.mongo.suite;

import com.mongodb.DBObject;
import de.caluga.morphium.MorphiumSingleton;
import org.junit.Test;

import java.io.Serializable;

/**
 * Created by stephan on 18.11.14.
 */
public class NonEntitySerialization extends MongoTest {

    @Test
    public void testNonEntity() throws Exception {
        NonEntity ne = new NonEntity();
        ne.setInteger(42);
        ne.setValue("Thank you for the fish");

        DBObject obj = MorphiumSingleton.get().getMapper().marshall(ne);
        log.info(obj.toString());

        NonEntity ne2 = MorphiumSingleton.get().getMapper().unmarshall(NonEntity.class, obj);
        assert (ne2.getInteger() == 42);
    }


    public static class NonEntity implements Serializable {
        private String value;
        private Integer integer;

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public Integer getInteger() {
            return integer;
        }

        public void setInteger(Integer integer) {
            this.integer = integer;
        }
    }
}
