package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Index;
import de.caluga.morphium.query.Query;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 19.03.13
 * Time: 21:50
 * <p/>
 * TODO: Add documentation here
 */
public class TextIndexTest extends MongoTest {

    @Test
    public void textIndexTest() throws Exception {
        MorphiumSingleton.get().dropCollection(Person.class);
        try {
            MorphiumSingleton.get().ensureIndicesFor(Person.class);

        } catch (Exception e) {
            log.info("Text search not enabled - test skipped");
            return;
        }
        MorphiumSingleton.get().store(new Person("hugo", "Strange", "Mr.", 65));
        MorphiumSingleton.get().store(new Person("bruce", "Wayne", "Mr.", 38));
        MorphiumSingleton.get().store(new Person("Kitty", "Galore", "Mrs.", 25));
        MorphiumSingleton.get().store(new Person("Peter", "Parker", "Mr.", 22));

        waitForWrites();
        Query<Person> p = MorphiumSingleton.get().createQueryFor(Person.class);
        List<Person> lst = p.textSearch("hugo", "bruce");
        assert (lst.size() == 2);
    }


    @Entity
    @Index(value = {"vorname:text,nachname:text,anrede:text", "age:1"}, options = {"name:myIdx"})
    public static class Person {
        @Id
        private ObjectId id;
        private String vorname;
        private String nachname;
        private String anrede;
        private int age;

        public Person() {
        }

        public Person(String vorname, String nachname, String anrede, int age) {
            this.vorname = vorname;
            this.nachname = nachname;
            this.anrede = anrede;
            this.age = age;
        }

        public ObjectId getId() {
            return id;
        }

        public void setId(ObjectId id) {
            this.id = id;
        }

        public String getVorname() {
            return vorname;
        }

        public void setVorname(String vorname) {
            this.vorname = vorname;
        }

        public String getNachname() {
            return nachname;
        }

        public void setNachname(String nachname) {
            this.nachname = nachname;
        }

        public String getAnrede() {
            return anrede;
        }

        public void setAnrede(String anrede) {
            this.anrede = anrede;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

    }

}
