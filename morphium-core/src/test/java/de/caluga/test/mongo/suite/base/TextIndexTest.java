package de.caluga.test.mongo.suite.base;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Index;
import de.caluga.morphium.annotations.ReadOnly;
import de.caluga.morphium.driver.MorphiumId;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;

/**
 * User: Stephan Bösebeck
 * Date: 19.03.13
 * Time: 21:50
 * <p>
 * TODO: Add documentation here
 */
@Disabled
@Tag("admin")
public class TextIndexTest extends MultiDriverTestBase {
    //
    //    @Test
    //    public void textIndexTest() {
    //        morphium.dropCollection(Person.class);
    //        try {
    //            morphium.ensureIndicesFor(Person.class);
    //
    //        } catch (Exception e) {
    //            log.info("Text search not enabled - test skipped");
    //            return;
    //        }
    //        createData();
    //        TestUtils.waitForWrites(morphium,log);
    //        Query<Person> p = morphium.createQueryFor(Person.class);
    //        List<Person> lst = p.text(Query.TextSearchLanguages.english, "hugo", "bruce").asList();
    //        assert (lst.size() == 2) : "size is " + lst.size();
    //        p = morphium.createQueryFor(Person.class);
    //        lst = p.text(Query.TextSearchLanguages.english, false, false, "Hugo", "Bruce").asList();
    //        assert (lst.size() == 2) : "size is " + lst.size();
    //    }
    //
    //
    //    @Test
    //    public void textOperatorTest() {
    //        morphium.dropCollection(Person.class);
    //        try {
    //            morphium.ensureIndicesFor(Person.class);
    //
    //        } catch (Exception e) {
    //            log.info("Text search not enabled - test skipped");
    //            return;
    //        }
    //        createData();
    //
    //        TestUtils.waitForWrites(morphium,log);
    //
    //        Query<Person> p = morphium.createQueryFor(Person.class);
    //        p = p.text("hugo", "Bruce");
    //        p = p.f("anrede").eq("Mr.");
    //        log.info("Query: " + p.toQueryObject().toString());
    //        List<Person> lst = p.asList();
    //        assert (lst.size() == 2);
    //
    //        p = p.q();
    //        p.text("batman");
    //        log.info("List: " + p.countAll());
    //        for (Person pers : p.asIterable()) {
    //            log.info(" Name: " + pers.getNachname());
    //        }
    //        p = p.q();
    //
    //        p = p.text("score", Query.TextSearchLanguages.english, "batman");
    //        log.info("Query: " + p.toQueryObject().toString());
    //        for (Person pers : p.asIterable()) {
    //            log.info(" Name: " + pers.getNachname() + " score: " + pers.getScore());
    //            assert (pers.getScore() > 0);
    //        }
    //
    //        //TODO - solve sort of text indexes
    //        //        HashMap<String, Integer> sort = new HashMap<>();
    //        //        sort.put("score", morphium.getMap("$meta", "textScore"));
    //
    //        //        p.sort(sort);
    //        //        float last = 999999;
    //        //        for (Person pers : p.asIterable()) {
    //        //            log.info(" Name: " + pers.getNachname() + " score: " + pers.getScore());
    //        //            assert (pers.getScore() > 0);
    //        //            assert (last >= pers.getScore());
    //        //            last = pers.getScore();
    //        //        }
    //
    //    }

    // Disabled along with tests above - uses morphium field which no longer exists
    // private void createData() {
    //     morphium.store(new Person("hugo", "Strange", "Mr.", 65,
    //         "Professor Hugo Strange is a fictional character..."));
    //     morphium.store(new Person("Kitty", "Galore", "Mrs.", 25, "..."));
    //     // ... rest of test data creation omitted
    // }


    @Entity
    @Index(value = {"vorname:text,nachname:text,anrede:text,description:text", "age:1"}, options = {"name:myIdx"})
    public static class Person {
        @Id
        private MorphiumId id;
        private String vorname;
        private String nachname;
        private String anrede;
        private int age;
        @ReadOnly
        private float score;

        private String description;

        public Person() {
        }

        public Person(String vorname, String nachname, String anrede, int age, String description) {
            this.vorname = vorname;
            this.nachname = nachname;
            this.anrede = anrede;
            this.age = age;
            this.description = description;
        }

        public float getScore() {
            return score;
        }

        public void setScore(float score) {
            this.score = score;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public MorphiumId getId() {
            return id;
        }

        public void setId(MorphiumId id) {
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
