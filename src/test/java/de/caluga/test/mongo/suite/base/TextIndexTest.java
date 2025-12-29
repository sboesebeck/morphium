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

    private void createData() {
        morphium.store(new Person("hugo", "Strange", "Mr.", 65,
            "Professor Hugo Strange is a fictional character, a comic book supervillain appearing in books published by DC Comics. He serves as an adversary of Batman. He first appeared in Detective Comics #36 (February 1940),[3] and is one of Batman's first recurring villains, preceding the Joker and Catwoman by several months. He is also one of the first of Batman's villains to deduce Batman's secret identity."));
        morphium.store(new Person("Kitty", "Galore", "Mrs.", 25,
            "Cats & Dogs: The Revenge of Kitty Galore is a 2010 family action comedy film, directed by Brad Peyton. The film stars Chris O'Donnell and Jack McBrayer. The film also stars the voices of James Marsden, Nick Nolte, Christina Applegate, Katt Williams, Bette Midler, and Neil Patrick Harris. The film is a sequel to the 2001 film Cats & Dogs and was released on July 30, 2010. It received extremely negative reviews from film critics, but was a modest commercial success, grossing over $110 million worldwide."));
        morphium.store(new Person("Peter", "Parker", "Mr.", 22,
            "Spider-Man is a fictional character, a comic book superhero that appears in comic books published by Marvel Comics. Created by writer-editor Stan Lee and writer-artist Steve Ditko, he first appeared in Amazing Fantasy #15 (cover-dated Aug. 1962). Lee and Ditko conceived the character as an orphan being raised by his Aunt May and Uncle Ben, and as a teenager, having to deal with the normal struggles of adolescence in addition to those of a costumed crimefighter. Spider-Man's creators gave him super strength and agility, the ability to cling to most surfaces, shoot spider-webs using wrist-mounted devices of his own invention (which he called \"web-shooters\"), and react to danger quickly with his \"spider-sense\", enabling him to combat his foes."));
        morphium.store(new Person("J.J.", "Jameson", "Mr.", 56,
            "J. J. Jameson (Real name Norman A. Porter, Jr.) was a self-proclaimed poet and activist in Chicago, Illinois from the mid-1980s until March 2005. His work was marked by an ironic and humorous cast. In 1993 Jameson was arrested on theft charges in Chicago.\n"
            +
            "He was known for his live performances as a poet and MC at local poetry jams and open mike nights. He also received attention for his September 1999 poetry chapbook, Lady Rutherford's Cauliflower, published by Puddin'head Press, which had been planning to publish a second volume of his work. He was known to be suffering from head tumors in early 2005. In March 2005 Jameson was named Poet of the Month by C. J. Laity of ChicagoPoetry.com chicagopoetry.com. Friends and acquaintances planned to celebrate the twentieth anniversary of his arrival in Chicago with a roast and poetry reading later in 2005."));
        morphium.store(new Person("Tony", "Stark", "Mr.", 42,
            "Iron Man is a fictional character, a superhero that appears in comic books published by Marvel Comics. The character was created by writer-editor Stan Lee, developed by scripter Larry Lieber, and designed by artists Don Heck and Jack Kirby. He made his first appearance in Tales of Suspense #39 (March 1963).\n"
            +
            "An American billionaire playboy, industrialist, and ingenious engineer, Tony Stark suffers a severe chest injury during a kidnapping in which his captors attempt to force him to build a weapon of mass destruction. He instead creates a powered suit of armor to save his life and escape captivity. He later uses the suit and successive versions to protect the world as Iron Man. Through his corporation ― Stark Industries ― Tony has created many military weapons, some of which, along with other technological devices of his making, have been integrated into his suit, helping him fight crime. Initially, Iron Man was a vehicle for Stan Lee to explore Cold War themes, particularly the role of American technology and business in the fight against communism. Subsequent re-imaginings of Iron Man have transitioned from Cold War themes to contemporary concerns, such as corporate crime and terrorism."));
        morphium.store(new Person("Jessica", "Drew", "Mrs.", 28,
            "Spider-Woman (Jessica Drew) is a fictional character, a superheroine in comic books published by Marvel Comics. The character first appeared in Marvel Spotlight #32 (cover-dated February 1977), and 50 issues of an ongoing series titled Spider-Woman followed. At its conclusion she was killed, and though later resurrected in an Avengers story arc, she fell into disuse, supplanted by other characters using the name Spider-Woman."));
        morphium.store(new Person("Betty", "Kane", "Mrs.", 18,
            "Batgirl is the name of several fictional characters appearing in comic books published by DC Comics, depicted as female counterparts to the superhero Batman. Although the character Betty Kane was introduced into publication in 1961 by Bill Finger and Sheldon Moldoff as Bat-Girl, she is replaced by Barbara Gordon in 1967, who later came to be identified as the iconic Batgirl. Her creation came about as a joint project between DC Comics editor Julius Schwartz and the producers of the 1960s Batman television series. In order to boost ratings for the third season of Batman, the producers requested a new female character be introduced into publication that could be adapted into the television series. At Schwartz's direction, Barbara Gordon debuted in Detective Comics #359 titled, \"The Million Dollar Debut of Batgirl!\" (1967) by writer Gardner Fox and artist Carmine Infantino. Depicted as the daughter of Gotham City police commissioner James Gordon, her civilian identity is given a doctorate in library science and she is employed as head of Gotham City Public Library, as well as later being elected to the United States Congress. As Batgirl, the character operates primarily in Gotham City, allying herself with Batman and the original Robin Dick Grayson, as well as other prominent heroes in the DC Universe."));
        morphium.store(new Person("Kathy", "Kane", "Mrs.", 32,
            "Batwoman is a fictional character, a superheroine who appears in comic books published by DC Comics. In all incarnations, Batwoman is a wealthy heiress who—inspired by the notorious superhero Batman—chooses, like him, to put her wealth and resources towards funding a war on crime in her home of Gotham City. The identity of Batwoman is shared by two heroines in mainstream DC publications; both women are named Katherine Kane, with the original Batwoman commonly referred to by her nickname Kathy and the modern incarnation going by the name Kate."));
        morphium.store(new Person("Jack", "Napier", "Mr.", 48,
            "The Joker is a fictional character, a comic book supervillain appearing in DC Comics publications. The character was created by Jerry Robinson, Bill Finger and Bob Kane, and first appeared in Batman #1 (Spring 1940). Credit for the character's creation is disputed; Kane and Robinson claimed responsibility for the Joker's design, but acknowledged Finger's writing contribution. Although the Joker was planned to be killed off during his initial appearance, he was spared by editorial intervention, allowing the character to endure as the archenemy of the superhero Batman."));
        morphium.store(new Person("Lois", "Lane", "Mrs.", 48,
            "Lois Lane is a fictional character appearing in comic books published by DC Comics. Created by writer Jerry Siegel and artist Joe Shuster, she first appeared in Action Comics #1 (June 1938). Lois is an award-winning journalist and the primary love interest of the superhero, Superman (For fifteen years in DC Comics continuity, she was also his wife). Like Superman's alter ego Clark Kent, she is a reporter for the Metropolis newspaper, the Daily Planet."));
        morphium.store(new Person("Clark", "Kent", "Mr.", 37,
            "Clark Kent is an American fictional character created by Jerry Siegel and Joe Shuster. Appearing regularly in stories published by DC Comics, he debuted in Action Comics #1 (June 1938) and serves as the civilian and secret identity of the superhero Superman.\n"
            +
            "Over the decades there has been considerable debate as to which personality the character identifies with most. From his first introduction in 1938 to the mid-1980s, \"Clark Kent\" was seen mostly as a disguise for Superman, enabling him to mix with ordinary people. This was the view in most comics and other media such as movie serials and TV (e.g., in Atom Man vs. Superman starring Kirk Alyn and The Adventures of Superman starring George Reeves) and radio. In 1986, during John Byrne's revamping of the character, the emphasis was on Superman being the manufactured persona of Clark Kent, the side of the character with whom he most identifies. Different takes persist in the present.[when?]"));
        morphium.store(new Person("bruce", "Wayne", "Mr.", 38,
            "Batman is a fictional character, a comic book superhero appearing in comic books published by DC Comics. Batman was created by artist Bob Kane and writer Bill Finger, and first appeared in Detective Comics #27 (May 1939). Originally referred to as \"the Bat-Man\" and still referred to at times as \"the Batman\", the character is additionally known as \"the Caped Crusader\",[5] \"the Dark Knight\",[5] and \"the World's Greatest Detective\",[5] among other titles.\n"
            +
            "Batman is the secret identity of Bruce Wayne, an American billionaire, industrialist, and philanthropist. Having witnessed the murder of his parents as a child, he swore revenge on criminals, an oath tempered with the greater ideal of justice. Wayne trains himself both physically and intellectually and dons a bat-themed costume in order to fight crime.[6] Batman operates in the fictional Gotham City, assisted by various supporting characters including his crime-fighting partner, Robin, his butler Alfred Pennyworth, the police commissioner Jim Gordon, and occasionally the heroine Batgirl. He fights an assortment of villains, often referred to as the \"rogues gallery\", which includes the Joker, the Penguin, the Riddler, Two-Face, Ra's al Ghul, Scarecrow, Poison Ivy, and Catwoman, among many others. Unlike most superheroes, he does not possess any superpowers; he makes use of intellect, detective skills, science and technology, wealth, physical prowess, martial arts skills, an indomitable will, fear, and intimidation in his continuous war on crime."));
    }


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
