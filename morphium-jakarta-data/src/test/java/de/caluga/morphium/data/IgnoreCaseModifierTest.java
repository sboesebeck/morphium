package de.caluga.morphium.data;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * MOR-092: {@code IgnoreCase} is a modifier of the property, in front of the operator: {@code findByCodeIgnoreCaseBetween}, {@code ...IgnoreCaseIn}, {@code ...IgnoreCaseGreaterThan},
 * {@code ...IgnoreCaseContains}. It was read as an operator of its own (equals, ignoring case) and only as the last word of the condition.
 */
class IgnoreCaseModifierTest {

    @Entity
    static class Tag {
        @Id
        private String id;
        private String code;
        private String note;

        Tag() {}

        Tag(String id, String code, String note) {
            this.id = id;
            this.code = code;
            this.note = note;
        }
    }

    static class TagRepo extends AbstractMorphiumRepository<Tag, String> {
        TagRepo(Morphium morphium) {
            super(new RepositoryMetadata(Tag.class, String.class, "id"));
            setMorphium(morphium);
        }
    }

    private Morphium morphium;
    private TagRepo repo;

    @BeforeEach
    void setUp() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setDatabase("mor092_" + System.nanoTime());
        cfg.addHostToSeed("localhost");
        cfg.setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg);
        repo = new TagRepo(morphium);
        morphium.store(new Tag("1", "a", "Alpha"));
        morphium.store(new Tag("2", "B", "bravo"));
        morphium.store(new Tag("3", "c", "CHARLIE"));
        morphium.store(new Tag("4", "D", "Delta"));
        morphium.store(new Tag("5", "e", "echo"));
    }

    @AfterEach
    void tearDown() {
        morphium.close();
    }

    @SuppressWarnings("unchecked")
    private List<String> codes(String method, Object... args) {
        Object result = QueryMethodBridge.executeQuery(repo, method, args, false, false, false, false);
        return ((List<Tag>) result).stream().map(t -> t.code).sorted().toList();
    }

    @Test
    @DisplayName("the modifier is read in front of the operator, the plain forms are unchanged")
    void parsing() {
        QueryDescriptor between = MethodNameParser.parse("findByCodeIgnoreCaseBetween", Set.of("code", "note"));

        assertThat(between.conditions()).hasSize(1);
        assertThat(between.conditions().get(0).field()).isEqualTo("code");
        assertThat(between.conditions().get(0).operator()).isEqualTo(QueryDescriptor.Operator.BETWEEN);
        assertThat(between.conditions().get(0).ignoreCase()).isTrue();

        QueryDescriptor plain = MethodNameParser.parse("findByCodeIgnoreCase", Set.of("code", "note"));
        assertThat(plain.conditions().get(0).operator()).isEqualTo(QueryDescriptor.Operator.IGNORE_CASE);
        assertThat(plain.conditions().get(0).ignoreCase()).isFalse();

        QueryDescriptor mixed = MethodNameParser.parse("findByCodeIgnoreCaseBetweenAndNoteNotIn", Set.of("code", "note"));
        assertThat(mixed.conditions()).extracting(QueryDescriptor.Condition::ignoreCase).containsExactly(true, false);
        assertThat(mixed.conditions()).extracting(QueryDescriptor.Condition::operator).containsExactly(QueryDescriptor.Operator.BETWEEN, QueryDescriptor.Operator.NIN);
    }

    @Test
    @DisplayName("ranges compare without case")
    void ranges() {
        assertThat(codes("findByCodeIgnoreCaseBetween", "b", "d")).containsExactly("B", "D", "c");
        assertThat(codes("findByCodeIgnoreCaseGreaterThan", "c")).containsExactly("D", "e");
        assertThat(codes("findByCodeIgnoreCaseGreaterThanEqual", "C")).containsExactly("D", "c", "e");
        assertThat(codes("findByCodeIgnoreCaseLessThan", "c")).containsExactly("B", "a");
        assertThat(codes("findByCodeIgnoreCaseLessThanEqual", "C")).containsExactly("B", "a", "c");
    }

    @Test
    @DisplayName("equality, Not, In and NotIn compare without case")
    void sets() {
        assertThat(codes("findByCodeIgnoreCaseEquals", "b")).containsExactly("B");
        assertThat(codes("findByCodeIgnoreCaseNot", "A")).containsExactly("B", "D", "c", "e");
        assertThat(codes("findByCodeIgnoreCaseIn", Set.of("A", "e"))).containsExactly("a", "e");
        assertThat(codes("findByCodeIgnoreCaseNotIn", Set.of("A", "E"))).containsExactly("B", "D", "c");
    }

    @Test
    @DisplayName("text operators compare without case")
    void text() {
        assertThat(codes("findByNoteIgnoreCaseContains", "ARL")).containsExactly("c");
        assertThat(codes("findByNoteIgnoreCaseStartsWith", "br")).containsExactly("B");
        assertThat(codes("findByNoteIgnoreCaseEndsWith", "TA")).containsExactly("D");
        assertThat(codes("findByNoteIgnoreCaseLike", "%LPH_")).containsExactly("a");
    }

    @Test
    @DisplayName("the modifier applies to its own condition only: a NotIn next to it stays exact")
    void onlyItsOwnCondition() {
        assertThat(codes("findByCodeIgnoreCaseBetweenAndNoteNotIn", "a", "e", Set.of("alpha", "bravo", "Delta"))).containsExactly("a", "c", "e");
    }

    @Test
    @DisplayName("a null argument is a plain null condition, not the text \"null\"")
    void nullArgument() {
        morphium.store(new Tag("6", "null", "textual null"));
        morphium.store(new Tag("7", null, "no code"));

        assertThat(codes("findByCodeIgnoreCaseEquals", (Object) null)).containsOnlyNulls().hasSize(1);    // the document without a code, not the one with the text "null"
    }
}
