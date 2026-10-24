package de.caluga.morphium.data;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * MOR-075: the parser refuses an operand that is neither a parameter nor a literal, with the position, instead of taking it as a string that matches nothing.
 */
class JdqlParserOperandTest {

    @Test
    @DisplayName("WHERE id == :id is a parse error at the condition, not the condition id = \"= :id\"")
    void doubleEqualsIsRefused() {
        assertThatThrownBy(() -> JdqlParser.parse("WHERE id == :id"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("JDQL parse error at position")
                .hasMessageContaining("neither a parameter");
    }

    @Test
    @DisplayName("WHERE price >> 5 is refused")
    void doubleGreaterIsRefused() {
        assertThatThrownBy(() -> JdqlParser.parse("WHERE price >> 5"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("position");
    }

    @Test
    @DisplayName("WHERE id = :id garbage is refused")
    void trailingGarbageIsRefused() {
        assertThatThrownBy(() -> JdqlParser.parse("WHERE id = :id garbage"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("neither a parameter");
    }

    @Test
    @DisplayName("the position points at the failing condition of a longer clause")
    void positionIsTheFailingCondition() {
        assertThatThrownBy(() -> JdqlParser.parse("WHERE a = :a AND b == :b"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("position 17");
    }

    @Test
    @DisplayName("an operand of BETWEEN, IN and LIKE is checked, too")
    void otherOperatorsAreChecked() {
        assertThatThrownBy(() -> JdqlParser.parse("WHERE n BETWEEN :a AND what is this")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JdqlParser.parse("WHERE n IN (:a, nonsense here)")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JdqlParser.parse("WHERE n LIKE %x")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("parameters, ordinal parameters, literals and enum constants are operands")
    void validOperandsStillParse() {
        for (String ok : new String[] {
            "WHERE a = :a", "WHERE a = ?1", "WHERE a = 'x'", "WHERE a = 'it''s'", "WHERE a = 5", "WHERE a = 5.5", "WHERE a = -3",
            "WHERE a = TRUE", "WHERE a = false", "WHERE a = NULL", "WHERE a <> :a AND b >= 2 OR c <= :c",
            "WHERE a IN (:a, :b)", "WHERE a IN ('x', 'y', :z)", "WHERE a BETWEEN :lo AND :hi", "WHERE a BETWEEN 1 AND 5",
            "WHERE a LIKE :p", "WHERE a LIKE 'x%'", "WHERE kind = com.example.Kind.BIG", "WHERE NOT a = :a"}) {
            assertThat(JdqlParser.parse(ok).conditions()).as(ok).isNotEmpty();
        }
    }
}
