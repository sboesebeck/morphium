package de.caluga.morphium.data;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** MOR-021: the operands of a condition are parameters or literals; the parser alone takes {@code == :id} for the operand {@code "= :id"}. */
class JdqlOperandsTest {

    @Test
    @DisplayName("parameters, ordinals and literals of every kind are fine, also in groups and BETWEEN")
    void fine() {
        assertThat(JdqlOperands.check(null)).isNull();
        assertThat(JdqlOperands.check("")).isNull();
        assertThat(JdqlOperands.check("WHERE id = :id AND (price >= 5 OR name = 'it''s' OR flag = TRUE) AND d BETWEEN :a AND 2.5")).isNull();
        assertThat(JdqlOperands.check("id = ?1 AND code IN :codes AND note IS NULL")).isNull();
        assertThat(JdqlOperands.check("WHERE name LIKE 'a%' AND x <> -3")).isNull();
    }

    @Test
    @DisplayName("what the parser takes as an operand without being one is reported with the operand")
    void typos() {
        assertThat(JdqlOperands.check("WHERE id == :id")).contains("= :id").contains("id");
        assertThat(JdqlOperands.check("WHERE price >> 5")).contains("> 5");
        assertThat(JdqlOperands.check("WHERE id = :id garbage")).contains(":id garbage");
        assertThat(JdqlOperands.check("WHERE a = 1 AND (b = :x OR c == :y)")).contains("= :y");
        assertThat(JdqlOperands.check("WHERE name = unquoted")).contains("unquoted");
    }
}
