package de.caluga.morphium.data;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** MOR-024: {@code ?1} style parameters of a JDQL query are rewritten to named ones and bound to the n-th query parameter of the method. */
class JdqlOrdinalParametersTest {

    @Test
    @DisplayName("a query without ordinals is left alone")
    void namedOnly() {
        assertThat(JdqlOrdinalParameters.has("WHERE price >= :min")).isFalse();
        assertThat(JdqlOrdinalParameters.has("")).isFalse();
        assertThat(JdqlOrdinalParameters.has(null)).isFalse();
    }

    @Test
    @DisplayName("ordinals are found and rewritten to the names the bridge binds")
    void rewritten() {
        String jdql = "WHERE price >= ?1 AND category = ?2 OR price < ?1";
        assertThat(JdqlOrdinalParameters.has(jdql)).isTrue();
        assertThat(JdqlOrdinalParameters.rewrite(jdql)).isEqualTo("WHERE price >= :_p1 AND category = :_p2 OR price < :_p1");
    }

    @Test
    @DisplayName("a question mark inside a string literal is not a parameter")
    void literalsAreSkipped() {
        String jdql = "WHERE name = 'what?1' AND code = ?1";
        assertThat(JdqlOrdinalParameters.rewrite(jdql)).isEqualTo("WHERE name = 'what?1' AND code = :_p1");
        assertThat(JdqlOrdinalParameters.has("WHERE name = 'what?1'")).isFalse();
        assertThat(JdqlOrdinalParameters.has("WHERE name = 'it''s ?1' AND code = ?2")).isTrue();
    }

    @Test
    @DisplayName("ordinal n is bound to the n-th method parameter that is not a special one")
    void bindsToQueryParameters() {
        // method (Sort s, String a, Limit l, int b): indexes 0 and 2 are special
        String spec = JdqlOrdinalParameters.paramMapSpec("WHERE x = ?1 AND y = ?2", 4, 0, -1, -1, 2);
        assertThat(spec).isEqualTo("_p1:1,_p2:3");
    }

    @Test
    @DisplayName("an ordinal without a method parameter is rejected with the query in the message")
    void outOfRange() {
        assertThatThrownBy(() -> JdqlOrdinalParameters.paramMapSpec("WHERE x = ?3", 2, -1, -1, -1, -1))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("?3").hasMessageContaining("WHERE x = ?3");
        assertThatThrownBy(() -> JdqlOrdinalParameters.paramMapSpec("WHERE x = ?0", 2, -1, -1, -1, -1))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("?0");
    }

    @Test
    @DisplayName("named and ordinal parameters in one query are rejected")
    void mixed() {
        assertThat(JdqlOrdinalParameters.mixesStyles("WHERE x = :a AND y = ?1")).isTrue();
        assertThat(JdqlOrdinalParameters.mixesStyles("WHERE x = ?1 AND y = ?2")).isFalse();
        assertThat(JdqlOrdinalParameters.mixesStyles("WHERE x = :a AND y = :b")).isFalse();
        assertThat(JdqlOrdinalParameters.mixesStyles("WHERE t = '10:30' AND y = ?1")).isFalse();
    }

    @Test
    @DisplayName("the highest ordinal is reported for the build-time check")
    void highest() {
        assertThat(JdqlOrdinalParameters.highest("WHERE a = ?2 AND b = ?1")).isEqualTo(2);
        assertThat(JdqlOrdinalParameters.highest("WHERE a = :x")).isZero();
    }
}
