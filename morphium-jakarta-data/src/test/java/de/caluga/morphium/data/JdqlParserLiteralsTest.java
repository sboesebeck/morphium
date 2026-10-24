package de.caluga.morphium.data;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * MOR-082: what the Jakarta Data TCK asks of the JDQL parser that the rest of the tests did not: AND binds stronger than OR, a keyword or an operator inside a string literal is text, NOT
 * stands before BETWEEN, IN and LIKE as well, and an IN list may hold literals.
 */
class JdqlParserLiteralsTest {

    @Test
    @DisplayName("AND binds stronger than OR: a OR b AND c is a OR (b AND c)")
    void andBeforeOr() {
        JdqlQuery q = JdqlParser.parse("WHERE a = :a OR b = :b AND c = :c");

        assertThat(q.combinator()).isEqualTo(JdqlQuery.Combinator.OR);
        assertThat(q.conditions()).hasSize(2);
        assertThat(q.conditions().get(0).fieldName()).isEqualTo("a");
        JdqlQuery.JdqlCondition and = q.conditions().get(1);
        assertThat(and.isGroup()).isTrue();
        assertThat(and.groupCombinator()).isEqualTo(JdqlQuery.Combinator.AND);
        assertThat(and.groupConditions()).extracting(JdqlQuery.JdqlCondition::fieldName).containsExactly("b", "c");
    }

    @Test
    @DisplayName("a AND b OR c is (a AND b) OR c, also inside parentheses")
    void andGroupBeforeOr() {
        JdqlQuery q = JdqlParser.parse("WHERE a = :a AND b = :b OR c = :c");

        assertThat(q.combinator()).isEqualTo(JdqlQuery.Combinator.OR);
        assertThat(q.conditions().get(0).isGroup()).isTrue();
        assertThat(q.conditions().get(0).groupConditions()).extracting(JdqlQuery.JdqlCondition::fieldName).containsExactly("a", "b");
        assertThat(q.conditions().get(1).fieldName()).isEqualTo("c");

        JdqlQuery nested = JdqlParser.parse("WHERE x = :x AND (a = :a AND b = :b OR c = :c)");
        JdqlQuery.JdqlCondition group = nested.conditions().get(1);
        assertThat(group.groupCombinator()).isEqualTo(JdqlQuery.Combinator.OR);
        assertThat(group.groupConditions().get(0).isGroup()).isTrue();
    }

    @Test
    @DisplayName("BETWEEN ... AND inside an OR chain keeps its AND")
    void betweenInsideOr() {
        JdqlQuery q = JdqlParser.parse("WHERE a BETWEEN 1 AND 5 OR b = :b AND c BETWEEN :x AND :y");

        assertThat(q.combinator()).isEqualTo(JdqlQuery.Combinator.OR);
        assertThat(q.conditions().get(0).operator()).isEqualTo(JdqlQuery.Operator.BETWEEN);
        assertThat(q.conditions().get(1).groupConditions().get(1).valueRef2()).isEqualTo(":y");
    }

    @Test
    @DisplayName("ORDER BY, WHERE, AND, IN and an operator inside a string literal are text")
    void keywordsInsideLiterals() {
        JdqlQuery q = JdqlParser.parse("WHERE h <> ' ORDER BY isn''t a keyword when inside a literal' AND g IN ('4a', '4b', :p) ORDER BY id");

        assertThat(q.conditions()).hasSize(2);
        assertThat(q.conditions().get(0).fieldName()).isEqualTo("h");
        assertThat(q.conditions().get(0).operator()).isEqualTo(JdqlQuery.Operator.NE);
        assertThat(q.conditions().get(0).valueRef()).isEqualTo("' ORDER BY isn''t a keyword when inside a literal'");
        assertThat(q.conditions().get(1).operator()).isEqualTo(JdqlQuery.Operator.IN);
        assertThat(q.conditions().get(1).valueRef()).isEqualTo("('4a', '4b', :p)");
        assertThat(q.orderBy()).extracting(JdqlQuery.OrderSpec::field).containsExactly("id");

        JdqlQuery ops = JdqlParser.parse("WHERE name = 'a=b' AND other LIKE 'x IN y' AND third = 'p BETWEEN q AND r'");
        assertThat(ops.conditions()).extracting(JdqlQuery.JdqlCondition::fieldName).containsExactly("name", "other", "third");
        assertThat(ops.conditions()).extracting(JdqlQuery.JdqlCondition::operator).containsExactly(JdqlQuery.Operator.EQ, JdqlQuery.Operator.LIKE, JdqlQuery.Operator.EQ);

        JdqlQuery select = JdqlParser.parse("SELECT a FROM T WHERE b = ' FROM x WHERE y GROUP BY z' ORDER BY a");
        assertThat(select.selectFields()).containsExactly("a");
        assertThat(select.conditions()).hasSize(1);
        assertThat(select.groupByFields()).isNull();
    }

    @Test
    @DisplayName("lower case keywords, NOT LIKE, NOT IN with literals and NOT BETWEEN")
    void notBeforeTheOperator() {
        JdqlQuery q = JdqlParser.parse("select thisCharacter where hexadecimal like '4_' and hexadecimal not like '%0' and thisCharacter not in ('E', 'G') and id not between 72 and 78 order by id asc");

        assertThat(q.selectFields()).containsExactly("thisCharacter");
        assertThat(q.conditions()).hasSize(4);

        assertThat(q.conditions().get(0).fieldName()).isEqualTo("hexadecimal");
        assertThat(q.conditions().get(0).operator()).isEqualTo(JdqlQuery.Operator.LIKE);
        assertThat(q.conditions().get(0).negated()).isFalse();

        assertThat(q.conditions().get(1).fieldName()).isEqualTo("hexadecimal");
        assertThat(q.conditions().get(1).operator()).isEqualTo(JdqlQuery.Operator.LIKE);
        assertThat(q.conditions().get(1).negated()).isTrue();

        assertThat(q.conditions().get(2).fieldName()).isEqualTo("thisCharacter");
        assertThat(q.conditions().get(2).operator()).isEqualTo(JdqlQuery.Operator.NOT_IN);
        assertThat(q.conditions().get(2).valueRef()).isEqualTo("('E', 'G')");

        assertThat(q.conditions().get(3).fieldName()).isEqualTo("id");
        assertThat(q.conditions().get(3).operator()).isEqualTo(JdqlQuery.Operator.BETWEEN);
        assertThat(q.conditions().get(3).negated()).isTrue();
        assertThat(q.conditions().get(3).valueRef()).isEqualTo("72");
        assertThat(q.conditions().get(3).valueRef2()).isEqualTo("78");

        assertThat(q.orderBy()).hasSize(1);
        assertThat(q.orderBy().get(0).ascending()).isTrue();
    }

    @Test
    @DisplayName("a qualified enum constant is an operand, and a keyword is not an identifier prefix: orderId, android")
    void enumConstantAndIdentifiers() {
        JdqlQuery q = JdqlParser.parse("WHERE isOdd = false AND numType = ee.example.NumberType.PRIME AND orderId = :o AND android = :a");

        assertThat(q.conditions()).hasSize(4);
        assertThat(q.conditions().get(1).valueRef()).isEqualTo("ee.example.NumberType.PRIME");
        assertThat(q.conditions().get(2).fieldName()).isEqualTo("orderId");
        assertThat(q.conditions().get(3).fieldName()).isEqualTo("android");
    }

    @Test
    @DisplayName("NOT in front of a condition and NOT in front of a group still work next to the new forms")
    void notPrefix() {
        JdqlQuery q = JdqlParser.parse("WHERE NOT a LIKE 'x%' AND NOT b IN (1, 2) AND NOT (c = 1 OR d = 2)");

        assertThat(q.conditions()).hasSize(3);
        assertThat(q.conditions().get(0).negated()).isTrue();
        assertThat(q.conditions().get(1).operator()).isEqualTo(JdqlQuery.Operator.IN);
        assertThat(q.conditions().get(1).negated()).isTrue();
        assertThat(q.conditions().get(2).isGroup()).isTrue();
        assertThat(q.conditions().get(2).negated()).isTrue();
    }
}
