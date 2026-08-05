package de.caluga.morphium.data;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link JdqlParser}, covering empty/null input handling,
 * parenthesized group support, and parenthesis-aware top-level splitting.
 */
class JdqlParserTest {

    @Nested
    @DisplayName("Empty and null input — find all contract")
    class EmptyInputTests {

        @Test
        @DisplayName("parse(\"\") returns empty conditions (find all)")
        void emptyStringReturnsEmptyConditions() {
            JdqlQuery result = JdqlParser.parse("");
            assertThat(result.conditions()).isEmpty();
            assertThat(result.orderBy()).isEmpty();
            assertThat(result.combinator()).isEqualTo(JdqlQuery.Combinator.AND);
        }

        @Test
        @DisplayName("parse(null) returns empty conditions (find all)")
        void nullReturnsEmptyConditions() {
            JdqlQuery result = JdqlParser.parse(null);
            assertThat(result.conditions()).isEmpty();
            assertThat(result.orderBy()).isEmpty();
            assertThat(result.combinator()).isEqualTo(JdqlQuery.Combinator.AND);
        }

        @Test
        @DisplayName("parse(\"   \") (blank) returns empty conditions (find all)")
        void blankStringReturnsEmptyConditions() {
            JdqlQuery result = JdqlParser.parse("   ");
            assertThat(result.conditions()).isEmpty();
            assertThat(result.orderBy()).isEmpty();
            assertThat(result.combinator()).isEqualTo(JdqlQuery.Combinator.AND);
        }
    }

    @Nested
    @DisplayName("Parenthesized group parsing")
    class ParenthesizedGroupTests {

        @Test
        @DisplayName("AND with parenthesized OR group: a = :a AND (b IS NULL OR b = '')")
        void andWithParenthesizedOrGroup() {
            String jdql = "WHERE campaignNumber = :campaignNumber AND (otaUpdateError IS NULL OR otaUpdateError = '')";
            JdqlQuery result = JdqlParser.parse(jdql);

            assertThat(result.combinator()).isEqualTo(JdqlQuery.Combinator.AND);
            assertThat(result.conditions()).hasSize(2);

            // First condition: simple campaignNumber = :campaignNumber
            JdqlQuery.JdqlCondition first = result.conditions().get(0);
            assertThat(first.isGroup()).isFalse();
            assertThat(first.fieldName()).isEqualTo("campaignNumber");
            assertThat(first.operator()).isEqualTo(JdqlQuery.Operator.EQ);
            assertThat(first.valueRef()).isEqualTo(":campaignNumber");

            // Second condition: group (otaUpdateError IS NULL OR otaUpdateError = '')
            JdqlQuery.JdqlCondition second = result.conditions().get(1);
            assertThat(second.isGroup()).isTrue();
            assertThat(second.groupCombinator()).isEqualTo(JdqlQuery.Combinator.OR);
            assertThat(second.groupConditions()).hasSize(2);

            JdqlQuery.JdqlCondition groupCond1 = second.groupConditions().get(0);
            assertThat(groupCond1.fieldName()).isEqualTo("otaUpdateError");
            assertThat(groupCond1.operator()).isEqualTo(JdqlQuery.Operator.IS_NULL);

            JdqlQuery.JdqlCondition groupCond2 = second.groupConditions().get(1);
            assertThat(groupCond2.fieldName()).isEqualTo("otaUpdateError");
            assertThat(groupCond2.operator()).isEqualTo(JdqlQuery.Operator.EQ);
            assertThat(groupCond2.valueRef()).isEqualTo("''");
        }

        @Test
        @DisplayName("Multiple AND conditions with parenthesized OR: a = :a AND b = :b AND (c IS NULL OR c = '')")
        void multipleAndWithParenthesizedOr() {
            String jdql = "WHERE a = :a AND b = :b AND (c IS NULL OR c = '')";
            JdqlQuery result = JdqlParser.parse(jdql);

            assertThat(result.combinator()).isEqualTo(JdqlQuery.Combinator.AND);
            assertThat(result.conditions()).hasSize(3);

            assertThat(result.conditions().get(0).isGroup()).isFalse();
            assertThat(result.conditions().get(0).fieldName()).isEqualTo("a");

            assertThat(result.conditions().get(1).isGroup()).isFalse();
            assertThat(result.conditions().get(1).fieldName()).isEqualTo("b");

            JdqlQuery.JdqlCondition group = result.conditions().get(2);
            assertThat(group.isGroup()).isTrue();
            assertThat(group.groupCombinator()).isEqualTo(JdqlQuery.Combinator.OR);
            assertThat(group.groupConditions()).hasSize(2);
        }

        @Test
        @DisplayName("Parenthesized AND group inside OR: (a = :a AND b = :b) OR c = :c")
        void parenthesizedAndGroupInsideOr() {
            String jdql = "WHERE (a = :a AND b = :b) OR c = :c";
            JdqlQuery result = JdqlParser.parse(jdql);

            assertThat(result.combinator()).isEqualTo(JdqlQuery.Combinator.OR);
            assertThat(result.conditions()).hasSize(2);

            JdqlQuery.JdqlCondition group = result.conditions().get(0);
            assertThat(group.isGroup()).isTrue();
            assertThat(group.groupCombinator()).isEqualTo(JdqlQuery.Combinator.AND);
            assertThat(group.groupConditions()).hasSize(2);

            assertThat(result.conditions().get(1).isGroup()).isFalse();
            assertThat(result.conditions().get(1).fieldName()).isEqualTo("c");
        }

        @Test
        @DisplayName("Single condition in parentheses is unwrapped: (a = :a)")
        void singleConditionInParentheses() {
            String jdql = "WHERE (a = :a)";
            JdqlQuery result = JdqlParser.parse(jdql);

            assertThat(result.conditions()).hasSize(1);
            JdqlQuery.JdqlCondition cond = result.conditions().get(0);
            assertThat(cond.isGroup()).isFalse();
            assertThat(cond.fieldName()).isEqualTo("a");
            assertThat(cond.operator()).isEqualTo(JdqlQuery.Operator.EQ);
        }

        @Test
        @DisplayName("Nested groups: (a = :a OR (b = :b AND c = :c))")
        void nestedGroups() {
            String jdql = "WHERE x = :x AND (a = :a OR (b = :b AND c = :c))";
            JdqlQuery result = JdqlParser.parse(jdql);

            assertThat(result.combinator()).isEqualTo(JdqlQuery.Combinator.AND);
            assertThat(result.conditions()).hasSize(2);

            assertThat(result.conditions().get(0).fieldName()).isEqualTo("x");

            JdqlQuery.JdqlCondition outerGroup = result.conditions().get(1);
            assertThat(outerGroup.isGroup()).isTrue();
            assertThat(outerGroup.groupCombinator()).isEqualTo(JdqlQuery.Combinator.OR);
            assertThat(outerGroup.groupConditions()).hasSize(2);

            // First inner: a = :a
            assertThat(outerGroup.groupConditions().get(0).fieldName()).isEqualTo("a");

            // Second inner: (b = :b AND c = :c)
            JdqlQuery.JdqlCondition innerGroup = outerGroup.groupConditions().get(1);
            assertThat(innerGroup.isGroup()).isTrue();
            assertThat(innerGroup.groupCombinator()).isEqualTo(JdqlQuery.Combinator.AND);
            assertThat(innerGroup.groupConditions()).hasSize(2);
        }
    }

    @Nested
    @DisplayName("Top-level OR (no parentheses) — existing behavior preserved")
    class TopLevelOrTests {

        @Test
        @DisplayName("Simple OR: a = :a OR b = :b")
        void simpleOr() {
            String jdql = "WHERE a = :a OR b = :b";
            JdqlQuery result = JdqlParser.parse(jdql);

            assertThat(result.combinator()).isEqualTo(JdqlQuery.Combinator.OR);
            assertThat(result.conditions()).hasSize(2);
            assertThat(result.conditions().get(0).fieldName()).isEqualTo("a");
            assertThat(result.conditions().get(1).fieldName()).isEqualTo("b");
        }

        @Test
        @DisplayName("Simple AND: a = :a AND b = :b")
        void simpleAnd() {
            String jdql = "WHERE a = :a AND b = :b";
            JdqlQuery result = JdqlParser.parse(jdql);

            assertThat(result.combinator()).isEqualTo(JdqlQuery.Combinator.AND);
            assertThat(result.conditions()).hasSize(2);
        }
    }

    @Nested
    @DisplayName("BETWEEN...AND inside parenthesized groups")
    class BetweenTests {

        @Test
        @DisplayName("BETWEEN...AND is not split: a BETWEEN :min AND :max AND b = :b")
        void betweenNotSplit() {
            String jdql = "WHERE a BETWEEN :min AND :max AND b = :b";
            JdqlQuery result = JdqlParser.parse(jdql);

            assertThat(result.combinator()).isEqualTo(JdqlQuery.Combinator.AND);
            assertThat(result.conditions()).hasSize(2);

            JdqlQuery.JdqlCondition between = result.conditions().get(0);
            assertThat(between.operator()).isEqualTo(JdqlQuery.Operator.BETWEEN);
            assertThat(between.valueRef()).isEqualTo(":min");
            assertThat(between.valueRef2()).isEqualTo(":max");
        }
    }

    @Nested
    @DisplayName("containsTopLevelOr must not see OR inside parentheses")
    class ContainsTopLevelOrTests {

        @Test
        @DisplayName("OR only inside parentheses → no top-level OR → combinator is AND")
        void orInsideParenthesesIsNotTopLevel() {
            String jdql = "WHERE a = :a AND (b = :b OR c = :c)";
            JdqlQuery result = JdqlParser.parse(jdql);

            // The top-level combinator must be AND, not OR
            assertThat(result.combinator()).isEqualTo(JdqlQuery.Combinator.AND);
            assertThat(result.conditions()).hasSize(2);
        }

        @Test
        @DisplayName("OR at top level → combinator is OR")
        void orAtTopLevel() {
            String jdql = "WHERE a = :a OR b = :b";
            JdqlQuery result = JdqlParser.parse(jdql);

            assertThat(result.combinator()).isEqualTo(JdqlQuery.Combinator.OR);
        }
    }

    @Nested
    @DisplayName("Real-world OTA Authority queries")
    class RealWorldTests {

        @Test
        @DisplayName("UpdateMorphiumRepository: campaignNumber = :cn AND (otaUpdateError IS NULL OR otaUpdateError = '')")
        void updateRepositoryQuery() {
            String jdql = "WHERE campaignNumber = :campaignNumber AND (otaUpdateError IS NULL OR otaUpdateError = '')";
            JdqlQuery result = JdqlParser.parse(jdql);

            // Must be AND at top level with 2 conditions
            assertThat(result.combinator()).isEqualTo(JdqlQuery.Combinator.AND);
            assertThat(result.conditions()).hasSize(2);

            // First: campaignNumber = :campaignNumber
            JdqlQuery.JdqlCondition campaignCond = result.conditions().get(0);
            assertThat(campaignCond.isGroup()).isFalse();
            assertThat(campaignCond.fieldName()).isEqualTo("campaignNumber");

            // Second: OR group
            JdqlQuery.JdqlCondition orGroup = result.conditions().get(1);
            assertThat(orGroup.isGroup()).isTrue();
            assertThat(orGroup.groupCombinator()).isEqualTo(JdqlQuery.Combinator.OR);
            assertThat(orGroup.groupConditions()).hasSize(2);

            // Group condition 1: otaUpdateError IS NULL
            assertThat(orGroup.groupConditions().get(0).fieldName()).isEqualTo("otaUpdateError");
            assertThat(orGroup.groupConditions().get(0).operator()).isEqualTo(JdqlQuery.Operator.IS_NULL);

            // Group condition 2: otaUpdateError = ''
            assertThat(orGroup.groupConditions().get(1).fieldName()).isEqualTo("otaUpdateError");
            assertThat(orGroup.groupConditions().get(1).operator()).isEqualTo(JdqlQuery.Operator.EQ);
        }
    }

    @Nested
    @DisplayName("NOT BETWEEN support")
    class NotBetweenTests {

        @Test
        @DisplayName("NOT field BETWEEN :min AND :max")
        void notBetween() {
            String jdql = "WHERE NOT price BETWEEN :min AND :max";
            JdqlQuery result = JdqlParser.parse(jdql);

            assertThat(result.conditions()).hasSize(1);
            JdqlQuery.JdqlCondition cond = result.conditions().get(0);
            assertThat(cond.operator()).isEqualTo(JdqlQuery.Operator.BETWEEN);
            assertThat(cond.fieldName()).isEqualTo("price");
            assertThat(cond.valueRef()).isEqualTo(":min");
            assertThat(cond.valueRef2()).isEqualTo(":max");
            assertThat(cond.negated()).isTrue();
        }

        @Test
        @DisplayName("NOT BETWEEN combined with other AND conditions")
        void notBetweenWithAnd() {
            String jdql = "WHERE status = :status AND NOT price BETWEEN :min AND :max";
            JdqlQuery result = JdqlParser.parse(jdql);

            assertThat(result.combinator()).isEqualTo(JdqlQuery.Combinator.AND);
            assertThat(result.conditions()).hasSize(2);

            JdqlQuery.JdqlCondition first = result.conditions().get(0);
            assertThat(first.fieldName()).isEqualTo("status");
            assertThat(first.operator()).isEqualTo(JdqlQuery.Operator.EQ);

            JdqlQuery.JdqlCondition second = result.conditions().get(1);
            assertThat(second.operator()).isEqualTo(JdqlQuery.Operator.BETWEEN);
            assertThat(second.negated()).isTrue();
        }
    }

    @Nested
    @DisplayName("NOT (...) group negation")
    class NotGroupTests {

        @Test
        @DisplayName("NOT (a = :a OR b = :b) creates negated group")
        void notOrGroup() {
            String jdql = "WHERE NOT (a = :a OR b = :b)";
            JdqlQuery result = JdqlParser.parse(jdql);

            assertThat(result.conditions()).hasSize(1);
            JdqlQuery.JdqlCondition cond = result.conditions().get(0);
            assertThat(cond.isGroup()).isTrue();
            assertThat(cond.negated()).isTrue();
            assertThat(cond.groupCombinator()).isEqualTo(JdqlQuery.Combinator.OR);
            assertThat(cond.groupConditions()).hasSize(2);
            assertThat(cond.groupConditions().get(0).fieldName()).isEqualTo("a");
            assertThat(cond.groupConditions().get(1).fieldName()).isEqualTo("b");
        }

        @Test
        @DisplayName("NOT (a = :a AND b = :b) creates negated group")
        void notAndGroup() {
            String jdql = "WHERE NOT (a = :a AND b = :b)";
            JdqlQuery result = JdqlParser.parse(jdql);

            assertThat(result.conditions()).hasSize(1);
            JdqlQuery.JdqlCondition cond = result.conditions().get(0);
            assertThat(cond.isGroup()).isTrue();
            assertThat(cond.negated()).isTrue();
            assertThat(cond.groupCombinator()).isEqualTo(JdqlQuery.Combinator.AND);
            assertThat(cond.groupConditions()).hasSize(2);
        }

        @Test
        @DisplayName("x = :x AND NOT (a = :a OR b = :b)")
        void notGroupCombinedWithAnd() {
            String jdql = "WHERE x = :x AND NOT (a = :a OR b = :b)";
            JdqlQuery result = JdqlParser.parse(jdql);

            assertThat(result.combinator()).isEqualTo(JdqlQuery.Combinator.AND);
            assertThat(result.conditions()).hasSize(2);

            assertThat(result.conditions().get(0).isGroup()).isFalse();
            assertThat(result.conditions().get(0).fieldName()).isEqualTo("x");

            JdqlQuery.JdqlCondition group = result.conditions().get(1);
            assertThat(group.isGroup()).isTrue();
            assertThat(group.negated()).isTrue();
            assertThat(group.groupCombinator()).isEqualTo(JdqlQuery.Combinator.OR);
        }

        @Test
        @DisplayName("NOT (single condition) just negates it")
        void notSingleConditionInParens() {
            String jdql = "WHERE NOT (a = :a)";
            JdqlQuery result = JdqlParser.parse(jdql);

            assertThat(result.conditions()).hasSize(1);
            JdqlQuery.JdqlCondition cond = result.conditions().get(0);
            assertThat(cond.isGroup()).isFalse();
            assertThat(cond.fieldName()).isEqualTo("a");
            assertThat(cond.operator()).isEqualTo(JdqlQuery.Operator.EQ);
            assertThat(cond.negated()).isTrue();
        }

        @Test
        @DisplayName("NOT (NOT (...)) double negation cancels out")
        void doubleNegationCancels() {
            String jdql = "WHERE NOT (NOT (a = :a OR b = :b))";
            JdqlQuery result = JdqlParser.parse(jdql);

            assertThat(result.conditions()).hasSize(1);
            JdqlQuery.JdqlCondition cond = result.conditions().get(0);
            assertThat(cond.isGroup()).isTrue();
            assertThat(cond.negated()).isFalse(); // double NOT cancels
            assertThat(cond.groupCombinator()).isEqualTo(JdqlQuery.Combinator.OR);
            assertThat(cond.groupConditions()).hasSize(2);
        }

        @Test
        @DisplayName("NOT (a AND NOT (b OR c)) — nested negated group preserved")
        void nestedNegatedGroup() {
            String jdql = "WHERE NOT (a = :a AND NOT (b = :b OR c = :c))";
            JdqlQuery result = JdqlParser.parse(jdql);

            assertThat(result.conditions()).hasSize(1);
            JdqlQuery.JdqlCondition outer = result.conditions().get(0);
            assertThat(outer.isGroup()).isTrue();
            assertThat(outer.negated()).isTrue();
            assertThat(outer.groupCombinator()).isEqualTo(JdqlQuery.Combinator.AND);
            assertThat(outer.groupConditions()).hasSize(2);

            // First child: a = :a (simple condition)
            JdqlQuery.JdqlCondition first = outer.groupConditions().get(0);
            assertThat(first.isGroup()).isFalse();
            assertThat(first.fieldName()).isEqualTo("a");

            // Second child: NOT (b = :b OR c = :c) — inner negated group
            JdqlQuery.JdqlCondition inner = outer.groupConditions().get(1);
            assertThat(inner.isGroup()).isTrue();
            assertThat(inner.negated()).isTrue();
            assertThat(inner.groupCombinator()).isEqualTo(JdqlQuery.Combinator.OR);
            assertThat(inner.groupConditions()).hasSize(2);
        }
    }

    @Nested
    @DisplayName("Error messages with position info")
    class ErrorMessageTests {

        @Test
        @DisplayName("Parse error includes position and caret")
        void parseErrorIncludesPosition() {
            String jdql = "WHERE name = :name AND status > ";
            try {
                JdqlParser.parse(jdql);
                org.junit.jupiter.api.Assertions.fail("Expected IllegalArgumentException");
            } catch (IllegalArgumentException e) {
                assertThat(e.getMessage()).contains("JDQL parse error at position");
                assertThat(e.getMessage()).contains("^");
                assertThat(e.getMessage()).contains(jdql);
            }
        }

        @Test
        @DisplayName("Duplicate fragment points at correct (second) occurrence")
        void duplicateFragmentPointsAtCorrectOccurrence() {
            // "a = :a" appears twice; the error is in the second occurrence (incomplete)
            String jdql = "WHERE a = :a AND a > ";
            try {
                JdqlParser.parse(jdql);
                org.junit.jupiter.api.Assertions.fail("Expected IllegalArgumentException");
            } catch (IllegalArgumentException e) {
                // Position must point past the first "a = :a AND ", i.e. at the second "a"
                assertThat(e.getMessage()).contains("JDQL parse error at position");
                // "a > " starts at position 17 in "WHERE a = :a AND a > "
                assertThat(e.getMessage()).contains("position 17");
            }
        }

        @Test
        @DisplayName("Parse error for invalid HAVING includes position")
        void havingParseErrorIncludesPosition() {
            String jdql = "SELECT COUNT(this) FROM Entity GROUP BY status HAVING badexpr";
            try {
                JdqlParser.parse(jdql);
                org.junit.jupiter.api.Assertions.fail("Expected IllegalArgumentException");
            } catch (IllegalArgumentException e) {
                assertThat(e.getMessage()).contains("JDQL parse error at position");
                assertThat(e.getMessage()).contains("^");
            }
        }
    }

    @Nested
    @DisplayName("ORDER BY with parenthesized groups")
    class OrderByWithGroupTests {

        @Test
        @DisplayName("Parenthesized group with ORDER BY")
        void groupWithOrderBy() {
            String jdql = "WHERE a = :a AND (b IS NULL OR b = '') ORDER BY a ASC";
            JdqlQuery result = JdqlParser.parse(jdql);

            assertThat(result.combinator()).isEqualTo(JdqlQuery.Combinator.AND);
            assertThat(result.conditions()).hasSize(2);
            assertThat(result.conditions().get(1).isGroup()).isTrue();
            assertThat(result.orderBy()).hasSize(1);
            assertThat(result.orderBy().get(0).field()).isEqualTo("a");
            assertThat(result.orderBy().get(0).ascending()).isTrue();
        }
    }
}
