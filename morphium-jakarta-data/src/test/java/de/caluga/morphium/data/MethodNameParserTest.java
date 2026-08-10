package de.caluga.morphium.data;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link MethodNameParser}, covering the "match all" contract
 * (empty suffix after prefix) and basic method-name derivation.
 */
class MethodNameParserTest {

    private static final Set<String> ENTITY_FIELDS = Set.of("id", "status", "name", "campaignNumber", "createdAt");

    @Nested
    @DisplayName("Empty suffix — match all contract")
    class MatchAllTests {

        @Test
        @DisplayName("countBy() with empty suffix returns count-all descriptor")
        void countByEmptySuffix() {
            QueryDescriptor result = MethodNameParser.parse("countBy", ENTITY_FIELDS);

            assertThat(result.prefix()).isEqualTo(QueryDescriptor.Prefix.COUNT);
            assertThat(result.conditions()).isEmpty();
            assertThat(result.combinator()).isEqualTo(QueryDescriptor.Combinator.AND);
            assertThat(result.returnType()).isEqualTo(QueryDescriptor.ReturnType.COUNT);
        }

        @Test
        @DisplayName("findBy() with empty suffix returns find-all descriptor")
        void findByEmptySuffix() {
            QueryDescriptor result = MethodNameParser.parse("findBy", ENTITY_FIELDS);

            assertThat(result.prefix()).isEqualTo(QueryDescriptor.Prefix.FIND);
            assertThat(result.conditions()).isEmpty();
            assertThat(result.combinator()).isEqualTo(QueryDescriptor.Combinator.AND);
            assertThat(result.returnType()).isEqualTo(QueryDescriptor.ReturnType.LIST);
        }

        @Test
        @DisplayName("existsBy() with empty suffix returns exists-all descriptor")
        void existsByEmptySuffix() {
            QueryDescriptor result = MethodNameParser.parse("existsBy", ENTITY_FIELDS);

            assertThat(result.prefix()).isEqualTo(QueryDescriptor.Prefix.EXISTS);
            assertThat(result.conditions()).isEmpty();
            assertThat(result.combinator()).isEqualTo(QueryDescriptor.Combinator.AND);
            assertThat(result.returnType()).isEqualTo(QueryDescriptor.ReturnType.BOOLEAN);
        }

        @Test
        @DisplayName("deleteBy() with empty suffix returns delete-all descriptor")
        void deleteByEmptySuffix() {
            QueryDescriptor result = MethodNameParser.parse("deleteBy", ENTITY_FIELDS);

            assertThat(result.prefix()).isEqualTo(QueryDescriptor.Prefix.DELETE);
            assertThat(result.conditions()).isEmpty();
            assertThat(result.combinator()).isEqualTo(QueryDescriptor.Combinator.AND);
            assertThat(result.returnType()).isEqualTo(QueryDescriptor.ReturnType.COUNT);
        }
    }

    @Nested
    @DisplayName("Single condition parsing")
    class SingleConditionTests {

        @Test
        @DisplayName("findByStatus parses as FIND with EQ on status")
        void findByStatus() {
            QueryDescriptor result = MethodNameParser.parse("findByStatus", ENTITY_FIELDS);

            assertThat(result.prefix()).isEqualTo(QueryDescriptor.Prefix.FIND);
            assertThat(result.conditions()).hasSize(1);
            assertThat(result.conditions().get(0).field()).isEqualTo("status");
            assertThat(result.conditions().get(0).operator()).isEqualTo(QueryDescriptor.Operator.EQ);
        }

        @Test
        @DisplayName("existsById parses as EXISTS with EQ on id")
        void existsById() {
            QueryDescriptor result = MethodNameParser.parse("existsById", ENTITY_FIELDS);

            assertThat(result.prefix()).isEqualTo(QueryDescriptor.Prefix.EXISTS);
            assertThat(result.conditions()).hasSize(1);
            assertThat(result.conditions().get(0).field()).isEqualTo("id");
            assertThat(result.conditions().get(0).operator()).isEqualTo(QueryDescriptor.Operator.EQ);
        }

        @Test
        @DisplayName("deleteByStatus parses as DELETE with EQ on status")
        void deleteByStatus() {
            QueryDescriptor result = MethodNameParser.parse("deleteByStatus", ENTITY_FIELDS);

            assertThat(result.prefix()).isEqualTo(QueryDescriptor.Prefix.DELETE);
            assertThat(result.conditions()).hasSize(1);
            assertThat(result.conditions().get(0).field()).isEqualTo("status");
            assertThat(result.conditions().get(0).operator()).isEqualTo(QueryDescriptor.Operator.EQ);
        }
    }

    @Nested
    @DisplayName("Method name validation")
    class ValidationTests {

        @Test
        @DisplayName("Invalid prefix throws IllegalArgumentException")
        void invalidPrefix() {
            assertThatThrownBy(() -> MethodNameParser.parse("getByStatus", ENTITY_FIELDS))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Cannot parse repository method name");
        }

        @Test
        @DisplayName("Mixed And/Or combinators throw IllegalArgumentException instead of silently mis-parsing")
        void mixedAndOrCombinatorsRejected() {
            assertThatThrownBy(() ->
                    MethodNameParser.parse("findByStatusAndCategoryOrPriority", ENTITY_FIELDS))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Mixed And/Or combinators")
                    .hasMessageContaining("findByStatusAndCategoryOrPriority");
        }

        @Test
        @DisplayName("Unknown field (typo) in derived query method throws IllegalArgumentException")
        void unknownFieldRejected() {
            assertThatThrownBy(() ->
                    MethodNameParser.parse("findByStatuss", ENTITY_FIELDS))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Unknown field")
                    .hasMessageContaining("statuss");
        }

        @Test
        @DisplayName("Unknown field validation is skipped when entityFields is null (no validation possible)")
        void unknownFieldNotRejectedWhenEntityFieldsNull() {
            QueryDescriptor result = MethodNameParser.parse("findByStatuss", null);

            assertThat(result.conditions()).hasSize(1);
            assertThat(result.conditions().get(0).field()).isEqualTo("statuss");
        }

        @Test
        @DisplayName("Unknown field validation is skipped when entityFields is empty (no validation possible)")
        void unknownFieldNotRejectedWhenEntityFieldsEmpty() {
            QueryDescriptor result = MethodNameParser.parse("findByStatuss", Set.of());

            assertThat(result.conditions()).hasSize(1);
            assertThat(result.conditions().get(0).field()).isEqualTo("statuss");
        }
    }

    @Nested
    @DisplayName("Combinator detection with acronym/digit-ending field segments")
    class CombinatorAcronymTests {

        private static final Set<String> URL_ENTITY_FIELDS = Set.of("url", "status", "category");

        @Test
        @DisplayName("findByURLOrStatus splits correctly into URL and Status despite acronym ending in uppercase")
        void acronymEndingSegmentSplitsOnOr() {
            QueryDescriptor result = MethodNameParser.parse("findByURLOrStatus", URL_ENTITY_FIELDS);

            assertThat(result.prefix()).isEqualTo(QueryDescriptor.Prefix.FIND);
            assertThat(result.combinator()).isEqualTo(QueryDescriptor.Combinator.OR);
            assertThat(result.conditions()).hasSize(2);
            assertThat(result.conditions().get(0).field()).isEqualTo("url");
            assertThat(result.conditions().get(1).field()).isEqualTo("status");
        }

        @Test
        @DisplayName("findByStatusAndCategory (normal lowercase-before-combinator case) still splits correctly")
        void regularLowercaseSegmentStillSplitsOnAnd() {
            QueryDescriptor result = MethodNameParser.parse("findByStatusAndCategory", URL_ENTITY_FIELDS);

            assertThat(result.prefix()).isEqualTo(QueryDescriptor.Prefix.FIND);
            assertThat(result.combinator()).isEqualTo(QueryDescriptor.Combinator.AND);
            assertThat(result.conditions()).hasSize(2);
            assertThat(result.conditions().get(0).field()).isEqualTo("status");
            assertThat(result.conditions().get(1).field()).isEqualTo("category");
        }
    }
}
