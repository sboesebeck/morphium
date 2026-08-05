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
    }
}
