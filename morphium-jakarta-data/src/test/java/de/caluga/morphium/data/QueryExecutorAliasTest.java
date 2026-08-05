package de.caluga.morphium.data;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Aliases;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.data.QueryDescriptor.Combinator;
import de.caluga.morphium.data.QueryDescriptor.Condition;
import de.caluga.morphium.data.QueryDescriptor.Operator;
import de.caluga.morphium.data.QueryDescriptor.Prefix;
import de.caluga.morphium.data.QueryDescriptor.ReturnType;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.query.Query;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that derived queries correctly use $or to match both the current
 * MongoDB field name and any @Aliases, so old documents stored under a
 * previous field name are found.
 */
class QueryExecutorAliasTest {

    private static Morphium morphium;

    @Entity
    static class OtaUpdate {
        @Id
        private String id;

        private String campaignNumber;

        private String vin;

        @Aliases({"updateId"})
        private String otaUpdateId;

        @Aliases({"uploadDate"})
        private String otaUploadDate;

        private String status;
    }

    @BeforeAll
    static void setUp() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setDatabase("test");
        cfg.addHostToSeed("localhost");
        cfg.setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg);
    }

    @AfterAll
    static void tearDown() {
        if (morphium != null) {
            morphium.close();
        }
    }

    @Test
    @DisplayName("Query on aliased field generates $or with current name + aliases")
    void queryOnAliasedFieldGeneratesOr() {
        QueryDescriptor descriptor = new QueryDescriptor(
                Prefix.FIND,
                List.of(new Condition("otaUpdateId", Operator.EQ, 0)),
                Combinator.AND,
                List.of(),
                ReturnType.LIST
        );

        Query<?> query = buildQuery(descriptor, new Object[]{"update-123"});
        Map<String, Object> queryObj = query.toQueryObject();

        assertThat(queryObj).containsKey("$or");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> orList = (List<Map<String, Object>>) queryObj.get("$or");
        assertThat(orList).hasSize(2);

        Set<String> queriedFields = new HashSet<>();
        for (Map<String, Object> sub : orList) {
            queriedFields.addAll(sub.keySet());
        }
        assertThat(queriedFields).containsExactlyInAnyOrder("ota_update_id", "updateId");
    }

    @Test
    @DisplayName("Query on non-aliased field generates simple condition (no $or)")
    void queryOnNonAliasedFieldIsSimple() {
        QueryDescriptor descriptor = new QueryDescriptor(
                Prefix.FIND,
                List.of(new Condition("campaignNumber", Operator.EQ, 0)),
                Combinator.AND,
                List.of(),
                ReturnType.LIST
        );

        Query<?> query = buildQuery(descriptor, new Object[]{"CN-001"});
        Map<String, Object> queryObj = query.toQueryObject();

        assertThat(queryObj).doesNotContainKey("$or");
        assertThat(queryObj).containsKey("campaign_number");
    }

    @Test
    @DisplayName("Combined AND query with aliased + non-aliased fields")
    void combinedAndQueryWithAliasedField() {
        QueryDescriptor descriptor = new QueryDescriptor(
                Prefix.FIND,
                List.of(
                        new Condition("campaignNumber", Operator.EQ, 0),
                        new Condition("otaUpdateId", Operator.EQ, 1)
                ),
                Combinator.AND,
                List.of(),
                ReturnType.LIST
        );

        Query<?> query = buildQuery(descriptor, new Object[]{"CN-001", "update-123"});
        Map<String, Object> queryObj = query.toQueryObject();

        // Should have $and containing the simple condition + the $or for the alias
        assertThat(queryObj).containsKey("$and");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> andList = (List<Map<String, Object>>) queryObj.get("$and");
        assertThat(andList).hasSizeGreaterThanOrEqualTo(2);

        // Find the campaign_number condition in $and
        boolean hasCampaignNumber = andList.stream()
                .anyMatch(entry -> entry.containsKey("campaign_number"));
        assertThat(hasCampaignNumber).as("$and should contain campaign_number condition").isTrue();

        // Find the $or sub-condition for otaUpdateId aliases
        @SuppressWarnings("unchecked")
        Optional<Map<String, Object>> orEntry = andList.stream()
                .filter(entry -> entry.containsKey("$or"))
                .findFirst();
        assertThat(orEntry).as("$and should contain an $or entry for aliased field").isPresent();

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> orList = (List<Map<String, Object>>) orEntry.get().get("$or");
        Set<String> orFields = new HashSet<>();
        for (Map<String, Object> sub : orList) {
            orFields.addAll(sub.keySet());
        }
        assertThat(orFields).containsExactlyInAnyOrder("ota_update_id", "updateId");
    }

    @Test
    @DisplayName("Multiple aliased fields each get their own $or")
    void multipleAliasedFields() {
        QueryDescriptor descriptor = new QueryDescriptor(
                Prefix.FIND,
                List.of(
                        new Condition("otaUpdateId", Operator.EQ, 0),
                        new Condition("otaUploadDate", Operator.EQ, 1)
                ),
                Combinator.AND,
                List.of(),
                ReturnType.LIST
        );

        Query<?> query = buildQuery(descriptor, new Object[]{"update-123", "2025-01-01"});
        Map<String, Object> queryObj = query.toQueryObject();

        // Should have $and with two $or groups
        assertThat(queryObj).containsKey("$and");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> andList = (List<Map<String, Object>>) queryObj.get("$and");

        // Collect all $or groups from the $and list
        List<Set<String>> orFieldSets = new ArrayList<>();
        for (Map<String, Object> entry : andList) {
            if (entry.containsKey("$or")) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> orList = (List<Map<String, Object>>) entry.get("$or");
                Set<String> fields = new HashSet<>();
                for (Map<String, Object> sub : orList) {
                    fields.addAll(sub.keySet());
                }
                orFieldSets.add(fields);
            }
        }
        assertThat(orFieldSets).as("should have two separate $or groups").hasSize(2);

        // One $or for otaUpdateId aliases, one for otaUploadDate aliases
        assertThat(orFieldSets).anySatisfy(fields ->
                assertThat(fields).containsExactlyInAnyOrder("ota_update_id", "updateId"));
        assertThat(orFieldSets).anySatisfy(fields ->
                assertThat(fields).containsExactlyInAnyOrder("ota_upload_date", "uploadDate"));
    }

    @Test
    @DisplayName("IN operator on aliased field generates $or with $in payload")
    void inOperatorOnAliasedField() {
        QueryDescriptor descriptor = new QueryDescriptor(
                Prefix.FIND,
                List.of(new Condition("otaUpdateId", Operator.IN, 0)),
                Combinator.AND,
                List.of(),
                ReturnType.LIST
        );

        List<String> searchValues = List.of("u1", "u2");
        Query<?> query = buildQuery(descriptor, new Object[]{searchValues});
        Map<String, Object> queryObj = query.toQueryObject();

        assertThat(queryObj).containsKey("$or");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> orList = (List<Map<String, Object>>) queryObj.get("$or");
        assertThat(orList).hasSize(2);

        // Verify each $or branch contains $in with the correct values
        Set<String> queriedFields = new HashSet<>();
        for (Map<String, Object> sub : orList) {
            for (Map.Entry<String, Object> e : sub.entrySet()) {
                queriedFields.add(e.getKey());
                @SuppressWarnings("unchecked")
                Map<String, Object> opMap = (Map<String, Object>) e.getValue();
                assertThat(opMap).containsKey("$in");
                assertThat(opMap.get("$in")).isEqualTo(searchValues);
            }
        }
        assertThat(queriedFields).containsExactlyInAnyOrder("ota_update_id", "updateId");
    }

    @Test
    @DisplayName("OR combinator with aliased field generates nested $or")
    void orCombinatorWithAliasedField() {
        // findByCampaignNumberOrOtaUpdateId — OR combinator, one aliased field
        QueryDescriptor descriptor = new QueryDescriptor(
                Prefix.FIND,
                List.of(
                        new Condition("campaignNumber", Operator.EQ, 0),
                        new Condition("otaUpdateId", Operator.EQ, 1)
                ),
                Combinator.OR,
                List.of(),
                ReturnType.LIST
        );

        Query<?> query = buildQuery(descriptor, new Object[]{"CN-001", "update-123"});
        Map<String, Object> queryObj = query.toQueryObject();

        // The top-level OR should contain: one branch for campaignNumber,
        // and one branch that itself contains $or for the aliased otaUpdateId
        assertThat(queryObj).containsKey("$or");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> orList = (List<Map<String, Object>>) queryObj.get("$or");
        assertThat(orList).hasSize(2);

        // One branch should have campaign_number
        boolean hasCampaignNumber = orList.stream()
                .anyMatch(entry -> entry.containsKey("campaign_number"));
        assertThat(hasCampaignNumber).as("top-level $or should contain campaign_number branch").isTrue();

        // The other branch should have a nested $or for the aliased field
        @SuppressWarnings("unchecked")
        Optional<Map<String, Object>> aliasedBranch = orList.stream()
                .filter(entry -> entry.containsKey("$or"))
                .findFirst();
        assertThat(aliasedBranch).as("top-level $or should contain a nested $or for aliased field").isPresent();

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> nestedOr = (List<Map<String, Object>>) aliasedBranch.get().get("$or");
        Set<String> nestedFields = new HashSet<>();
        for (Map<String, Object> sub : nestedOr) {
            nestedFields.addAll(sub.keySet());
        }
        assertThat(nestedFields).containsExactlyInAnyOrder("ota_update_id", "updateId");
    }

    @Test
    @DisplayName("LIKE operator generates anchored $regex with escaped metacharacters")
    void likeOperatorGeneratesRegex() {
        QueryDescriptor descriptor = new QueryDescriptor(
                Prefix.FIND,
                List.of(new Condition("campaignNumber", Operator.LIKE, 0)),
                Combinator.AND,
                List.of(),
                ReturnType.LIST
        );

        Query<?> query = buildQuery(descriptor, new Object[]{"%test[1]%"});
        Map<String, Object> queryObj = query.toQueryObject();

        assertThat(queryObj).containsKey("campaign_number");
        @SuppressWarnings("unchecked")
        Map<String, Object> regexMap = (Map<String, Object>) queryObj.get("campaign_number");
        String regex = (String) regexMap.get("$regex");
        assertThat(regex).startsWith("^");
        assertThat(regex).endsWith("$");
        // Pattern.quote escapes the whole literal block including [1]
        assertThat(regex).contains("\\Qtest[1]\\E");
    }

    @Test
    @DisplayName("STARTS_WITH generates anchored $regex with ^ prefix")
    void startsWithGeneratesRegex() {
        QueryDescriptor descriptor = new QueryDescriptor(
                Prefix.FIND,
                List.of(new Condition("campaignNumber", Operator.STARTS_WITH, 0)),
                Combinator.AND,
                List.of(),
                ReturnType.LIST
        );

        Query<?> query = buildQuery(descriptor, new Object[]{"CN-"});
        Map<String, Object> queryObj = query.toQueryObject();

        @SuppressWarnings("unchecked")
        Map<String, Object> regexMap = (Map<String, Object>) queryObj.get("campaign_number");
        String regex = (String) regexMap.get("$regex");
        assertThat(regex).startsWith("^");
        assertThat(regex).contains("\\QCN-\\E");
    }

    @Test
    @DisplayName("ENDS_WITH generates $regex with $ suffix")
    void endsWithGeneratesRegex() {
        QueryDescriptor descriptor = new QueryDescriptor(
                Prefix.FIND,
                List.of(new Condition("status", Operator.ENDS_WITH, 0)),
                Combinator.AND,
                List.of(),
                ReturnType.LIST
        );

        Query<?> query = buildQuery(descriptor, new Object[]{"_done"});
        Map<String, Object> queryObj = query.toQueryObject();

        @SuppressWarnings("unchecked")
        Map<String, Object> regexMap = (Map<String, Object>) queryObj.get("status");
        String regex = (String) regexMap.get("$regex");
        assertThat(regex).endsWith("$");
        assertThat(regex).contains("\\Q_done\\E");
    }

    @Test
    @DisplayName("MATCHES generates $regex with raw pattern")
    void matchesGeneratesRegex() {
        QueryDescriptor descriptor = new QueryDescriptor(
                Prefix.FIND,
                List.of(new Condition("vin", Operator.MATCHES, 0)),
                Combinator.AND,
                List.of(),
                ReturnType.LIST
        );

        Query<?> query = buildQuery(descriptor, new Object[]{"^WBA[0-9]+"});
        Map<String, Object> queryObj = query.toQueryObject();

        @SuppressWarnings("unchecked")
        Map<String, Object> regexMap = (Map<String, Object>) queryObj.get("vin");
        assertThat(regexMap).containsEntry("$regex", "^WBA[0-9]+");
    }

    @Test
    @DisplayName("IGNORE_CASE generates $regex with case-insensitive option")
    void ignoreCaseGeneratesRegexWithOption() {
        QueryDescriptor descriptor = new QueryDescriptor(
                Prefix.FIND,
                List.of(new Condition("status", Operator.IGNORE_CASE, 0)),
                Combinator.AND,
                List.of(),
                ReturnType.LIST
        );

        Query<?> query = buildQuery(descriptor, new Object[]{"Active"});
        Map<String, Object> queryObj = query.toQueryObject();

        @SuppressWarnings("unchecked")
        Map<String, Object> regexMap = (Map<String, Object>) queryObj.get("status");
        assertThat(regexMap).containsKey("$regex");
        assertThat(regexMap).containsEntry("$options", "i");
        String regex = (String) regexMap.get("$regex");
        assertThat(regex).startsWith("^");
        assertThat(regex).endsWith("$");
        assertThat(regex).contains("\\QActive\\E");
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Query<?> buildQuery(QueryDescriptor descriptor, Object[] args) {
        Class entityClass = OtaUpdate.class;
        Query query = morphium.createQueryFor(entityClass);
        QueryExecutor.applyConditions(query, descriptor, args, morphium, entityClass);
        return query;
    }
}
