package de.caluga.test.morphium;

import de.caluga.test.mongo.suite.data.UncachedObject;
import de.caluga.test.mongo.suite.inmem.MorphiumInMemTestBase;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for missing {@code return} in
 * {@code MorphiumBase.save(T o, String collection, AsyncOperationCallback callback)}.
 *
 * Before the fix, passing a List to {@code save(list, collection, null)} would
 * first correctly call {@code saveList()}, then fall through and attempt to
 * treat the List itself as an entity — causing exceptions or silent corruption.
 *
 * @see <a href="https://github.com/sboesebeck/morphium/issues/XXX">GitHub Issue</a>
 */
class SaveListWithCollectionTest extends MorphiumInMemTestBase {

    private static final String COLLECTION = "uncached_object";

    @Test
    void save_list_with_collection_should_not_fall_through() {
        var o1 = new UncachedObject("one", 1);
        var o2 = new UncachedObject("two", 2);

        // This calls save(T o, String collection, callback) with a List — the buggy path
        morphium.store(Arrays.asList(o1, o2), COLLECTION);

        var result = morphium.createQueryFor(UncachedObject.class)
                .setCollectionName(COLLECTION)
                .asList();

        assertThat(result).hasSize(2);
        assertThat(result).extracting(UncachedObject::getStrValue)
                .containsExactlyInAnyOrder("one", "two");
    }

    @Test
    void save_collection_with_collection_should_not_fall_through() {
        var o1 = new UncachedObject("alpha", 10);
        var o2 = new UncachedObject("beta", 20);

        // HashSet is a Collection but not a List — tests the else-if branch
        var set = new HashSet<>(Arrays.asList(o1, o2));
        morphium.store(set, COLLECTION);

        var result = morphium.createQueryFor(UncachedObject.class)
                .setCollectionName(COLLECTION)
                .asList();

        assertThat(result).hasSize(2);
        assertThat(result).extracting(UncachedObject::getStrValue)
                .containsExactlyInAnyOrder("alpha", "beta");
    }

    @Test
    void save_single_entity_with_collection_still_works() {
        var o1 = new UncachedObject("single", 99);

        // Single entity path must still work after the fix
        morphium.store(o1, COLLECTION);

        var result = morphium.createQueryFor(UncachedObject.class)
                .setCollectionName(COLLECTION)
                .asList();

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getStrValue()).isEqualTo("single");
    }
}
