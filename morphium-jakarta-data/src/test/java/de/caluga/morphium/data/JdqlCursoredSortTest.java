package de.caluga.morphium.data;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import jakarta.data.Order;
import jakarta.data.Sort;
import jakarta.data.page.CursoredPage;
import jakarta.data.page.PageRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * MOR-087: the keyset of a {@code @Query} that returns a {@code CursoredPage} is the ORDER BY of the query (or {@code @OrderBy}), then every dynamic {@code Sort}, {@code Sort...} and
 * {@code Order}. The dynamic ones were left out, so a query without ORDER BY and with an {@code Order} parameter had no keyset at all.
 */
class JdqlCursoredSortTest {

    @Entity
    static class Item {
        @Id
        private String id;
        private int amount;

        Item() {}

        Item(String id, int amount) {
            this.id = id;
            this.amount = amount;
        }
    }

    static class ItemRepo extends AbstractMorphiumRepository<Item, String> {
        ItemRepo(Morphium morphium) {
            super(new RepositoryMetadata(Item.class, String.class, "id"));
            setMorphium(morphium);
        }
    }

    private Morphium morphium;
    private ItemRepo repo;

    @BeforeEach
    void setUp() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setDatabase("mor087_" + System.nanoTime());
        cfg.addHostToSeed("localhost");
        cfg.setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg);
        repo = new ItemRepo(morphium);

        for (int i = 1; i <= 5; i++) {
            morphium.store(new Item("i" + i, i * 10));
        }
    }

    @AfterEach
    void tearDown() {
        morphium.close();
    }

    @SuppressWarnings("unchecked")
    private CursoredPage<Item> page(String jdql, String orderBySpec, Object[] args, int sortIdx, int orderIdx, int pageIdx) {
        return (CursoredPage<Item>) JdqlMethodBridge.executeJdql(repo, jdql, "", sortIdx, orderIdx, pageIdx, -1, args, false, false, false, false, true, orderBySpec, false, null);
    }

    private static List<String> ids(CursoredPage<Item> page) {
        return page.content().stream().map(i -> i.id).toList();
    }

    @Test
    @DisplayName("a dynamic Order is the keyset when the query has no ORDER BY")
    void dynamicOrderIsTheKeyset() {
        Order<Item> order = Order.by(Sort.desc("amount"));
        CursoredPage<Item> first = page("WHERE amount > 0", "", new Object[] {order, PageRequest.ofSize(2)}, -1, 0, 1);

        assertThat(ids(first)).containsExactly("i5", "i4");

        CursoredPage<Item> second = page("WHERE amount > 0", "", new Object[] {order, first.nextPageRequest()}, -1, 0, 1);
        assertThat(ids(second)).containsExactly("i3", "i2");
    }

    @Test
    @DisplayName("a dynamic Sort comes after the ORDER BY of the query")
    void dynamicSortAfterOrderBy() {
        morphium.store(new Item("i6", 10));    // the same amount as i1

        CursoredPage<Item> first = page("WHERE amount > 0 ORDER BY amount", "", new Object[] {Sort.desc("id"), PageRequest.ofSize(3)}, 0, -1, 1);

        assertThat(ids(first)).containsExactly("i6", "i1", "i2");

        CursoredPage<Item> second = page("WHERE amount > 0 ORDER BY amount", "", new Object[] {Sort.desc("id"), first.nextPageRequest()}, 0, -1, 1);
        assertThat(ids(second)).containsExactly("i3", "i4", "i5");
    }
}
