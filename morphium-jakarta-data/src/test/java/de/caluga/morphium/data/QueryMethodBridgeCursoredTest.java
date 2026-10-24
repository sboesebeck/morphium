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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * MOR-086: a derived query ({@code findByGroupOrderByAmountAscIdAsc(group, PageRequest)}) can return a {@code CursoredPage}. The bridge answered a {@code MorphiumPage}, which the
 * generated code could not cast. The keyset is the order of the method name, then the {@code @OrderBy}, then the dynamic sorts.
 */
class QueryMethodBridgeCursoredTest {

    @Entity
    static class Row {
        @Id
        private String id;
        private String grp;
        private int amount;

        Row() {}

        Row(String id, String grp, int amount) {
            this.id = id;
            this.grp = grp;
            this.amount = amount;
        }
    }

    static class RowRepo extends AbstractMorphiumRepository<Row, String> {
        RowRepo(Morphium morphium) {
            super(new RepositoryMetadata(Row.class, String.class, "id"));
            setMorphium(morphium);
        }
    }

    private Morphium morphium;
    private RowRepo repo;

    @BeforeEach
    void setUp() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setDatabase("mor086_" + System.nanoTime());
        cfg.addHostToSeed("localhost");
        cfg.setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg);
        repo = new RowRepo(morphium);

        for (int i = 1; i <= 7; i++) {
            morphium.store(new Row("r" + i, i <= 5 ? "g" : "h", i * 10));
        }
    }

    @AfterEach
    void tearDown() {
        morphium.close();
    }

    @SuppressWarnings("unchecked")
    private CursoredPage<Row> page(String method, Object[] args, String orderBySpec, int orderIdx, int pageIdx) {
        return (CursoredPage<Row>) QueryMethodBridge.executeQuery(repo, method, args, false, false, false, false, orderBySpec, -1, orderIdx, pageIdx, -1, true);
    }

    private static List<String> ids(CursoredPage<Row> page) {
        return page.content().stream().map(r -> r.id).toList();
    }

    @Test
    @DisplayName("the first page, the next page by its cursor and the previous page back")
    void pages() {
        CursoredPage<Row> first = page("findByGrpOrderByAmountAsc", new Object[] {"g", PageRequest.ofSize(2)}, "", -1, 1);

        assertThat(ids(first)).containsExactly("r1", "r2");
        assertThat(first.hasNext()).isTrue();

        CursoredPage<Row> second = page("findByGrpOrderByAmountAsc", new Object[] {"g", first.nextPageRequest()}, "", -1, 1);
        assertThat(ids(second)).containsExactly("r3", "r4");

        CursoredPage<Row> third = page("findByGrpOrderByAmountAsc", new Object[] {"g", second.nextPageRequest()}, "", -1, 1);
        assertThat(ids(third)).containsExactly("r5");
        assertThat(third.hasNext()).isFalse();

        CursoredPage<Row> back = page("findByGrpOrderByAmountAsc", new Object[] {"g", third.previousPageRequest()}, "", -1, 1);
        assertThat(ids(back)).containsExactly("r3", "r4");
    }

    @Test
    @DisplayName("totals when asked for, and an empty result")
    void totalsAndEmpty() {
        CursoredPage<Row> withTotal = page("findByGrpOrderByAmountDesc", new Object[] {"g", PageRequest.ofSize(2).withTotal()}, "", -1, 1);

        assertThat(ids(withTotal)).containsExactly("r5", "r4");
        assertThat(withTotal.totalElements()).isEqualTo(5);

        CursoredPage<Row> none = page("findByGrpOrderByAmountAsc", new Object[] {"nobody", PageRequest.ofSize(2)}, "", -1, 1);
        assertThat(none.content()).isEmpty();
        assertThat(none.hasNext()).isFalse();
    }

    @Test
    @DisplayName("the keyset is the order of the name, then @OrderBy, then a dynamic Order")
    void keysetOrder() {
        morphium.store(new Row("r8", "g", 10));    // the same amount as r1

        CursoredPage<Row> byNameThenId = page("findByGrpOrderByAmountAsc", new Object[] {"g", PageRequest.ofSize(3)}, "id:ASC", -1, 1);
        assertThat(ids(byNameThenId)).containsExactly("r1", "r8", "r2");

        CursoredPage<Row> next = page("findByGrpOrderByAmountAsc", new Object[] {"g", byNameThenId.nextPageRequest()}, "id:ASC", -1, 1);
        assertThat(ids(next)).containsExactly("r3", "r4", "r5");

        Order<Row> order = Order.by(Sort.desc("id"));
        CursoredPage<Row> dynamic = page("findByGrpOrderByAmountAsc", new Object[] {"g", order, PageRequest.ofSize(3)}, "", 1, 2);
        assertThat(ids(dynamic)).containsExactly("r8", "r1", "r2");
    }

    @Test
    @DisplayName("the async form answers a CursoredPage too")
    @SuppressWarnings("unchecked")
    void async() throws Exception {
        CursoredPage<Row> first = (CursoredPage<Row>) QueryMethodBridge.executeQueryAsync(repo, "findByGrpOrderByAmountAsc", new Object[] {"g", PageRequest.ofSize(2)}, false, false, false, false, "", -1, -1, 1,
                -1, true).toCompletableFuture().get(10, java.util.concurrent.TimeUnit.SECONDS);

        assertThat(ids(first)).containsExactly("r1", "r2");
        assertThat(first.hasNext()).isTrue();
    }

    @Test
    @DisplayName("without any order there is no keyset: the bridge says so")
    void noKeyset() {
        CursoredPage<Row> first = page("findByGrpOrderByAmountAsc", new Object[] {"g", PageRequest.ofSize(2)}, "", -1, 1);

        assertThatThrownBy(() -> page("findByGrp", new Object[] {"g", first.nextPageRequest()}, "", -1, 1)).isInstanceOf(IllegalArgumentException.class);
    }
}
