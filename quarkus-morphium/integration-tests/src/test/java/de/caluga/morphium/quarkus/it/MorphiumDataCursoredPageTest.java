package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.Morphium;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.data.Order;
import jakarta.data.Sort;
import jakarta.data.page.CursoredPage;
import jakarta.data.page.PageRequest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for CursoredPage (keyset/cursor-based pagination).
 */
@QuarkusTest
@DisplayName("Jakarta Data CursoredPage Pagination")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MorphiumDataCursoredPageTest {

    @Inject
    PaginatedOrderRepository repository;

    @Inject
    Morphium morphium;

    @BeforeEach
    void setUp() {
        morphium.clearCollection(OrderEntity.class);
        // Create 25 OPEN orders with amounts 10, 20, ..., 250
        for (int i = 1; i <= 25; i++) {
            var order = new OrderEntity();
            order.setCustomerId("C" + i);
            order.setAmount(i * 10.0);
            order.setStatus("OPEN");
            morphium.store(order);
        }
    }

    @AfterEach
    void resetThreadLocals() {
        morphium.resetThreadLocalOverrides();
    }

    @Test
    @org.junit.jupiter.api.Order(1)
    @DisplayName("#1 First page with offset mode returns correct results")
    void firstPage_offsetMode() {
        PageRequest request = PageRequest.ofSize(5);
        CursoredPage<OrderEntity> page = repository.findPagedByStatus("OPEN", request);

        assertThat(page.content()).hasSize(5);
        assertThat(page.hasNext()).isTrue();
        assertThat(page.numberOfElements()).isEqualTo(5);
        // Cursors should be available for each element
        for (int i = 0; i < page.numberOfElements(); i++) {
            assertThat(page.cursor(i)).isNotNull();
        }
    }

    @Test
    @org.junit.jupiter.api.Order(2)
    @DisplayName("#2 Next page via CURSOR_NEXT returns subsequent results")
    void nextPage_cursorNext() {
        PageRequest request = PageRequest.ofSize(5);
        CursoredPage<OrderEntity> page1 = repository.findPagedByStatus("OPEN", request);

        PageRequest nextRequest = page1.nextPageRequest();
        assertThat(nextRequest).isNotNull();

        CursoredPage<OrderEntity> page2 = repository.findPagedByStatus("OPEN", nextRequest);
        assertThat(page2.content()).hasSize(5);

        // No duplicates between page1 and page2
        List<String> page1Ids = page1.content().stream().map(OrderEntity::getId).toList();
        List<String> page2Ids = page2.content().stream().map(OrderEntity::getId).toList();
        assertThat(page2Ids).doesNotContainAnyElementsOf(page1Ids);

        // Page2 amounts should be higher than page1 amounts (sorted by amount ASC)
        double lastAmountPage1 = page1.content().get(page1.numberOfElements() - 1).getAmount();
        double firstAmountPage2 = page2.content().get(0).getAmount();
        assertThat(firstAmountPage2).isGreaterThan(lastAmountPage1);
    }

    @Test
    @org.junit.jupiter.api.Order(3)
    @DisplayName("#3 Previous page via CURSOR_PREVIOUS returns original results")
    void previousPage_cursorPrevious() {
        PageRequest request = PageRequest.ofSize(5);
        CursoredPage<OrderEntity> page1 = repository.findPagedByStatus("OPEN", request);
        CursoredPage<OrderEntity> page2 = repository.findPagedByStatus("OPEN", page1.nextPageRequest());

        PageRequest prevRequest = page2.previousPageRequest();
        assertThat(prevRequest).isNotNull();

        CursoredPage<OrderEntity> prevPage = repository.findPagedByStatus("OPEN", prevRequest);
        assertThat(prevPage.content()).hasSize(5);

        // Previous page should have the same IDs as page1
        List<String> page1Ids = page1.content().stream().map(OrderEntity::getId).toList();
        List<String> prevPageIds = prevPage.content().stream().map(OrderEntity::getId).toList();
        assertThat(prevPageIds).containsExactlyElementsOf(page1Ids);
    }

    @Test
    @org.junit.jupiter.api.Order(4)
    @DisplayName("#4 Last page has hasNext=false")
    void lastPage_hasNextFalse() {
        PageRequest request = PageRequest.ofSize(5);
        CursoredPage<OrderEntity> page = repository.findPagedByStatus("OPEN", request);

        List<OrderEntity> allCollected = new ArrayList<>(page.content());
        int pages = 1;
        while (page.hasNext()) {
            page = repository.findPagedByStatus("OPEN", page.nextPageRequest());
            allCollected.addAll(page.content());
            pages++;
        }

        assertThat(page.hasNext()).isFalse();
        assertThat(allCollected).hasSize(25);
        assertThat(pages).isEqualTo(5); // 25 items / 5 per page
    }

    @Test
    @org.junit.jupiter.api.Order(5)
    @DisplayName("#5 Cursor values match sort field values")
    void cursorValues_matchSortFields() {
        PageRequest request = PageRequest.ofSize(5);
        CursoredPage<OrderEntity> page = repository.findPagedByStatus("OPEN", request);

        for (int i = 0; i < page.numberOfElements(); i++) {
            PageRequest.Cursor cursor = page.cursor(i);
            OrderEntity entity = page.content().get(i);
            // Cursor should have 2 elements (amount, id)
            assertThat(cursor.size()).isEqualTo(2);
            assertThat(cursor.get(0)).isEqualTo(entity.getAmount());
            assertThat(cursor.get(1)).isEqualTo(entity.getId());
        }
    }

    @Test
    @org.junit.jupiter.api.Order(6)
    @DisplayName("#6 @Query with CursoredPage works")
    void queryAnnotation_cursoredPage() {
        PageRequest request = PageRequest.ofSize(5);
        CursoredPage<OrderEntity> page = repository.queryPagedByStatus("OPEN", request);

        assertThat(page.content()).hasSize(5);
        assertThat(page.hasNext()).isTrue();

        // Navigate to next page
        CursoredPage<OrderEntity> page2 = repository.queryPagedByStatus("OPEN", page.nextPageRequest());
        assertThat(page2.content()).hasSize(5);

        // No duplicates
        List<String> page1Ids = page.content().stream().map(OrderEntity::getId).toList();
        List<String> page2Ids = page2.content().stream().map(OrderEntity::getId).toList();
        assertThat(page2Ids).doesNotContainAnyElementsOf(page1Ids);
    }

    @Test
    @org.junit.jupiter.api.Order(7)
    @DisplayName("#7 findAll with CursoredPage works")
    void findAll_cursoredPage() {
        Order<OrderEntity> order = Order.by(Sort.asc("amount"), Sort.asc("id"));
        PageRequest request = PageRequest.ofSize(10);
        CursoredPage<OrderEntity> page = repository.findAll(request, order);

        assertThat(page.content()).hasSize(10);
        assertThat(page.hasNext()).isTrue();

        CursoredPage<OrderEntity> page2 = repository.findAll(page.nextPageRequest(), order);
        assertThat(page2.content()).hasSize(10);

        // Verify ordering
        double lastAmount = page.content().get(page.numberOfElements() - 1).getAmount();
        double firstAmountPage2 = page2.content().get(0).getAmount();
        assertThat(firstAmountPage2).isGreaterThan(lastAmount);
    }

    @Test
    @org.junit.jupiter.api.Order(8)
    @DisplayName("#8 withTotal returns correct totalElements")
    void withTotal_countWorks() {
        PageRequest request = PageRequest.ofSize(5).withTotal();
        CursoredPage<OrderEntity> page = repository.findPagedByStatus("OPEN", request);

        assertThat(page.hasTotals()).isTrue();
        assertThat(page.totalElements()).isEqualTo(25);
        assertThat(page.totalPages()).isEqualTo(5);
    }

    @Test
    @org.junit.jupiter.api.Order(9)
    @DisplayName("#9 Empty result returns empty page with hasNext=false")
    void emptyResult_noCursors() {
        PageRequest request = PageRequest.ofSize(5);
        CursoredPage<OrderEntity> page = repository.findPagedByStatus("NONEXISTENT", request);

        assertThat(page.content()).isEmpty();
        assertThat(page.hasNext()).isFalse();
        assertThat(page.numberOfElements()).isEqualTo(0);
        assertThat(page.hasContent()).isFalse();
    }
}
