package de.caluga.test.morphium;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.Sequence;
import de.caluga.morphium.Sequence.SeqLock;
import de.caluga.morphium.annotations.AutoSequence;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.inmem.InMemoryDriver;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link AutoSequence} annotation — verifies that Morphium automatically
 * assigns sequence numbers from a MongoDB-backed {@link de.caluga.morphium.SequenceGenerator}
 * when storing entities with {@code @AutoSequence}-annotated fields.
 */
@Tag("inmemory")
public class AutoSequenceAnnotationTest {

    // -------------------------------------------------------------------------
    // Test entity with a long @AutoSequence field (default sequence name)
    // -------------------------------------------------------------------------

    @Entity
    public static class OrderItem {
        @Id
        private MorphiumId id;

        /** Sequence name derived from the field's MongoDB name ("order_num"). */
        @AutoSequence(name = "order_num")
        private Long orderNum;

        private String description;

        public MorphiumId getId()      { return id; }
        public Long getOrderNum()      { return orderNum; }
        public void setOrderNum(Long v){ orderNum = v; }
        public String getDescription() { return description; }
        public void setDescription(String d){ description = d; }
    }

    // -------------------------------------------------------------------------
    // Entity with custom startValue and inc
    // -------------------------------------------------------------------------

    @Entity
    public static class Invoice {
        @Id
        private MorphiumId id;

        @AutoSequence(name = "invoice_num", startValue = 1000, inc = 10)
        private Long invoiceNumber;

        public MorphiumId getId()          { return id; }
        public Long getInvoiceNumber()     { return invoiceNumber; }
        public void setInvoiceNumber(Long v){ invoiceNumber = v; }
    }

    // -------------------------------------------------------------------------
    // Entity with int field type
    // -------------------------------------------------------------------------

    @Entity
    public static class Ticket {
        @Id
        private MorphiumId id;

        @AutoSequence(name = "ticket_seq")
        private Integer ticketNumber;

        public MorphiumId getId()              { return id; }
        public Integer getTicketNumber()       { return ticketNumber; }
        public void setTicketNumber(Integer v) { ticketNumber = v; }
    }

    // -------------------------------------------------------------------------
    // Entity with String field type
    // -------------------------------------------------------------------------

    @Entity
    public static class StringSeqEntity {
        @Id
        private MorphiumId id;

        @AutoSequence(name = "str_seq")
        private String seqValue;

        public MorphiumId getId()       { return id; }
        public String getSeqValue()     { return seqValue; }
        public void setSeqValue(String v){ seqValue = v; }
    }

    // -------------------------------------------------------------------------
    // Entity with two independent @AutoSequence fields
    // -------------------------------------------------------------------------

    @Entity
    public static class MultiSeqEntity {
        @Id
        private MorphiumId id;

        @AutoSequence(name = "multi_a")
        private Long seqA;

        @AutoSequence(name = "multi_b", startValue = 100)
        private Long seqB;

        public MorphiumId getId() { return id; }
        public Long getSeqA()    { return seqA; }
        public Long getSeqB()    { return seqB; }
    }

    // -------------------------------------------------------------------------
    // Setup / teardown
    // -------------------------------------------------------------------------

    private Morphium morphium;

    @BeforeEach
    public void setUp() {
        MorphiumConfig cfg = new MorphiumConfig("auto_seq_test_db", 10, 10_000, 1_000);
        cfg.setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg);
        morphium.dropCollection(Sequence.class);
        morphium.dropCollection(SeqLock.class);
        morphium.dropCollection(OrderItem.class);
        morphium.dropCollection(Invoice.class);
        morphium.dropCollection(Ticket.class);
        morphium.dropCollection(StringSeqEntity.class);
        morphium.dropCollection(MultiSeqEntity.class);
    }

    @AfterEach
    public void tearDown() {
        if (morphium != null) morphium.close();
    }

    // =========================================================================
    // Single-entity store
    // =========================================================================

    @Test
    public void singleStore_autoAssignsSequenceNumber() {
        OrderItem item = new OrderItem();
        item.setDescription("first");
        morphium.store(item);

        OrderItem loaded = morphium.createQueryFor(OrderItem.class).get();
        assertThat(loaded.getOrderNum()).isNotNull().isEqualTo(1L);
    }

    @Test
    public void singleStore_consecutiveStores_sequenceIsMonotonic() {
        for (int i = 1; i <= 5; i++) {
            OrderItem item = new OrderItem();
            item.setDescription("item-" + i);
            morphium.store(item);
        }

        List<OrderItem> all = morphium.createQueryFor(OrderItem.class)
                .sort("order_num").asList();
        assertThat(all).hasSize(5);
        for (int i = 0; i < 5; i++) {
            assertThat(all.get(i).getOrderNum()).isEqualTo((long)(i + 1));
        }
    }

    @Test
    public void singleStore_explicitValueNotOverwritten() {
        OrderItem item = new OrderItem();
        item.setOrderNum(42L);
        item.setDescription("manual");
        morphium.store(item);

        OrderItem loaded = morphium.createQueryFor(OrderItem.class).get();
        assertThat(loaded.getOrderNum()).isEqualTo(42L);
    }

    @Test
    public void singleStore_updateExistingEntity_sequenceNotReassigned() {
        OrderItem item = new OrderItem();
        item.setDescription("original");
        morphium.store(item);

        Long firstNum = morphium.createQueryFor(OrderItem.class).get().getOrderNum();

        // update the entity — orderNum must NOT change
        item.setDescription("updated");
        morphium.store(item);

        List<OrderItem> all = morphium.createQueryFor(OrderItem.class).asList();
        assertThat(all).hasSize(1);
        assertThat(all.get(0).getOrderNum()).isEqualTo(firstNum);
    }

    // =========================================================================
    // Custom startValue and inc
    // =========================================================================

    @Test
    public void customStartValue_firstValueMatchesStartValue() {
        Invoice inv = new Invoice();
        morphium.store(inv);

        Invoice loaded = morphium.createQueryFor(Invoice.class).get();
        assertThat(loaded.getInvoiceNumber()).isEqualTo(1000L);
    }

    @Test
    public void customInc_valuesIncrementByStep() {
        for (int i = 0; i < 4; i++) {
            Invoice inv = new Invoice();
            morphium.store(inv);
        }

        List<Invoice> all = morphium.createQueryFor(Invoice.class)
                .sort("invoice_number").asList();
        assertThat(all).hasSize(4);
        assertThat(all.get(0).getInvoiceNumber()).isEqualTo(1000L);
        assertThat(all.get(1).getInvoiceNumber()).isEqualTo(1010L);
        assertThat(all.get(2).getInvoiceNumber()).isEqualTo(1020L);
        assertThat(all.get(3).getInvoiceNumber()).isEqualTo(1030L);
    }

    // =========================================================================
    // Field type variants
    // =========================================================================

    @Test
    public void intFieldType_autoAssigned() {
        Ticket t = new Ticket();
        morphium.store(t);

        Ticket loaded = morphium.createQueryFor(Ticket.class).get();
        assertThat(loaded.getTicketNumber()).isEqualTo(1);
    }

    @Test
    public void stringFieldType_autoAssigned() {
        StringSeqEntity e = new StringSeqEntity();
        morphium.store(e);

        StringSeqEntity loaded = morphium.createQueryFor(StringSeqEntity.class).get();
        assertThat(loaded.getSeqValue()).isEqualTo("1");
    }

    // =========================================================================
    // Multiple @AutoSequence fields on one entity
    // =========================================================================

    @Test
    public void multipleAutoSequenceFields_bothAssigned() {
        MultiSeqEntity e = new MultiSeqEntity();
        morphium.store(e);

        MultiSeqEntity loaded = morphium.createQueryFor(MultiSeqEntity.class).get();
        assertThat(loaded.getSeqA()).isEqualTo(1L);
        assertThat(loaded.getSeqB()).isEqualTo(100L);
    }

    @Test
    public void multipleAutoSequenceFields_independentCounters() {
        for (int i = 0; i < 3; i++) {
            morphium.store(new MultiSeqEntity());
        }

        List<MultiSeqEntity> all = morphium.createQueryFor(MultiSeqEntity.class)
                .sort("seq_a").asList();
        assertThat(all).hasSize(3);
        assertThat(all.get(0).getSeqA()).isEqualTo(1L);
        assertThat(all.get(1).getSeqA()).isEqualTo(2L);
        assertThat(all.get(2).getSeqA()).isEqualTo(3L);
        assertThat(all.get(0).getSeqB()).isEqualTo(100L);
        assertThat(all.get(1).getSeqB()).isEqualTo(101L);
        assertThat(all.get(2).getSeqB()).isEqualTo(102L);
    }

    // =========================================================================
    // storeList — batch allocation
    // =========================================================================

    @Test
    public void storeList_allValuesAssigned() {
        List<OrderItem> items = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            OrderItem item = new OrderItem();
            item.setDescription("item-" + i);
            items.add(item);
        }
        morphium.storeList(items);

        List<OrderItem> loaded = morphium.createQueryFor(OrderItem.class)
                .sort("order_num").asList();
        assertThat(loaded).hasSize(10);
        for (int i = 0; i < 10; i++) {
            assertThat(loaded.get(i).getOrderNum()).isEqualTo((long)(i + 1));
        }
    }

    @Test
    public void storeList_valuesAreUnique() {
        List<OrderItem> items = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            items.add(new OrderItem());
        }
        morphium.storeList(items);

        Set<Long> seen = new HashSet<>();
        for (OrderItem item : morphium.createQueryFor(OrderItem.class).asList()) {
            assertThat(seen.add(item.getOrderNum()))
                    .as("duplicate orderNum: " + item.getOrderNum()).isTrue();
        }
    }

    @Test
    public void storeList_mixedNullAndExplicit_onlyNullAssigned() {
        List<OrderItem> items = new ArrayList<>();
        OrderItem withExplicit = new OrderItem();
        withExplicit.setOrderNum(999L);
        items.add(withExplicit);
        for (int i = 0; i < 3; i++) {
            items.add(new OrderItem());
        }
        morphium.storeList(items);

        List<OrderItem> loaded = morphium.createQueryFor(OrderItem.class)
                .sort("order_num").asList();
        assertThat(loaded).hasSize(4);
        // explicit value must be preserved
        assertThat(loaded.stream().mapToLong(OrderItem::getOrderNum).min().getAsLong()).isEqualTo(1L);
        assertThat(loaded.stream().anyMatch(o -> o.getOrderNum() == 999L)).isTrue();
        // auto-assigned values must not include 999
        long[] autoValues = loaded.stream()
                .mapToLong(OrderItem::getOrderNum)
                .filter(v -> v != 999L)
                .sorted()
                .toArray();
        assertThat(autoValues).containsExactly(1L, 2L, 3L);
    }

    @Test
    public void storeList_followedBySingleStore_sequenceContinues() {
        List<OrderItem> batch = new ArrayList<>();
        for (int i = 0; i < 5; i++) batch.add(new OrderItem());
        morphium.storeList(batch);

        OrderItem next = new OrderItem();
        morphium.store(next);

        long maxBatch = batch.stream().mapToLong(OrderItem::getOrderNum).max().getAsLong();
        assertThat(next.getOrderNum()).isGreaterThan(maxBatch);
    }

    // =========================================================================
    // Concurrency — no duplicates under parallel load
    // =========================================================================

    @Test
    public void concurrent_storeList_noOverlap() throws InterruptedException {
        final int threads   = 4;
        final int batchSize = 50;
        List<Long> allValues = new CopyOnWriteArrayList<>();

        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done  = new CountDownLatch(threads);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                    List<OrderItem> items = new ArrayList<>();
                    for (int i = 0; i < batchSize; i++) items.add(new OrderItem());
                    morphium.storeList(items);
                    for (OrderItem item : items) allValues.add(item.getOrderNum());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    done.countDown();
                }
            });
        }

        ready.await();
        start.countDown();
        assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
        pool.shutdown();

        assertThat(allValues).hasSize(threads * batchSize);
        Set<Long> unique = new HashSet<>(allValues);
        assertThat(unique).as("duplicate sequence values across threads").hasSize(threads * batchSize);
    }

    // =========================================================================
    // Performance — batch must be significantly faster than N individual stores
    // =========================================================================

    @Entity
    public static class PerfItemIndividual {
        @Id private MorphiumId id;
        @AutoSequence(name = "perf_seq_individual") private Long seqNum;
        public Long getSeqNum() { return seqNum; }
    }

    @Entity
    public static class PerfItemBatch {
        @Id private MorphiumId id;
        @AutoSequence(name = "perf_seq_batch") private Long seqNum;
        public Long getSeqNum() { return seqNum; }
    }

    @Test
    public void storeList_fasterThanNIndividualStores() {
        final int n = 100;

        // Warm up with a batch so SequenceGenerator + indices are initialised
        List<PerfItemBatch> warm = new ArrayList<>();
        for (int i = 0; i < 5; i++) warm.add(new PerfItemBatch());
        morphium.storeList(warm);

        // Measure N individual stores (each triggers one getNextValue() call)
        long tIndividual = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            morphium.store(new PerfItemIndividual());
        }
        tIndividual = System.currentTimeMillis() - tIndividual;

        // Measure one storeList (triggers a single getNextBatch(n) call)
        List<PerfItemBatch> batch = new ArrayList<>();
        for (int i = 0; i < n; i++) batch.add(new PerfItemBatch());
        long tBatch = System.currentTimeMillis();
        morphium.storeList(batch);
        tBatch = System.currentTimeMillis() - tBatch;

        // storeList should be at least 5× faster
        assertThat(tBatch)
                .as("storeList(%d) [%dms] should be at least 5× faster than %d individual stores [%dms]",
                        n, tBatch, n, tIndividual)
                .isLessThan(tIndividual / 5);
    }
}
