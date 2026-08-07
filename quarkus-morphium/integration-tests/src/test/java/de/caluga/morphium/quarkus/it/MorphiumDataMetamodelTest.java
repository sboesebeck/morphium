package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.Morphium;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.data.Sort;
import jakarta.data.Order;
import jakarta.data.metamodel.Attribute;
import jakarta.data.metamodel.SortableAttribute;
import jakarta.data.metamodel.StaticMetamodel;
import jakarta.data.metamodel.TextAttribute;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Jakarta Data Phase 6: @StaticMetamodel generation.
 */
@QuarkusTest
@DisplayName("Jakarta Data @StaticMetamodel Generation")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MorphiumDataMetamodelTest {

    @Inject
    OrderRepository repository;

    @Inject
    Morphium morphium;

    @BeforeEach
    void setUp() {
        morphium.clearCollection(OrderEntity.class);

        createOrder("C1", 100.0, "OPEN");
        createOrder("C2", 250.0, "OPEN");
        createOrder("C3", 50.0, "CLOSED");
        createOrder("C4", 300.0, "CLOSED");
        createOrder("C5", 150.0, "PENDING");
    }

    @AfterEach
    void resetThreadLocals() {
        morphium.resetThreadLocalOverrides();
    }

    // -- Metamodel class existence --

    @Test
    @org.junit.jupiter.api.Order(1)
    @DisplayName("OrderEntity_ metamodel class exists and is annotated")
    void metamodelClassExists() throws Exception {
        Class<?> metamodel = Class.forName("de.caluga.morphium.quarkus.it.OrderEntity_");
        assertThat(metamodel).isNotNull();

        StaticMetamodel annotation = metamodel.getAnnotation(StaticMetamodel.class);
        assertThat(annotation).isNotNull();
        assertThat(annotation.value()).isEqualTo(OrderEntity.class);
    }

    @Test
    @org.junit.jupiter.api.Order(2)
    @DisplayName("ItemEntity_ metamodel class exists and is annotated")
    void itemMetamodelClassExists() throws Exception {
        Class<?> metamodel = Class.forName("de.caluga.morphium.quarkus.it.ItemEntity_");
        assertThat(metamodel).isNotNull();

        StaticMetamodel annotation = metamodel.getAnnotation(StaticMetamodel.class);
        assertThat(annotation).isNotNull();
        assertThat(annotation.value()).isEqualTo(ItemEntity.class);
    }

    // -- String constants --

    @Test
    @org.junit.jupiter.api.Order(3)
    @DisplayName("OrderEntity_ has String constants for all fields")
    void stringConstants() throws Exception {
        Class<?> m = Class.forName("de.caluga.morphium.quarkus.it.OrderEntity_");

        // Check String constants exist and have correct values
        assertStringConstant(m, "ID", "id");
        assertStringConstant(m, "CUSTOMER_ID", "customerId");
        assertStringConstant(m, "AMOUNT", "amount");
        assertStringConstant(m, "STATUS", "status");
        assertStringConstant(m, "CREATED_AT", "createdAt");
        assertStringConstant(m, "VERSION", "version");
    }

    // -- Attribute types --

    @Test
    @org.junit.jupiter.api.Order(4)
    @DisplayName("OrderEntity_ String fields are TextAttribute")
    void textAttributes() throws Exception {
        Class<?> m = Class.forName("de.caluga.morphium.quarkus.it.OrderEntity_");

        Object statusAttr = m.getField("status").get(null);
        assertThat(statusAttr).isInstanceOf(TextAttribute.class);
        assertThat(((Attribute<?>) statusAttr).name()).isEqualTo("status");

        Object customerIdAttr = m.getField("customerId").get(null);
        assertThat(customerIdAttr).isInstanceOf(TextAttribute.class);
    }

    @Test
    @org.junit.jupiter.api.Order(5)
    @DisplayName("OrderEntity_ numeric fields are SortableAttribute")
    void sortableAttributes() throws Exception {
        Class<?> m = Class.forName("de.caluga.morphium.quarkus.it.OrderEntity_");

        Object amountAttr = m.getField("amount").get(null);
        assertThat(amountAttr).isInstanceOf(SortableAttribute.class);
        assertThat(((Attribute<?>) amountAttr).name()).isEqualTo("amount");
    }

    @Test
    @org.junit.jupiter.api.Order(6)
    @DisplayName("OrderEntity_ date fields are SortableAttribute")
    void dateAttributes() throws Exception {
        Class<?> m = Class.forName("de.caluga.morphium.quarkus.it.OrderEntity_");

        Object createdAtAttr = m.getField("createdAt").get(null);
        assertThat(createdAtAttr).isInstanceOf(SortableAttribute.class);
        assertThat(((Attribute<?>) createdAtAttr).name()).isEqualTo("createdAt");
    }

    // -- Attribute functional usage --

    @Test
    @org.junit.jupiter.api.Order(7)
    @DisplayName("TextAttribute.asc() creates correct Sort")
    void textAttributeAsc() throws Exception {
        Class<?> m = Class.forName("de.caluga.morphium.quarkus.it.OrderEntity_");
        @SuppressWarnings("unchecked")
        TextAttribute<OrderEntity> statusAttr =
                (TextAttribute<OrderEntity>) m.getField("status").get(null);

        Sort<OrderEntity> sort = statusAttr.asc();
        assertThat(sort.property()).isEqualTo("status");
        assertThat(sort.isAscending()).isTrue();
        assertThat(sort.ignoreCase()).isFalse();
    }

    @Test
    @org.junit.jupiter.api.Order(8)
    @DisplayName("SortableAttribute.desc() creates correct Sort")
    void sortableAttributeDesc() throws Exception {
        Class<?> m = Class.forName("de.caluga.morphium.quarkus.it.OrderEntity_");
        @SuppressWarnings("unchecked")
        SortableAttribute<OrderEntity> amountAttr =
                (SortableAttribute<OrderEntity>) m.getField("amount").get(null);

        Sort<OrderEntity> sort = amountAttr.desc();
        assertThat(sort.property()).isEqualTo("amount");
        assertThat(sort.isDescending()).isTrue();
    }

    @Test
    @org.junit.jupiter.api.Order(9)
    @DisplayName("TextAttribute.ascIgnoreCase() creates correct Sort")
    void textAttributeAscIgnoreCase() throws Exception {
        Class<?> m = Class.forName("de.caluga.morphium.quarkus.it.OrderEntity_");
        @SuppressWarnings("unchecked")
        TextAttribute<OrderEntity> statusAttr =
                (TextAttribute<OrderEntity>) m.getField("status").get(null);

        Sort<OrderEntity> sort = statusAttr.ascIgnoreCase();
        assertThat(sort.property()).isEqualTo("status");
        assertThat(sort.isAscending()).isTrue();
        assertThat(sort.ignoreCase()).isTrue();
    }

    // -- Practical usage with repository --

    @Test
    @org.junit.jupiter.api.Order(10)
    @DisplayName("Metamodel attributes can be used to build Order for findAll")
    void metamodelWithRepository() throws Exception {
        Class<?> m = Class.forName("de.caluga.morphium.quarkus.it.OrderEntity_");
        @SuppressWarnings("unchecked")
        SortableAttribute<OrderEntity> amountAttr =
                (SortableAttribute<OrderEntity>) m.getField("amount").get(null);

        // Use metamodel attribute to create Sort, then wrap in Order
        Sort<OrderEntity> sortByAmount = amountAttr.desc();
        Order<OrderEntity> order = Order.by(sortByAmount);

        var page = repository.findAll(
                jakarta.data.page.PageRequest.ofSize(3),
                order);

        assertThat(page.content()).hasSize(3);
        // Should be sorted by amount DESC: 300, 250, 150
        assertThat(page.content().get(0).getAmount()).isEqualTo(300.0);
        assertThat(page.content().get(1).getAmount()).isEqualTo(250.0);
        assertThat(page.content().get(2).getAmount()).isEqualTo(150.0);
    }

    // -- Helpers --

    private void assertStringConstant(Class<?> metamodel, String constantName, String expectedValue)
            throws Exception {
        Field field = metamodel.getField(constantName);
        assertThat(field).isNotNull();
        assertThat(Modifier.isStatic(field.getModifiers())).isTrue();
        assertThat(Modifier.isFinal(field.getModifiers())).isTrue();
        assertThat(field.getType()).isEqualTo(String.class);
        assertThat(field.get(null)).isEqualTo(expectedValue);
    }

    private void createOrder(String customerId, double amount, String status) {
        var order = new OrderEntity();
        order.setCustomerId(customerId);
        order.setAmount(amount);
        order.setStatus(status);
        morphium.store(order);
    }
}
