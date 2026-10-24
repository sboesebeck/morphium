package de.caluga.morphium.data;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Property;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import jakarta.data.metamodel.SortableAttribute;
import jakarta.data.metamodel.TextAttribute;
import jakarta.data.metamodel.impl.SortableAttributeRecord;
import jakarta.data.metamodel.impl.TextAttributeRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * MOR-012: the {@code name()} of a Jakarta Data static metamodel attribute is the Java name of the field, and the update builder resolves it to the stored name like it does
 * for a String or an enum constant (a {@code @Property} name, camel case translation).
 */
class MetamodelAttributeFieldReferenceTest {

    private static Morphium morphium;

    @Entity
    static class Product {
        @Id
        String id;
        @Property(fieldName = "lbl")
        String label;
        int stockCount;
    }

    /** what a generated {@code _Product} class holds with API 1.0: the records of {@code jakarta.data.metamodel.impl}, constructed with the Java name of the field */
    static final class _Product {
        static final TextAttribute<Product> label = new TextAttributeRecord<>("label");
        static final SortableAttribute<Product> stockCount = new SortableAttributeRecord<>("stockCount");
    }

    @BeforeAll
    static void setUp() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setDatabase("metamodel_attribute_test");
        cfg.addHostToSeed("localhost");
        cfg.setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg);
    }

    @AfterAll
    static void tearDown() {
        morphium.close();
    }

    @Test
    @DisplayName("the name of a metamodel attribute resolves to the stored name, like the String and the enum spelling")
    void attributeNameIsAFieldReference() {
        morphium.dropCollection(Product.class);
        Product p = new Product();
        p.id = "p";
        p.label = "old";
        p.stockCount = 1;
        morphium.store(p);

        morphium.createQueryFor(Product.class).f("id").eq("p").modify().set(_Product.label.name(), "new").inc(_Product.stockCount.name(), 4).updateOne();

        Product read = morphium.createQueryFor(Product.class).f("id").eq("p").get();
        assertThat(read.label).isEqualTo("new");
        assertThat(read.stockCount).isEqualTo(5);
        assertThat(morphium.createQueryFor(Product.class).f(_Product.label.name()).eq("new").countAll()).as("the filter takes the same spelling").isEqualTo(1);
    }
}
