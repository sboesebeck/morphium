package de.caluga.test.mongo.suite.base;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Reference;
import de.caluga.morphium.annotations.Transient;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MOR-014 (option E of the research): an {@code @Embedded} class with the default {@code translateCamelCase=true} that sits in a class with {@code translateCamelCase=false} is stored
 * in snake_case, while the owner keeps its camelCase keys. It works and reads back, so nobody notices until a raw query uses the wrong name. The mapper warns once per class, naming
 * both classes and the field; {@code ObjectMappingSettings.strictEmbeddedNaming} turns the warning into an {@link IllegalStateException}. The stored data is not changed.
 */
@Tag("core")
public class EmbeddedNamingGuardTest extends MultiDriverTestBase {

    @Embedded
    public static class Translating {
        public String fileName;
    }

    @Embedded(translateCamelCase = false)
    public static class Keeping {
        public String fileName;
    }

    /** a non translating embedded that holds a translating one */
    @Embedded(translateCamelCase = false)
    public static class KeepingOwner {
        public Translating inner;
    }

    @Entity(translateCamelCase = false)
    public static class OwnerWithTranslatingField {
        @Id
        public String id;
        public String ownerName;
        public Translating child;
    }

    @Entity(translateCamelCase = false)
    public static class OwnerWithKeepingField {
        @Id
        public String id;
        public Keeping child;
        public List<Keeping> children;
    }

    @Entity
    public static class TranslatingOwner {
        @Id
        public String id;
        public Translating child;
        public Keeping other;
    }

    @Entity(translateCamelCase = false)
    public static class OwnerWithContainers {
        @Id
        public String id;
        public List<Translating> list;
        public Map<String, Translating> map;
        public Translating[] array;
    }

    @Entity(translateCamelCase = false)
    public static class OwnerWithNestedMismatch {
        @Id
        public String id;
        public KeepingOwner nested;
    }

    @Entity(translateCamelCase = false)
    public static class OwnerWithIgnoredFields {
        @Id
        public String id;
        @Transient
        public Translating notStored;
        public transient Translating javaTransient;
        @Reference
        public OwnerWithKeepingField reference;
    }

    private List<ILoggingEvent> warningsOf(Runnable body) {
        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ObjectMapperImpl.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.list = new java.util.concurrent.CopyOnWriteArrayList<>();
        appender.start();
        logger.addAppender(appender);

        try {
            body.run();
            return appender.list.stream().filter(e -> e.getLevel() == Level.WARN && e.getFormattedMessage().contains("translateCamelCase")).toList();
        } finally {
            logger.detachAppender(appender);
        }
    }

    private static String message(List<ILoggingEvent> warnings) {
        return warnings.get(0).getFormattedMessage();
    }

    // ------------------------------------------------------------------ the analysis

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theAnalysisNamesOwnerFieldAndEmbedded(Morphium m) throws Exception {
        try (m) {
            AnnotationAndReflectionHelper helper = m.getARHelper();
            var found = helper.findEmbeddedNamingMismatches(OwnerWithTranslatingField.class);
            assertEquals(1, found.size(), String.valueOf(found));
            assertEquals(OwnerWithTranslatingField.class, found.get(0).owner());
            assertEquals("child", found.get(0).field());
            assertEquals(Translating.class, found.get(0).embedded());
            assertTrue(helper.findEmbeddedNamingMismatches(OwnerWithKeepingField.class).isEmpty(), "an embedded with its own translateCamelCase=false");
            assertTrue(helper.findEmbeddedNamingMismatches(TranslatingOwner.class).isEmpty(), "a translating owner");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void containersAndNestingAreWalked(Morphium m) throws Exception {
        try (m) {
            var containers = m.getARHelper().findEmbeddedNamingMismatches(OwnerWithContainers.class);
            assertEquals(List.of("array", "list", "map"), containers.stream().map(x -> x.field()).sorted().toList(), String.valueOf(containers));
            var nested = m.getARHelper().findEmbeddedNamingMismatches(OwnerWithNestedMismatch.class);
            assertEquals(1, nested.size(), String.valueOf(nested));
            assertEquals(KeepingOwner.class, nested.get(0).owner(), "the owner of the mismatch is the embedded that does not translate");
            assertEquals("inner", nested.get(0).field());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void fieldsThatAreNotStoredAsEmbeddedAreIgnored(Morphium m) throws Exception {
        try (m) {
            assertTrue(m.getARHelper().findEmbeddedNamingMismatches(OwnerWithIgnoredFields.class).isEmpty());
        }
    }

    // ------------------------------------------------------------------ the warning

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aMismatchIsWarnedAboutOnceNamingBothClasses(Morphium m) throws Exception {
        try (m) {
            // a fresh mapper: Morphium has already looked at the entity classes of the classpath when it started, and warned then
            ObjectMapperImpl fresh = new ObjectMapperImpl();
            fresh.setMorphium(m);
            Supplier<String> use = () -> fresh.getCollectionName(OwnerWithTranslatingField.class);
            List<ILoggingEvent> first = warningsOf(use::get);
            assertEquals(1, first.size(), first.toString());
            String text = message(first);
            assertTrue(text.contains(OwnerWithTranslatingField.class.getName()), text);
            assertTrue(text.contains(Translating.class.getName()), text);
            assertTrue(text.contains("child"), text);
            assertEquals(0, warningsOf(use::get).size(), "once per class");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void noWarningWhereNothingIsWrong(Morphium m) throws Exception {
        try (m) {
            ObjectMapperImpl fresh = new ObjectMapperImpl();
            fresh.setMorphium(m);
            List<ILoggingEvent> warnings = warningsOf(() -> {
                fresh.getCollectionName(OwnerWithKeepingField.class);
                fresh.getCollectionName(TranslatingOwner.class);
                fresh.getCollectionName(OwnerWithIgnoredFields.class);
            });
            assertEquals(0, warnings.size(), warnings.toString());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void storingStillWorksAndTheStoredKeysAreUnchanged(Morphium m) throws Exception {
        try (m) {
            m.dropCollection(OwnerWithTranslatingField.class);
            OwnerWithTranslatingField o = new OwnerWithTranslatingField();
            o.id = "x";
            o.ownerName = "n";
            o.child = new Translating();
            o.child.fileName = "f.txt";
            m.store(o);
            Map<String, Object> raw = m.createQueryFor(OwnerWithTranslatingField.class).asMapList().get(0);
            assertEquals("n", raw.get("ownerName"), "the owner keeps its names");
            assertEquals("f.txt", ((Map<?, ?>) raw.get("child")).get("file_name"), "the embedded is stored as before");
            assertEquals("f.txt", m.createQueryFor(OwnerWithTranslatingField.class).get().child.fileName);
        }
    }

    // ------------------------------------------------------------------ strict mode

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void strictModeRefusesTheEntityWithTheSameMessage(Morphium m) throws Exception {
        try (m) {
            assertEquals(false, m.getConfig().objectMappingSettings().isStrictEmbeddedNaming(), "off by default");
            m.getConfig().objectMappingSettings().setStrictEmbeddedNaming(true);

            try {
                IllegalStateException e = assertThrows(IllegalStateException.class, () -> m.getMapper().getCollectionName(OwnerWithTranslatingField.class));
                assertTrue(e.getMessage().contains(OwnerWithTranslatingField.class.getName()) && e.getMessage().contains(Translating.class.getName()), e.getMessage());
                m.getMapper().getCollectionName(OwnerWithKeepingField.class); // no mismatch: no exception
            } finally {
                m.getConfig().objectMappingSettings().setStrictEmbeddedNaming(false);
            }
        }
    }
}
