package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.morphium.driver.commands.MongoCommand;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;
import org.junit.jupiter.api.*;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that the static {@code KNOWN_COMMANDS} list in InMemoryDriver
 * is complete — i.e. it contains all concrete {@link MongoCommand} subclasses
 * (excluding abstract classes, interfaces, and {@link GenericCommand}).
 * <p>
 * This test uses ClassGraph (available in test scope) to discover all
 * MongoCommand subclasses and compares against the static list.
 */
@Tag("core")
class InMemoryDriverCommandListTest {

    @Test
    void knownCommands_containsAllConcreteMongoCommands() throws Exception {
        // Get the KNOWN_COMMANDS list via reflection
        Class<?> inMemClass = Class.forName("de.caluga.morphium.driver.inmem.InMemoryDriver");
        Field field = inMemClass.getDeclaredField("KNOWN_COMMANDS");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<Class<? extends MongoCommand>> knownCommands = (List<Class<? extends MongoCommand>>) field.get(null);
        Set<String> knownNames = knownCommands.stream()
            .map(Class::getName)
            .collect(Collectors.toSet());

        // Scan Morphium's command packages for all concrete MongoCommand subclasses
        Set<String> scannedNames = new HashSet<>();
        try (ScanResult scanResult = new ClassGraph()
            .acceptPackages("de.caluga.morphium.driver.commands")
            .enableClassInfo()
            .scan()) {
            for (ClassInfo info : scanResult.getSubclasses(MongoCommand.class.getName())) {
                Class<?> cls = Class.forName(info.getName());
                if (Modifier.isAbstract(cls.getModifiers()) || cls.isInterface()) {
                    continue;
                }
                if (cls.equals(GenericCommand.class)) {
                    continue;
                }
                scannedNames.add(cls.getName());
            }
        }

        // Every scanned command must be in KNOWN_COMMANDS
        Set<String> missing = new HashSet<>(scannedNames);
        missing.removeAll(knownNames);

        assertTrue(missing.isEmpty(),
            "KNOWN_COMMANDS in InMemoryDriver is missing these MongoCommand subclasses: " + missing);
    }
}
