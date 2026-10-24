package de.caluga.morphium.spring.autoconfigure;

import de.caluga.morphium.Morphium;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.pool.TypePool;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import static net.bytebuddy.matcher.ElementMatchers.isAnnotatedWith;
import static net.bytebuddy.matcher.ElementMatchers.named;

/**
 * MOR-026: runs the standalone entity tests of the Jakarta Data TCK 1.0.0 (jakarta.data-tck) against the repositories of this module, without Arquillian: the TCK classes are loaded
 * by {@link TckLoader}, which gives their entities the annotations of Morphium, and each test method runs on a fresh instance with repository proxies as fields.
 * <p>
 * Every test must pass except the ones in {@link #EXCLUDED}, each with its reason. A test that is excluded but passes fails the build, so the list cannot go stale.
 * Run it with {@code mvn -nsu test -Dtest=JakartaDataTckTest} in {@code spring-boot-morphium}; against a real MongoDB add
 * {@code -Dmorphium.driver-name=PooledDriver -Dmorphium.hosts=localhost:27017 -Dmorphium.database=tck}. {@code -Dtck.trace=<part of a test name>} prints the stack of a failure,
 * {@code -Dtck.reason.length=N} the length of the failure messages.
 */
@SpringBootTest(classes = TestApplication.class)
@ActiveProfiles("test")
class JakartaDataTckTest {

    @Autowired
    Morphium morphium;

    /** the tests of the TCK that are known not to pass, and why; see the ledger for the tasks */
    private static final java.util.Map<String, String> EXCLUDED = java.util.Map.of(
        "testOrderByHasPrecedenceOverSorts", "sorts an enum by ordinal; Morphium stores an enum by its name and sorts it alphabetically",
        "testOrderByHasPrecedenceOverPageRequestSorts", "sorts an enum by ordinal; Morphium stores an enum by its name and sorts it alphabetically",
        "testBasicRepository", "sorts an enum by ordinal; Morphium stores an enum by its name and sorts it alphabetically",
        "testLimitedRange", "sorts an enum by ordinal; Morphium stores an enum by its name and sorts it alphabetically",
        "testIgnoreCase", "MOR-092: IgnoreCase before Between, NotIn and the comparison operators is not supported");

    /** loads the classes of the TCK itself and gives its entities the annotations of Morphium */
    static class TckLoader extends ClassLoader {
        TckLoader(ClassLoader parent) {
            super(parent);
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (!name.startsWith("ee.jakarta.tck.data.")) {
                return super.loadClass(name, resolve);
            }

            synchronized (getClassLoadingLock(name)) {
                Class<?> loaded = findLoadedClass(name);

                if (loaded == null) {
                    try (InputStream in = getParent().getResourceAsStream(name.replace('.', '/') + ".class")) {
                        if (in == null) {
                            throw new ClassNotFoundException(name);
                        }

                        byte[] bytes = transform(name, in.readAllBytes());
                        loaded = defineClass(name, bytes, 0, bytes.length);
                    } catch (java.io.IOException e) {
                        throw new ClassNotFoundException(name, e);
                    }
                }

                return loaded;
            }
        }

        private byte[] transform(String name, byte[] bytes) {
            TypePool pool = TypePool.Default.of(getParent());
            TypeDescription type = pool.describe(name).resolve();
            boolean entity = type.getDeclaredAnnotations().stream().anyMatch(a -> a.getAnnotationType().getName().equals("jakarta.nosql.Entity")
                    || a.getAnnotationType().getName().equals("jakarta.persistence.Entity"));

            if (!entity) {
                return bytes;
            }

            DynamicType.Builder<?> builder = new ByteBuddy().redefine(type, ClassFileLocator.Simple.of(name, bytes))
                .annotateType(AnnotationDescription.Builder.ofType(de.caluga.morphium.annotations.Entity.class).define("translateCamelCase", false).build())
                .field(isAnnotatedWith(named("jakarta.nosql.Id").or(named("jakarta.persistence.Id"))))
                .annotateField(AnnotationDescription.Builder.ofType(de.caluga.morphium.annotations.Id.class).build());

            return builder.make().getBytes();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Object repository(Class<?> iface) throws Exception {
        MorphiumRepositoryFactoryBean factory = new MorphiumRepositoryFactoryBean(iface);
        Field f = MorphiumRepositoryFactoryBean.class.getDeclaredField("morphium");
        f.setAccessible(true);
        f.set(factory, morphium);
        return factory.getObject();
    }

    @Test
    void run() throws Exception {
        TckLoader loader = new TckLoader(getClass().getClassLoader());
        Class<?> tests = loader.loadClass("ee.jakarta.tck.data.standalone.entity.EntityTests");
        List<Method> methods = new ArrayList<>();
        List<Method> before = new ArrayList<>();

        for (Method m : tests.getDeclaredMethods()) {
            for (java.lang.annotation.Annotation a : m.getAnnotations()) {
                if (a.annotationType().getName().equals("org.junit.jupiter.api.Test") || a.annotationType().getName().equals("ee.jakarta.tck.data.framework.junit.anno.Assertion")) {
                    methods.add(m);
                } else if (a.annotationType().getName().equals("org.junit.jupiter.api.BeforeEach")) {
                    before.add(m);
                }
            }
        }

        methods.sort((a, b) -> a.getName().compareTo(b.getName()));
        TreeMap<String, String> results = new TreeMap<>();
        TreeMap<String, String> unsupported = new TreeMap<>();
        int passed = 0;

        for (Method test : methods) {
            try {
                Object instance = tests.getDeclaredConstructor().newInstance();

                for (Field field : tests.getDeclaredFields()) {
                    if (!Modifier.isStatic(field.getModifiers()) && field.getType().isInterface()) {
                        field.setAccessible(true);

                        try {
                            field.set(instance, repository(field.getType()));
                        } catch (IllegalArgumentException e) {
                            unsupported.put(field.getType().getSimpleName(), e.getMessage());
                        }
                    }
                }

                for (Method b : before) {
                    b.setAccessible(true);
                    b.invoke(instance);
                }

                test.setAccessible(true);
                test.invoke(instance);
                results.put(test.getName(), "PASS");
                passed++;
            } catch (InvocationTargetException e) {
                results.put(test.getName(), "FAIL " + e.getCause());

                // -Dtck.trace=<part of a test name> prints the stack of that failure and of its causes
                String trace = System.getProperty("tck.trace");

                if (trace != null && test.getName().contains(trace)) {
                    for (Throwable t = e.getCause(); t != null; t = t.getCause()) {
                        System.out.println("TCK-TRACE " + test.getName() + " " + t);

                        for (StackTraceElement frame : java.util.Arrays.stream(t.getStackTrace()).limit(14).toList()) {
                            System.out.println("TCK-TRACE     at " + frame);
                        }
                    }
                }
            } catch (Throwable e) {
                results.put(test.getName(), "ERROR " + e);
            }
        }

        unsupported.forEach((k, v) -> System.out.println("TCK no repository for " + k + ": " + v));
        System.out.println("TCK " + passed + " of " + methods.size() + " passed");
        java.util.Map<String, List<String>> byReason = new TreeMap<>();
        results.forEach((k, v) -> byReason.computeIfAbsent(v.length() > Integer.getInteger("tck.reason.length", 160) ? v.substring(0, Integer.getInteger("tck.reason.length", 160)) : v, x -> new ArrayList<>()).add(k));
        byReason.forEach((reason, names) -> System.out.println("TCK [" + names.size() + "] " + reason + " :: " + (names.size() > 6 ? names.subList(0, 6) + "..." : names)));

        List<String> unexpectedFailures = new ArrayList<>();
        List<String> unexpectedPasses = new ArrayList<>();

        results.forEach((name, result) -> {
            boolean passes = result.equals("PASS");

            if (!passes && !EXCLUDED.containsKey(name)) {
                unexpectedFailures.add(name + ": " + result);
            } else if (passes && EXCLUDED.containsKey(name)) {
                unexpectedPasses.add(name);
            }
        });

        org.junit.jupiter.api.Assertions.assertTrue(unexpectedFailures.isEmpty(), "TCK tests that fail and are not excluded: " + unexpectedFailures);
        org.junit.jupiter.api.Assertions.assertTrue(unexpectedPasses.isEmpty(), "TCK tests that are excluded but pass, remove them from EXCLUDED: " + unexpectedPasses);
    }
}
