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

/** A SPIKE for MOR-026: runs the standalone entity tests of the Jakarta Data TCK without Arquillian and prints what passes. */
@SpringBootTest(classes = TestApplication.class)
@ActiveProfiles("test")
class JakartaDataTckSpikeTest {

    @Autowired
    Morphium morphium;

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
            } catch (Throwable e) {
                results.put(test.getName(), "ERROR " + e);
            }
        }

        unsupported.forEach((k, v) -> System.out.println("TCK-SPIKE no repository for " + k + ": " + v));
        System.out.println("TCK-SPIKE " + passed + " of " + methods.size() + " passed");
        java.util.Map<String, List<String>> byReason = new TreeMap<>();
        results.forEach((k, v) -> byReason.computeIfAbsent(v.length() > 160 ? v.substring(0, 160) : v, x -> new ArrayList<>()).add(k));
        byReason.forEach((reason, names) -> System.out.println("TCK-SPIKE [" + names.size() + "] " + reason + " :: " + (names.size() > 6 ? names.subList(0, 6) + "..." : names)));
    }
}
