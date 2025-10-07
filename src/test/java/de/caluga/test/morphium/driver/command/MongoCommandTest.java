package de.caluga.test.morphium.driver.command;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.annotations.Transient;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.commands.MongoCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wire.SingleMongoConnection;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Tag("driver")
public class MongoCommandTest {
    private Logger log = LoggerFactory.getLogger(MongoCommandTest.class);

    @Test
    public void testMongoCommands() throws Exception{
        AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(false);
        Map < String, Class <? extends MongoCommand>> commandsByName = new HashMap<>();
        try (ScanResult scanResult =
                    new ClassGraph()
            //                     .verbose()             // Enable verbose logging
            .enableAnnotationInfo()
//                             .enableAllInfo()       // Scan classes, methods, fields, annotations
            .scan()) {
            ClassInfoList entities =
            scanResult.getSubclasses(MongoCommand.class.getName());
            for (ClassInfo info : entities) {
                String n = info.getName();
                Class cls = Class.forName(n);
                if (Modifier.isAbstract(cls.getModifiers())) continue;
                if (cls.isInterface()) continue;
                MongoCommand<MongoCommand> cmd = null;

                Constructor declaredConstructor = cls.getDeclaredConstructor(MongoConnection.class);
                declaredConstructor.setAccessible(true);
                cmd = (MongoCommand<MongoCommand>) declaredConstructor.newInstance(new SingleMongoConnection());

                log.info("Processing " + cls.getName() + "  command " + cmd.getCommandName());
                commandsByName.put(cmd.getCommandName(), cls);
                //generating test-data
                for (Field f : an.getAllFields(cls)) {
                    if (f.getAnnotation(Transient.class) != null) continue;
                    if (Modifier.isPublic(f.getModifiers()))continue;
                    f.setAccessible(true);
                    if (Integer.class.equals(f.getType()) || Long.class.equals(f.getType()) || Float.class.equals(f.getType()) || Double.class.equals(f.getType()) ||
                            int.class.equals(f.getType()) || long.class.equals(f.getType()) || float.class.equals(f.getType()) || double.class.equals(f.getType())
                       ) {
                        f.set(cmd, 17);
                    } else if (ReadPreference.class.equals(f.getType())) {
                        f.set(cmd, ReadPreference.nearest());
                    } else if (List.class.equals(f.getType())) {
                        Arrays.asList("This", "is", "a", "test");
                    } else if (Object.class.equals(f.getType())) {
                        f.set(cmd, "Object");
                    } else if (Map.class.equals(f.getType()) || Doc.class.equals(f.getType())) {
                        f.set(cmd, Doc.of("test", "value"));
                    } else if (Boolean.class.equals(f.getType()) || boolean.class.equals(f.getType())) {
                        f.set(cmd, Boolean.TRUE);
                    } else if (String.class.equals(f.getType())) {
                        f.set(cmd, "Hello World");
                    } else if (f.getType().isEnum()) {
                        Method method = f.getType().getDeclaredMethod("values");
                        Object obj = method.invoke(null);
                        Object[] values = (Object[])obj;
                        f.set(cmd, values[0]);
                    } else {
                        log.error("Unhandled value type: " + f.getType().getName());
                    }
                }
                long start = System.currentTimeMillis();
                var m = cmd.asMap();
                long dur = System.currentTimeMillis() - start;
                log.info("... creating Map took " + dur + "ms");
                var commandName = m.keySet().toArray(new String[1])[0];
                assertEquals(commandName, cmd.getCommandName());
                declaredConstructor = cls.getDeclaredConstructor(MongoConnection.class);
                declaredConstructor.setAccessible(true);
                var cmd2 = (MongoCommand<MongoCommand>) declaredConstructor.newInstance(new SingleMongoConnection());
                start = System.currentTimeMillis();
                cmd2.fromMap(m);
                dur = System.currentTimeMillis() - start;
                log.info("... reading map took " + dur + "ms");
                Map<String, Object> m2 = cmd2.asMap();
                commandName = m2.keySet().toArray(new String[1])[0];
                assertEquals(commandName, cmd2.getCommandName());
                for (var e : m.entrySet()) {
                    if (e.getKey() == null) continue;
                    if (e.getKey().equals("freeStorage")) continue;
                    assertEquals( e.getValue(), m2.get(e.getKey()), "Value differs: key = " + e.getKey());
                }

            }
        }
    }
}
