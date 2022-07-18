package de.caluga.test.morphium.driver.command;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.commands.MongoCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wire.SingleMongoConnection;
import de.caluga.test.DriverMock;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;

import static org.assertj.core.api.Assertions.assertThat;

public class CommandAsMapTest {
    private Logger log = LoggerFactory.getLogger(CommandAsMapTest.class);

    @Test
    public void asMapAllCommandsTest() throws Exception {
        try (ScanResult scanResult =
                     new ClassGraph()
                             //                     .verbose()             // Enable verbose logging
                             //.enableAnnotationInfo()
                             .enableAllInfo()       // Scan classes, methods, fields, annotations
                             .scan()) {
            ClassInfoList entities =
                    scanResult.getSubclasses(MongoCommand.class.getName());
            //entities.addAll(scanResult.getClassesWithAnnotation(Embedded.class.getName()));
            log.info("Found " + entities.size() + " MongoCommands in classpath");
            for (String cn : entities.getNames()) {
                log.info("Class -> " + cn);
                try {
                    Class<? extends MongoCommand> cls = (Class<? extends MongoCommand>) Class.forName(cn);
                    if (Modifier.isAbstract(cls.getModifiers())) continue;
                    MongoCommand cmd = cls.getConstructor(MongoConnection.class).newInstance(new SingleMongoConnection().setDriver(new DriverMock()));
                    cmd.setColl("testcoll").setDb("testDB").setMetaData("test", true);
                    var m = cmd.asMap();
                    assertThat(m).doesNotContainKey("test");
                    assertThat(m).containsKey("$db");
                    assertThat(m.get("$db")).isIn("testDB", "local", "admin");
                    assertThat(m).containsKey(cmd.getCommandName());
                } catch (Exception e) {
                    log.error("Exception for " + cn);
                    throw (e);
                }

            }
        }
    }

}