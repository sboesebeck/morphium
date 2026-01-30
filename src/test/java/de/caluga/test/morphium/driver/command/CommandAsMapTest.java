package de.caluga.test.morphium.driver.command;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Modifier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.morphium.driver.commands.HelloCommand;
import de.caluga.morphium.driver.commands.MongoCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.test.ConnectionMock;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;

@Tag("driver")
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
            log.debug("Found " + entities.size() + " MongoCommands in classpath");
            for (String cn : entities.getNames()) {
                if (cn.equals("de.caluga.morphium.driver.inmem.InMemAggregator$1" )) {
                    //inner class, need to skip
                    continue;
                }
                log.debug("Class -> " + cn);
                try {
                    Class <? extends MongoCommand > cls = (Class <? extends MongoCommand > ) Class.forName(cn);

                    if (Modifier.isAbstract(cls.getModifiers())) continue;
                    MongoCommand cmd = cls.getConstructor(MongoConnection.class).newInstance(new ConnectionMock());
                    cmd.setColl("testcoll").setDb("testDB").setMetaData("test", true);
                    var m = cmd.asMap();
                    assertFalse(m.containsKey("test"));
                    assertTrue(m.containsKey("$db"));
                    assertThat(m.get("$db")).isIn("testDB", "local", "admin");
                    assertThat(m).containsKey(cmd.getCommandName());
                } catch (Exception e) {
                    log.error("Exception for " + cn);
                    throw (e);
                }

            }
        }
    }

    @Test
    public void genericCommandTest() throws Exception {
        GenericCommand cmd = new GenericCommand(null).setCmdData(Doc.of("helllo", 1, "helloOk", true, "loadBalanced", false));
        var m = cmd.asMap();
        HelloCommand hc = new HelloCommand(null);
        hc.fromMap(m);
        assertTrue(hc.getHelloOk());
        // In hello, loadBalanced is optional/driver-dependent and can legitimately be false.
        // This test is about map roundtrip, not about forcing loadBalanced=true.
        assertFalse(hc.getLoadBalanced());



    }
}
