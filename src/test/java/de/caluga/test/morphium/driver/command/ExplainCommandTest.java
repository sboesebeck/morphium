
package de.caluga.test.morphium.driver.command;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Utils;
import de.caluga.morphium.annotations.Transient;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.commands.ExplainCommand;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.MongoCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wire.SingleMongoConnection;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import org.junit.jupiter.api.Test;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExplainCommandTest {
    @Test
    public void asMapFindCommandTest(){
        ExplainCommand cmd=new ExplainCommand(null);

        FindCommand find=new FindCommand(null).setDb("testdb").setColl("testcoll")
        .setComment("My comment").setSkip(10).setLimit(10).setFilter(Doc.of("field1","value1","field2",42))
        .setSort(Doc.of("field1",1));
        cmd.setCommand(find.asMap());


        var map=cmd.asMap();
        LoggerFactory.getLogger(ExplainCommandTest.class).info(Utils.toJsonString(map));
        assertTrue(map.get("explain") instanceof Map);


    }
}
