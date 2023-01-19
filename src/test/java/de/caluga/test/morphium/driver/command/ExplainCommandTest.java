
package de.caluga.test.morphium.driver.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.ExplainCommand;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.ExplainCommand.ExplainVerbosity;

public class ExplainCommandTest {
    @Test
    public void asMapFindCommandTest() {
        ExplainCommand cmd = new ExplainCommand(null);
        FindCommand find = new FindCommand(null).setDb("testdb").setColl("testcoll")
         .setComment("My comment").setSkip(10).setLimit(10).setFilter(Doc.of("field1", "value1", "field2", 42))
         .setSort(Doc.of("field1", 1));
        cmd.setCommand(find.asMap());
        cmd.setVerbosity(ExplainVerbosity.queryPlanner);
        var map = cmd.asMap();
        LoggerFactory.getLogger(ExplainCommandTest.class).info(Utils.toJsonString(map));
        assertTrue(map.get("explain") instanceof Map);
        assertEquals("queryPlanner", map.get("verbosity"));
    }
}
