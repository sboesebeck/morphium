package de.caluga.test.morphium.driver.command;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.test.morphium.driver.DriverTestBase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class ListCollectionsCommandTest extends DriverTestBase {
    private Logger log = LoggerFactory.getLogger(ListCollectionsCommandTest.class);


    @Test
    public void testListCollections() throws Exception {
        var con = getConnection();
        con.connect();

        new InsertMongoCommand(con.getConnection())
                .setDb(db)
                .setColl(coll)
                .setDocuments(Arrays.asList(Doc.of("str", "string", "value", 123)))
                .execute();
        new InsertMongoCommand(con.getConnection())
                .setDb(db)
                .setColl(coll + "_2")
                .setDocuments(Arrays.asList(Doc.of("str", "string", "value", 123)))
                .execute();
        new InsertMongoCommand(con.getConnection())
                .setDb(db)
                .setColl(coll + "_3")
                .setDocuments(Arrays.asList(Doc.of("str", "string", "value", 123)))
                .execute();

        var lst = con.listCollections(db, null);
        assertThat(lst).isNotNull();
        assertThat(lst.size()).isEqualTo(3);

        con.disconnect();
    }
}
