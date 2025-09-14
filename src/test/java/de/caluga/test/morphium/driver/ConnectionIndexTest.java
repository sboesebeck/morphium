package de.caluga.test.morphium.driver;

import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.CreateIndexesCommand;
import de.caluga.morphium.driver.commands.DropMongoCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.ListIndexesCommand;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


@Tag("driver")
public class ConnectionIndexTest extends ConnectionTestBase {
    private Logger log = LoggerFactory.getLogger(ConnectionIndexTest.class);

    @Test
    @Disabled
    public void listIndexTests() throws Exception {
        var con = getConnection();
        // DropMongoCommand dc = new DropMongoCommand(con).setDb(db).setColl(coll);
        // var drp = dc.execute();
        // log.info(Utils.toJsonString(drp));
        InsertMongoCommand cmd = new InsertMongoCommand(con).setDb(db).setColl(coll)
                .setDocuments(Arrays.asList(Doc.of("counter", 123, "str_val", "This is a test")));
        cmd.execute();
        Thread.sleep(1000);
        CreateIndexesCommand cic = new CreateIndexesCommand(con).addIndex(new IndexDescription().setKey(Doc.of("counter", 1)).setName("tst1").setSparse(true)).setDb(db).setColl(coll);
        //Created collection
        var res = cic.execute();
        log.info(Utils.toJsonString(res));
        ListIndexesCommand lcmd = new ListIndexesCommand(con).setDb(db).setColl(coll);
        List<IndexDescription> lst = lcmd.execute();
        for (IndexDescription idx : lst) {
            log.info("Index: " + idx.toString());
            assertThat(idx.getName()).isIn("_id_", "tst1");
        }
        assertEquals(2, lst.size());

        //adding another index

        IndexDescription id = new IndexDescription().setKey(Doc.of("counter", -1, "str_value", 1)).setName("MyIdx");
        IndexDescription id2 = new IndexDescription().setKey(Doc.of("timestamp", 1)).setExpireAfterSeconds(10).setName("ts_exp");
        cic = new CreateIndexesCommand(con).setDb(db).setColl(coll)
                .addIndex(id).addIndex(id2);
        var ret = cic.execute();
        log.info(Utils.toJsonString(ret));
        lcmd = new ListIndexesCommand(con).setDb(db).setColl(coll);
        lst = lcmd.execute();
        for (IndexDescription idx : lst) {
            log.info("Index: " + idx.toString());
            assertThat(idx.getName()).isIn("_id_", "tst1", "MyIdx", "ts_exp");
            if (idx.getName().equals("ts_exp")) {
                assertNotNull(idx.getExpireAfterSeconds());
                assertEquals(10, idx.getExpireAfterSeconds());
            }
        }
        assertEquals(4, lst.size());
        con.close();
    }
}
