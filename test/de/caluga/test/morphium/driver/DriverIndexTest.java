package de.caluga.test.morphium.driver;

import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.CreateIndexesCommand;
import de.caluga.morphium.driver.commands.DropMongoCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.ListIndexesCommand;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class DriverIndexTest extends DriverTestBase {
    private Logger log = LoggerFactory.getLogger(DriverIndexTest.class);

    @Test
    public void listIndexTests() throws Exception {
        var con = getConnection();
        con.connect();
        DropMongoCommand dc = new DropMongoCommand(con).setDb(db).setColl(coll);
        var drp = dc.execute();
        log.info(Utils.toJsonString(drp));
        InsertMongoCommand cmd = new InsertMongoCommand(con).setDb(db).setColl(coll)
                .setDocuments(Arrays.asList(Doc.of("counter", 123, "str_val", "This is a test")));
        cmd.execute();
        //Created collection
        var res = con.runCommand(db, Doc.of("createIndexes", coll, "indexes", Arrays.asList(Doc.of("key", Doc.of("counter", 1), "name", "tst1", "sparse", true)))).getCursor();
        log.info(Utils.toJsonString(res.next()));
        ListIndexesCommand lcmd = new ListIndexesCommand(con).setDb(db).setColl(coll);
        List<IndexDescription> lst = lcmd.execute();
        for (IndexDescription idx : lst) {
            log.info("Index: " + idx.toString());
            assertThat(idx.getName()).isIn("_id_", "tst1");
        }
        assertThat(lst.size()).isEqualTo(2);

        //adding another index

        IndexDescription id = new IndexDescription().setKey(Doc.of("counter", -1, "str_value", 1)).setName("MyIdx");
        IndexDescription id2 = new IndexDescription().setKey(Doc.of("timestamp", 1)).setExpireAfterSeconds(10).setName("ts_exp");
        CreateIndexesCommand cic = new CreateIndexesCommand(con).setDb(db).setColl(coll)
                .addIndex(id).addIndex(id2);
        var ret = cic.execute();
        log.info(Utils.toJsonString(ret));
        lcmd = new ListIndexesCommand(con).setDb(db).setColl(coll);
        lst = lcmd.execute();
        for (IndexDescription idx : lst) {
            log.info("Index: " + idx.toString());
            assertThat(idx.getName()).isIn("_id_", "tst1", "MyIdx", "ts_exp");
            if (idx.getName().equals("ts_exp")) {
                assertThat(idx.getExpireAfterSeconds()).isNotNull();
                assertThat(idx.getExpireAfterSeconds()).isEqualTo(10);
            }
        }
        assertThat(lst.size()).isEqualTo(4);


        con.disconnect();
    }
}
