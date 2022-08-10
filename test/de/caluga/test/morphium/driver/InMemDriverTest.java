package de.caluga.test.morphium.driver;

import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.*;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.junit.Assert.*;

public class InMemDriverTest {
    private Logger log= LoggerFactory.getLogger(InMemDriverTest.class);
    @Test
    public void inMemTest() throws Exception{
        InMemoryDriver drv=new InMemoryDriver();
        HelloCommand cmd=new HelloCommand(drv);
        var hello=cmd.execute();

        log.info(hello.toString());
        CreateIndexesCommand cimd=new CreateIndexesCommand(drv).addIndex(new IndexDescription().setKey(Doc.of("counter",1)));
        cimd.setDb("testing").setColl("testcoll1");
        cimd.execute();
//        drv.createIndex("testing","testcoll1", Doc.of("counter",1),Doc.of());

        ListIndexesCommand lcmd=new ListIndexesCommand(drv).setDb("testing").setColl("testcoll1");
        var ret=lcmd.execute();
        log.info("Indexes: "+ret.size());
        assertEquals(ret.size(),2);

        boolean exc=false;
        try {
            drv.createIndex("testing","testcoll1", Doc.of("counter",1),Doc.of("name","dings","unique",true));
        } catch(MorphiumDriverException e){
            exc=true;
        }
        assertTrue("Creating the same index should throw an exception",exc);

        ShutdownCommand shutdownCommand=new ShutdownCommand(drv).setTimeoutSecs(10);
        var sh=shutdownCommand.execute();
        log.info("Result: "+Utils.toJsonString(sh));
        assertNotSame(sh.get("ok"),1.0) ;

        var stepDown=new StepDownCommand(drv).setTimeToStepDown(10);
        sh=stepDown.execute();
        log.info("Result: "+Utils.toJsonString(sh));
        assertNotSame(sh.get("ok"),1.0) ;


        InsertMongoCommand insert=new InsertMongoCommand(drv).setColl("testcoll").setDb("testing");
        insert.setDocuments(Arrays.asList(Doc.of("_id",new MorphiumId(),"value",123,"strVal","Hello World")));
        var insertResult=insert.execute();
        log.info("Result: "+Utils.toJsonString(insertResult));
        assertEquals(insertResult.get("n"),1);
        new InsertMongoCommand(drv).setColl("testcoll").setDb("testing");
        insert.setDocuments(Arrays.asList(Doc.of("_id",new MorphiumId(),"value",13,"strVal","Hello"),
                Doc.of("_id",new MorphiumId(),"value",14,"strVal","Hello2"),
                Doc.of("_id",new MorphiumId(),"value",15,"strVal","Hello3")
        ));
        insertResult=insert.execute();
        assertEquals(insertResult.get("n"),3);

        FindCommand fnd=new FindCommand(drv).setColl("testcoll").setDb("testing");
        fnd.setFilter(Doc.of("value",14));
        var found=fnd.execute();
        assertEquals("Should only find 1",found.size(),1);

        fnd.setFilter(null);
        found=fnd.execute();
        assertEquals("Should find 4",found.size(),4);

        DistinctMongoCommand distinct=new DistinctMongoCommand(drv).setKey("strVal");
        distinct.setDb("testing").setColl("testcoll");
        var distinctResult=distinct.execute();
        log.info("Distinct values: "+distinctResult.size());
        assertEquals(4,distinctResult.size());


        CountMongoCommand count=new CountMongoCommand(drv).setColl("testcoll").setDb("testing").setQuery(Doc.of());
        assertEquals(4,count.getCount());
        ClearCollectionCommand clr=new ClearCollectionCommand(drv).setColl("testcoll").setDb("testing");
        var cleared=clr.execute();
        assertEquals(drv.getDatabase("testing").get("testcoll").size(),0);
    }
}
