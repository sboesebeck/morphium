package de.caluga.test.mongo.suite;

import de.caluga.test.mongo.suite.data.UncachedObject;
import de.caluga.test.mongo.suite.data.UncachedObjectDAO;
import org.junit.Test;

import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 17.05.12
 * Time: 15:17
 * <p/>
 */
public class DAOTest extends MongoTest {
    @Test
    public void daoTest() throws Exception {
        for (int i = 1; i <= 100; i++) {
            UncachedObject o = new UncachedObject();
            o.setCounter(i);
            o.setValue("Uncached " + i);
            morphium.store(o);
        }
        Thread.sleep(1000);
        UncachedObjectDAO dao = new UncachedObjectDAO(morphium);
        List<UncachedObject> lst = dao.getAll();
        assert (lst.size() == 100) : "Wrong element count: " + lst.size();

        lst = dao.findByField(UncachedObjectDAO.Field.counter, 55);
        assert (lst.size() == 1) : "Wrong element count in find: " + lst.size();

        assert (lst.get(0).getCounter() == 55) : "Got wrong element: " + lst.get(0).getCounter();
    }
}
