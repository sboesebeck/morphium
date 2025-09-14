package de.caluga.test.mongo.suite.base;

import de.caluga.test.mongo.suite.data.UncachedObject;
import de.caluga.test.mongo.suite.data.UncachedObjectDAO;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * User: Stephan Bösebeck
 * Date: 17.05.12
 * Time: 15:17
 * <p/>
 */
@Tag("core")
public class DAOTest extends MorphiumTestBase {
    @Test
    public void daoTest() throws Exception {
        for (int i = 1; i <= 100; i++) {
            UncachedObject o = new UncachedObject();
            o.setCounter(i);
            o.setStrValue("Uncached " + i);
            morphium.store(o);
        }
        Thread.sleep(1000);
        UncachedObjectDAO dao = new UncachedObjectDAO(morphium);
        List<UncachedObject> lst = dao.getAll();
        assert (lst.size() == 100) : "Wrong element count: " + lst.size();

        lst = dao.findByField(UncachedObjectDAO.Field.counter, 55);
        assert (lst.size() == 1) : "Wrong element count in find: " + lst.size();

        assert (lst.get(0).getCounter() == 55) : "Got wrong element: " + lst.get(0).getCounter();
        assertNotNull(dao.getValue(UncachedObjectDAO.Field.counter, lst.get(0)));
        ;
        assertNotNull(dao.getValue("counter", lst.get(0)));
        ;
        assert (dao.existsField("str_value"));
        dao.setValue(UncachedObjectDAO.Field.counter, 12, lst.get(0));
        assert (lst.get(0).getCounter() == 12) : "Got wrong element: " + lst.get(0).getCounter();
        dao.setValue("counter", 13, lst.get(0));
        assert (lst.get(0).getCounter() == 13) : "Got wrong element: " + lst.get(0).getCounter();

    }
}
