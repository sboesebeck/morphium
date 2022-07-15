package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.MorphiumIterator;
import de.caluga.morphium.query.Query;
import de.caluga.morphium.query.QueryIterator;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * User: Stephan Bösebeck
 * Date: 23.11.12
 * Time: 12:04
 * <p/>
 */
public class MorphiumCursorTest extends MorphiumTestBase {

    private final List<MorphiumId> data = Collections.synchronizedList(new ArrayList<>());

    private int runningThreads = 0;

    @Test
    public void cursorSortTest() throws Exception {
        for (int i = 0; i < 100; i++) {
            morphium.store(new SimpleEntity(((int) (Math.random() * 5.0)), (long) (Math.random() * 100000.0)));
        }
        Thread.sleep(1000);

        Query<SimpleEntity> q = morphium.createQueryFor(SimpleEntity.class);

        q.or(q.q().f("v2").lt(100), q.q().f("v2").gt(200));
        List<Integer> lst = new ArrayList<>();
        lst.add(100);
        lst.add(1001);
        q.f("v2").nin(lst);
        Map<String, Integer> map = UtilsMap.of("v1", 1);
        map.put("v2", 1);
        q.sort(map);


        long lastv2 = -1;
        int lastv1 = 0;
        MorphiumCursor it = q.getCursor();
        boolean error = false;
        for (Map<String, Object> m : it) {
            var u = morphium.getMapper().deserialize(SimpleEntity.class, m);
            log.info(u.v1 + " ---- " + u.v2);
            if (lastv1 > u.v1) {
                error = true;
            }

            if (lastv1 != u.v1) {
                lastv2 = -1;
            }
            if (lastv2 > u.v2) {
                error = true;
            }
            lastv2 = u.v2;
            lastv1 = u.v1;
        }
        assert (!error);
    }

    @Test
    public void cursorSkipAheadBackTest() throws Exception {
        for (int i = 0; i < 100; i++) {
            morphium.store(new SimpleEntity(i, (long) (Math.random() * 100000.0)));
        }
        var crs = morphium.createQueryFor(SimpleEntity.class).sort("v1").setBatchSize(10).getFindCmd().executeIterable(10);
        crs.ahead(14);
        var n = crs.next();
        assertThat(n).isNotNull();
        assertThat(crs.getCursor()).isEqualTo(15);
        assertThat(n.get("v1")).isEqualTo(14);
        assertThat(crs.available()).isEqualTo(5);
        crs.back(2);
        assertThat(crs.getCursor()).isEqualTo(13);
        n = crs.next();
        assertThat(n.get("v1")).isEqualTo(13);
        assertThat(crs.getCursor()).isEqualTo(14);


    }

    @Entity
    public static class SimpleEntity {
        @Id
        public MorphiumId id;
        public int v1;
        public long v2;
        public String v1str;

        public SimpleEntity(int v1, long v2) {
            this.v1 = v1;
            this.v2 = v2;
            v1str = "" + v1;
        }
    }


}
