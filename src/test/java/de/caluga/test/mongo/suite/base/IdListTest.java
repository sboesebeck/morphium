package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;


@Tag("core")
public class IdListTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void idList(Morphium morphium) throws Exception  {
        createUncachedObjects(morphium, 100);

        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).gte(10);
        List<Object> lst = q.idList();
        assertEquals(90, lst.size());
        assertThat(lst.get(0)).isInstanceOf(MorphiumId.class);
        assertThat(morphium.findById(UncachedObject.class, lst.get(0))).isInstanceOf(UncachedObject.class);
        assertNotNull(morphium.findById(UncachedObject.class, lst.get(0)));
    }
}
