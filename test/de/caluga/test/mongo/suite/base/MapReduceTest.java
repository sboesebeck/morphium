package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by stephan on 28.07.16.
 */
public class MapReduceTest extends MorphiumTestBase {

    @Test
    public void mapReduce() throws Exception {
        createUncachedObjects(100);

        doSimpleMRTest(morphium);
//        doSimpleMRTest(morphiumMongodb);
//
    }

    private void doSimpleMRTest(Morphium m) throws Exception {
        List<UncachedObject> result = m.mapReduce(UncachedObject.class, "function(){emit(this.counter%2==0,this);}", "function (key,values){var ret={_id:ObjectId(), str_value:\"\", counter:0}; if (key==true) {ret.str_value=\"even\";} else { ret.str_value=\"odd\";} for (var i=0; i<values.length;i++){ret.counter=ret.counter+values[i].counter;}return ret;}");
        assertThat(result.size()).isEqualTo(2);
        boolean odd = false;
        boolean even = false;
        for (UncachedObject r : result) {
            if (r.getStrValue().equals("odd")) {
                odd = true;
            }
            if (r.getStrValue().equals("even")) {
                even = true;
            }
            assert (r.getCounter() > 0);
        }
        assert (odd);
        assert (even);
    }
}
