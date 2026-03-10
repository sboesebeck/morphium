package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.driver.MorphiumId;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;

/**
 * Created with IntelliJ IDEA.
 * User: bfsmith
 * Date: 30.03.15
 * Test interface polymorphism mechanism in Morphium
 */
@SuppressWarnings("AssertWithSideEffects")
@Tag("core")
public class InterfacePolymorphismTest extends MultiDriverTestBase {
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void polymorphTest(Morphium morphium) throws Exception  {
        morphium.dropCollection(IfaceTestType.class);
        IfaceTestType ifaceTestType = new IfaceTestType();
        ifaceTestType.setName("A Complex Type");
        ifaceTestType.setPolyTest(new SubClass(11));
        morphium.store(ifaceTestType);
        Thread.sleep(100);
        assert (morphium.createQueryFor(IfaceTestType.class).countAll() == 1);
        List<IfaceTestType> lst = morphium.createQueryFor(IfaceTestType.class).asList();
        for (IfaceTestType tst : lst) {
            log.info("Class " + tst.getClass().toString());
        }
    }

    @Embedded(polymorph = true)
    @NoCache
    public interface IPolyTest {
        int getNumber();
    }

    @Entity
    public static class IfaceTestType {
        @Id
        private MorphiumId id;
        private String name;
        private IPolyTest polyTest;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public IPolyTest getPolyTest() {
            return polyTest;
        }

        public void setPolyTest(IPolyTest polyTest) {
            this.polyTest = polyTest;
        }
    }

    public static class SubClass implements IPolyTest {
        private int number;

        private SubClass() {
        }

        public SubClass(int num) {
            number = num;
        }

        public int getNumber() {
            return number;
        }

        public void setNumber(int number) {
            this.number = number;
        }
    }
}
