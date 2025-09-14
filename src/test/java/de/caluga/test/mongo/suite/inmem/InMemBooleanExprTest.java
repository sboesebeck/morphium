package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Property;
import de.caluga.morphium.driver.MorphiumId;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;


class InMemBooleanExprTest extends MorphiumInMemTestBase {

    @Test
    public void testNe() {
        BooleanTest testTrueTrue = new BooleanTest(true, true);
        BooleanTest testFalseFalse = new BooleanTest(false, false);
        morphium.store(Arrays.asList(testTrueTrue, testFalseFalse));

        List<BooleanTest> resultList = morphium.createQueryFor(BooleanTest.class)
                .expr(Expr.ne(Expr.field("a_boolean_primitive"), Expr.bool(false)))
                .asList();

        assertThat(resultList).hasSize(1);
        assertThat(resultList.get(0)).isEqualTo(testTrueTrue);
    }

    @Test
    public void testNeWithNullValue() {
        BooleanTest testNullFalse = new BooleanTest(null, false);
        BooleanTest testFalseFalse = new BooleanTest(false, false);
        morphium.store(List.of(testNullFalse, testFalseFalse));

        List<BooleanTest> resultList = morphium.createQueryFor(BooleanTest.class)
                .expr(Expr.ne(Expr.field("a_boolean_object"), Expr.nullExpr()))
                        .asList();

        assertThat(resultList).hasSize(1);
        assertThat(resultList.get(0)).isEqualTo(testFalseFalse);
    }

    @Entity
    private static class BooleanTest {
        @Id
        MorphiumId morphiumId;
        @Property(fieldName = "a_boolean_object")
        private Boolean aBooleanObject = null;
        @Property(fieldName = "a_boolean_primitive")
        private boolean aBooleanPrimitive;

        public BooleanTest(Boolean aBooleanObject, boolean aBooleanPrimitive) {
            this.aBooleanObject = aBooleanObject;
            this.aBooleanPrimitive = aBooleanPrimitive;
        }

        public Boolean getaBooleanObject() {
            return aBooleanObject;
        }

        public void setaBooleanObject(Boolean aBooleanObject) {
            this.aBooleanObject = aBooleanObject;
        }

        public boolean isaBooleanPrimitive() {
            return aBooleanPrimitive;
        }

        public void setaBooleanPrimitive(boolean aBooleanPrimitive) {
            this.aBooleanPrimitive = aBooleanPrimitive;
        }

        public MorphiumId getMorphiumId() {
            return morphiumId;
        }

        public void setMorphiumId(MorphiumId morphiumId) {
            this.morphiumId = morphiumId;
        }

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
        }

        @Override
        public boolean equals(Object o) {
            return EqualsBuilder.reflectionEquals(this, o);
        }

        @Override
        public int hashCode() {
            return HashCodeBuilder.reflectionHashCode(this);
        }
    }
}