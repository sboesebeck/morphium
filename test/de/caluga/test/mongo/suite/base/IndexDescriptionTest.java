package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.IndexDescription;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class IndexDescriptionTest {

    @Test
    public void asMapTest() throws Exception {
        IndexDescription idx = new IndexDescription();
        idx.setBits(12).setKey(Doc.of("_id", 1));
        idx.setName("Test");

        var m = idx.asMap();
        assertThat(m.containsKey("key")).isTrue();
        assertThat(m.containsKey("bits")).isTrue();
        assertThat(m.containsKey("name")).isTrue();
        assertThat(m.get("name")).isEqualTo("Test");
    }

    @Test
    public void asMapFromMapTest() throws Exception {
        IndexDescription idx = new IndexDescription();
        idx.setBits(12).setKey(Doc.of("_id", 1, "value1", -1));
        idx.setName("Test").setBackground(true).setHidden(false).setMax(10).setMin(4)
                .setExpireAfterSeconds(1000)
                .setUnique(true)
                .setWildcardProjection(Doc.of("$**", 1))
                .setSparse(true)
                .setTextIndexVersion(42);

        var m = idx.asMap();
        var idx2 = IndexDescription.fromMap(m);
        assertThat(idx2.getName()).isEqualTo(idx.getName());
        assertThat(idx2.getBits()).isEqualTo(idx.getBits());
        assertThat(idx2.getCollation()).isEqualTo(idx.getCollation());
        assertThat(idx2.getKey()).isEqualTo(idx.getKey());
        assertThat(idx2.getHidden()).isEqualTo(idx.getHidden());
        assertThat(idx2.getSparse()).isEqualTo(idx.getSparse());
    }
}
