package de.caluga.test.mongo.suite.data;

import de.caluga.morphium.annotations.Aliases;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Reference;
import de.caluga.morphium.driver.MorphiumId;

import java.util.List;

@Entity
public class AliasesEntity {
    @Id
    private MorphiumId id;

    @Aliases({"a_value", "the_value"})
    private String value;

    @Aliases({"lots_of_values", "value_list"})
    private List<String> values;

    @Aliases({"ucs", "others"})
    private List<UncachedObject> ucList;

    @Aliases({"uc_revs", "other_refs"})
    @Reference
    private List<UncachedObject> references;

    public MorphiumId getId() {
        return id;
    }

    public void setId(MorphiumId id) {
        this.id = id;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }

    public List<UncachedObject> getUcList() {
        return ucList;
    }

    public void setUcList(List<UncachedObject> ucList) {
        this.ucList = ucList;
    }

    public List<UncachedObject> getReferences() {
        return references;
    }

    public void setReferences(List<UncachedObject> references) {
        this.references = references;
    }

    public enum Fields {references, ucList, value, values, id}

}
