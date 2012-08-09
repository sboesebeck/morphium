package de.caluga.test.mongo.suite;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.SafetyLevel;
import de.caluga.morphium.annotations.WriteSafety;
import de.caluga.morphium.annotations.caching.NoCache;
import org.bson.types.ObjectId;

import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 04.05.12
 * Time: 12:44
 * <p/>
 */
@Entity
@NoCache
@WriteSafety(level = SafetyLevel.WAIT_FOR_ALL_SLAVES, waitForJournalCommit = true)
public class EnumEntity {
    @Id
    private ObjectId id;

    private TestEnum tst;
    private String value;
    private List<TestEnum> tstLst;

    public List<TestEnum> getTstLst() {
        return tstLst;
    }

    public void setTstLst(List<TestEnum> tstLst) {
        this.tstLst = tstLst;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public TestEnum getTst() {
        return tst;
    }

    public void setTst(TestEnum tst) {
        this.tst = tst;
    }
}
