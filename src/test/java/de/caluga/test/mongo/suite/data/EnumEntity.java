package de.caluga.test.mongo.suite.data;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.SafetyLevel;
import de.caluga.morphium.annotations.WriteSafety;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.test.mongo.suite.data.TestEnum;

import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 04.05.12
 * Time: 12:44
 * <p>
 */
@Entity
@NoCache
@WriteSafety(level = SafetyLevel.WAIT_FOR_ALL_SLAVES, waitForJournalCommit = false)
public class EnumEntity {
    @Id
    private MorphiumId id;

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

    public MorphiumId getId() {
        return id;
    }

    public void setId(MorphiumId id) {
        this.id = id;
    }

    public TestEnum getTst() {
        return tst;
    }

    public void setTst(TestEnum tst) {
        this.tst = tst;
    }
}
