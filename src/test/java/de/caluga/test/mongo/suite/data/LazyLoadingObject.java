package de.caluga.test.mongo.suite.data;

import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.driver.MorphiumId;

import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 29.05.12
 * Time: 00:03
 * <p>
 */
@Entity
@NoCache
@WriteSafety(waitForJournalCommit = false, level = SafetyLevel.WAIT_FOR_ALL_SLAVES)
public class LazyLoadingObject {
    @Id
    private MorphiumId id;

    @Reference(lazyLoading = true)
    private UncachedObject lazyUncached;

    @Reference(lazyLoading = true)
    private CachedObject lazyCached;

    @Reference(lazyLoading = true)
    private List<UncachedObject> lazyLst;


    private String name;

    public List<UncachedObject> getLazyLst() {
        return lazyLst;
    }

    public void setLazyLst(List<UncachedObject> lazyLst) {
        this.lazyLst = lazyLst;
    }

    public MorphiumId getId() {
        return id;
    }

    public void setId(MorphiumId id) {
        this.id = id;
    }

    public UncachedObject getLazyUncached() {
        return lazyUncached;
    }

    public void setLazyUncached(UncachedObject lazyUncached) {
        this.lazyUncached = lazyUncached;
    }

    public CachedObject getLazyCached() {
        return lazyCached;
    }

    public void setLazyCached(CachedObject lazyCached) {
        this.lazyCached = lazyCached;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public enum Fields {lazyCached, lazyLst, lazyUncached, name, id}
}
