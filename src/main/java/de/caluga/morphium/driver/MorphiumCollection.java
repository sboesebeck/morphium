package de.caluga.morphium.driver;

/**
 * Created by stephan on 15.10.15.
 */


@SuppressWarnings({"WeakerAccess", "unused", "DefaultFileTemplate"})
public class MorphiumCollection {

    private String collection;

    private ReadPreference readPreference;
    private WriteConcern writeConcern;


    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    public void setWriteConcern(WriteConcern writeConcern) {
        this.writeConcern = writeConcern;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public ReadPreference getReadPreference() {
        return readPreference;
    }

    public void setReadPreference(ReadPreference readPreference) {
        this.readPreference = readPreference;
    }
}
