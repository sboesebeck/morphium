package de.caluga.morphium.driver;/**
 * Created by stephan on 16.10.15.
 */

import java.util.Map;

/**
 * TODO: Add Documentation here
 **/
public interface MorphiumDb {
    MorphiumCollection getCollection(String name);

    String getName();

    void setDefaultReadPreference(ReadPreference rp);

    ReadPreference getDefaultReadPreference();

    void setDefaultWriteConcern(WriteConcern wc);

    WriteConcern getDefaultWriteConcern();


    void drop();

    MorphiumCollection createCollection(String name, Map<String, Object> options);

    Map<String, Object> command(Map<String, Object> cmd);

    boolean existsCollection(String coll);


}
