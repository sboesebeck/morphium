package de.caluga.morphium.driver;/**
 * Created by stephan on 16.10.15.
 */

import de.caluga.morphium.annotations.ReadPreferenceLevel;

/**
 * TODO: Add Documentation here
 **/
public interface MorphiumDb {
    MorphiumCollection getCollection(String name);

    String getName();

    void setDefaultReadPreference(ReadPreferenceLevel l);

    ReadPreferenceLevel getDefaultReadPreference();

    void setDefaultWriteSafety();


    void dropDatabase();

    MorphiumCollection createCollection(String name, MongoDocument options);

    MongoDocument command(MongoDocument cmd);

    boolean existsCollection(String coll);


    /**
     * bit num	name	description
     * 0	Reserved	Must be set to 0.
     * 1	TailableCursor	Tailable means cursor is not closed when the last data is retrieved. Rather, the cursor marks the final object’s position. You can resume using the cursor later, from where it was located, if more data were received. Like any “latent cursor”, the cursor may become invalid at some point (CursorNotFound) – for example if the final object it references were deleted.
     * 2	SlaveOk	Allow query of replica slave. Normally these return an error except for namespace “local”.
     * 3	OplogReplay	Internal replication use only - driver should not set
     * 4	NoCursorTimeout	The server normally times out idle cursors after an inactivity period (10 minutes) to prevent excess memory use. Set this option to prevent that.
     * 5	AwaitData	Use with TailableCursor. If we are at the end of the data, block for a while rather than returning no data. After a timeout period, we do return as normal.
     * 6	Exhaust	Stream the data down full blast in multiple “more” packages, on the assumption that the client will fully read all data queried. Faster when you are pulling a lot of data and know you want to pull it all down. Note: the client is not allowed to not read all the data unless it closes the connection.
     * 7	Partial	Get partial results from a mongos if some shards are down (instead of throwing an error)
     * 8-31	Reserved	Must be set to 0.
     */
    int getFlags();

    void setFlags(int f);
}
