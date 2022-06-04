package de.caluga.morphium.driver;/**
 * Created by stephan on 15.10.15.
 */

import de.caluga.morphium.driver.commands.*;

import java.util.List;

/**
 * Morphium driver interface
 * <p>
 * All drivers need to implement this interface. you can add your own drivers to morphium. These are actually not
 * limited to be mongodb drivers. There is also an InMemory implementation.
 **/
@SuppressWarnings({"BooleanMethodIsAlwaysInverted", "RedundantThrows", "UnusedReturnValue"})
public interface MorphiumDriver {
    String VERSION_NAME = "morphium version";

    String getName();

    int getBuildNumber();

    int getMajorVersion();

    int getMinorVersion();

    void setHostSeed(String... host);

    void setConnectionUrl(String connectionUrl);

    void connect();

    void connect(String replicaSetName);

    boolean isConnected();

    void disconnect();


    int getRetriesOnNetworkError();

    void setRetriesOnNetworkError(int r);

    int getSleepBetweenErrorRetries();

    void setSleepBetweenErrorRetries(int s);

    String getAtlasUrl();

    void setAtlasUrl(String atlas);

    void setCredentials(String db, String login, char[] pwd);


    List<Doc> aggregate(AggregateCmdSettings settings) throws MorphiumDriverException;

    long count(CountCmdSettings settings) throws MorphiumDriverException;

    void watch(WatchCmdSettings settings);

    List<Object> distinct(DistinctCmdSettings settings) throws MorphiumDriverException;

    List<Doc> mapReduce(MapReduceSettings settings);

    /**
     * @return number of deleted documents
     */
    int delete(DeleteCmdSettings settings) throws MorphiumDriverException;

    List<Doc> find(FindCmdSettings settings) throws MorphiumDriverException;

    List<Doc> findAndModify(FindAndModifyCmdSettings settings);

    void insert(InsertCmdSettings settings);

    Doc update(UpdateCmdSettings settings) throws MorphiumDriverException;

    Doc drop(DropCmdSettings settings);

    Doc dropDatabase(DropCmdSettings settings);

    int clearCollection(ClearCollectionSettings settings) throws MorphiumDriverException;


}
