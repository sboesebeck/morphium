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

    void connect() throws MorphiumDriverException;

    void connect(String replicaSetName) throws MorphiumDriverException;

    boolean isConnected();

    void disconnect();


    int getRetriesOnNetworkError();

    void setRetriesOnNetworkError(int r);

    int getSleepBetweenErrorRetries();

    void setSleepBetweenErrorRetries(int s);


    void setCredentials(String db, String login, String pwd);


    public abstract MorphiumTransactionContext startTransaction(boolean autoCommit);

    public abstract boolean isTransactionInProgress();

    public abstract void commitTransaction() throws MorphiumDriverException;

    MorphiumTransactionContext getTransactionContext();

    void setTransactionContext(MorphiumTransactionContext ctx);

    void abortTransaction() throws MorphiumDriverException;

    List<Doc> aggregate(AggregateCmdSettings settings) throws MorphiumDriverException;

    long count(CountCmdSettings settings) throws MorphiumDriverException;

    void watch(WatchCmdSettings settings) throws MorphiumDriverException;

    List<Object> distinct(DistinctCmdSettings settings) throws MorphiumDriverException;

    List<Doc> mapReduce(MapReduceSettings settings) throws MorphiumDriverException;

    /**
     * @return number of deleted documents
     */
    int delete(DeleteCmdSettings settings) throws MorphiumDriverException;

    List<Doc> find(FindCmdSettings settings) throws MorphiumDriverException;

    Doc findAndModify(FindAndModifyCmdSettings settings) throws MorphiumDriverException;

    void insert(InsertCmdSettings settings) throws MorphiumDriverException;

    Doc store(StoreCmdSettings settings) throws MorphiumDriverException;

    Doc update(UpdateCmdSettings settings) throws MorphiumDriverException;

    Doc drop(DropCmdSettings settings) throws MorphiumDriverException;

    Doc dropDatabase(DropCmdSettings settings) throws MorphiumDriverException;

    int clearCollection(ClearCollectionSettings settings) throws MorphiumDriverException;

    boolean exists(String db, String coll) throws MorphiumDriverException;

    Doc runCommand(String db, Doc cmd) throws MorphiumDriverException;

    List<String> listCollections(String db, String pattern) throws MorphiumDriverException;

}
