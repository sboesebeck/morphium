package de.caluga.morphium.driver;/**
 * Created by stephan on 15.10.15.
 */

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.bulk.BulkRequestContext;
import de.caluga.morphium.driver.commands.*;

import java.util.List;
import java.util.Map;

/**
 * Morphium driver interface
 * <p>
 * All drivers need to implement this interface. you can add your own drivers to morphium. These are actually not
 * limited to be mongodb drivers. There is also an InMemory implementation.
 **/
@SuppressWarnings({"BooleanMethodIsAlwaysInverted", "RedundantThrows", "UnusedReturnValue"})
public interface MorphiumDriver {

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


    abstract MorphiumTransactionContext startTransaction(boolean autoCommit);

    abstract boolean isTransactionInProgress();

    abstract void commitTransaction() throws MorphiumDriverException;

    MorphiumTransactionContext getTransactionContext();

    void setTransactionContext(MorphiumTransactionContext ctx);

    void abortTransaction() throws MorphiumDriverException;

    List<Map<String, Object>> aggregate(AggregateCmdSettings settings) throws MorphiumDriverException;

    MorphiumCursor initAggregationIteration(AggregateCmdSettings settings) throws MorphiumDriverException;

    MorphiumCursor initIteration(FindCmdSettings settings) throws MorphiumDriverException;

    MorphiumCursor nextIteration(MorphiumCursor crs) throws MorphiumDriverException;

    long count(CountCmdSettings settings) throws MorphiumDriverException;

    void watch(WatchCmdSettings settings) throws MorphiumDriverException;

    List<Object> distinct(DistinctCmdSettings settings) throws MorphiumDriverException;

    List<Map<String, Object>> mapReduce(MapReduceSettings settings) throws MorphiumDriverException;

    /**
     * @return number of deleted documents
     */
    int delete(DeleteCmdSettings settings) throws MorphiumDriverException;

    List<Map<String, Object>> find(FindCmdSettings settings) throws MorphiumDriverException;

    Map<String, Object> findAndModify(FindAndModifyCmdSettings settings) throws MorphiumDriverException;

    void insert(InsertCmdSettings settings) throws MorphiumDriverException;

    Map<String, Object> store(StoreCmdSettings settings) throws MorphiumDriverException;

    Map<String, Object> update(UpdateCmdSettings settings) throws MorphiumDriverException;

    Map<String, Object> drop(DropCmdSettings settings) throws MorphiumDriverException;

    Map<String, Object> dropDatabase(DropCmdSettings settings) throws MorphiumDriverException;

    int clearCollection(ClearCollectionSettings settings) throws MorphiumDriverException;

    boolean exists(String db, String coll) throws MorphiumDriverException;

    boolean exists(String db) throws MorphiumDriverException;

    Map<String, Object> runCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException;

    List<String> listCollections(String db, String pattern) throws MorphiumDriverException;

    BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered, WriteConcern wc);

    List<String> listDatabases() throws MorphiumDriverException;

    Map<String, Object> getDbStats(String db, boolean withStorage) throws MorphiumDriverException;

    Map<String, Object> getDbStats(String db) throws MorphiumDriverException;

    Map<String, Object> getCollStats(String db, String coll) throws MorphiumDriverException;

    Map<String, Object> getReplsetStatus() throws MorphiumDriverException;
}
