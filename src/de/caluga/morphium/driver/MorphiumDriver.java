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

    //     _______. _______ .___________.___________. __  .__   __.   _______      _______.
    //    /       ||   ____||           |           ||  | |  \ |  |  /  _____|    /       |
    //   |   (----`|  |__   `---|  |----`---|  |----`|  | |   \|  | |  |  __     |   (----`
    //    \   \    |   __|      |  |        |  |     |  | |  . `  | |  | |_ |     \   \
    //.----)   |   |  |____     |  |        |  |     |  | |  |\   | |  |__| | .----)   |
    //|_______/    |_______|    |__|        |__|     |__| |__| \__|  \______| |_______/
    String getName();

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


    ////////////////////////////////////////////////////
    // .___________..______          ___      .__   __.      _______.     ___       ______ .___________. __    ______   .__   __.      _______.
    //|           ||   _  \        /   \     |  \ |  |     /       |    /   \     /      ||           ||  |  /  __  \  |  \ |  |     /       |
    //`---|  |----`|  |_)  |      /  ^  \    |   \|  |    |   (----`   /  ^  \   |  ,----'`---|  |----`|  | |  |  |  | |   \|  |    |   (----`
    //    |  |     |      /      /  /_\  \   |  . `  |     \   \      /  /_\  \  |  |         |  |     |  | |  |  |  | |  . `  |     \   \
    //    |  |     |  |\  \----./  _____  \  |  |\   | .----)   |    /  _____  \ |  `----.    |  |     |  | |  `--'  | |  |\   | .----)   |
    //    |__|     | _| `._____/__/     \__\ |__| \__| |_______/    /__/     \__\ \______|    |__|     |__|  \______/  |__| \__| |_______/


    abstract MorphiumTransactionContext startTransaction(boolean autoCommit);

    abstract boolean isTransactionInProgress();

    abstract void commitTransaction() throws MorphiumDriverException;

    MorphiumTransactionContext getTransactionContext();

    void setTransactionContext(MorphiumTransactionContext ctx);

    void abortTransaction() throws MorphiumDriverException;


    // __  .___________. _______ .______          ___   .___________. __    ______   .__   __.      _______.
    //|  | |           ||   ____||   _  \        /   \  |           ||  |  /  __  \  |  \ |  |     /       |
    //|  | `---|  |----`|  |__   |  |_)  |      /  ^  \ `---|  |----`|  | |  |  |  | |   \|  |    |   (----`
    //|  |     |  |     |   __|  |      /      /  /_\  \    |  |     |  | |  |  |  | |  . `  |     \   \
    //|  |     |  |     |  |____ |  |\  \----./  _____  \   |  |     |  | |  `--'  | |  |\   | .----)   |
    //|__|     |__|     |_______|| _| `._____/__/     \__\  |__|     |__|  \______/  |__| \__| |_______/


    MorphiumCursor initAggregationIteration(AggregateMongoCommand settings) throws MorphiumDriverException;

    MorphiumCursor initIteration(FindCommand settings) throws MorphiumDriverException;

    //MorphiumCursor nextIteration(MorphiumCursor crs) throws MorphiumDriverException;

    void closeIteration(MorphiumCursor crs) throws MorphiumDriverException;


    //.______       __    __  .__   __.      ______   ______   .___  ___. .___  ___.      ___      .__   __.  _______
    //|   _  \     |  |  |  | |  \ |  |     /      | /  __  \  |   \/   | |   \/   |     /   \     |  \ |  | |       \
    //|  |_)  |    |  |  |  | |   \|  |    |  ,----'|  |  |  | |  \  /  | |  \  /  |    /  ^  \    |   \|  | |  .--.  |
    //|      /     |  |  |  | |  . `  |    |  |     |  |  |  | |  |\/|  | |  |\/|  |   /  /_\  \   |  . `  | |  |  |  |
    //|  |\  \----.|  `--'  | |  |\   |    |  `----.|  `--'  | |  |  |  | |  |  |  |  /  _____  \  |  |\   | |  '--'  |
    //| _| `._____| \______/  |__| \__|     \______| \______/  |__|  |__| |__|  |__| /__/     \__\ |__| \__| |_______/

    //Map<String, Object> runCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException;


    MorphiumCursor runCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException;

    /**
     * sends runcommand message, but only returns ID for later check for replies
     *
     * @param db
     * @param cmd
     * @return
     * @throws MorphiumDriverException
     */
    int sendCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException;


    //____    __    ____  ___   .___________.  ______  __    __
    //\   \  /  \  /   / /   \  |           | /      ||  |  |  |
    // \   \/    \/   / /  ^  \ `---|  |----`|  ,----'|  |__|  |
    //  \            / /  /_\  \    |  |     |  |     |   __   |
    //   \    /\    / /  _____  \   |  |     |  `----.|  |  |  |
    //    \__/  \__/ /__/     \__\  |__|      \______||__|  |__|
    void watch(WatchMongoCommand settings) throws MorphiumDriverException;

    //.______       _______ .______    __       __   _______     _______.
    //|   _  \     |   ____||   _  \  |  |     |  | |   ____|   /       |
    //|  |_)  |    |  |__   |  |_)  | |  |     |  | |  |__     |   (----`
    //|      /     |   __|  |   ___/  |  |     |  | |   __|     \   \
    //|  |\  \----.|  |____ |  |      |  `----.|  | |  |____.----)   |
    //| _| `._____||_______|| _|      |_______||__| |_______|_______/

    MorphiumCursor waitForReplyIterable(long id);


    List<Map<String, Object>> aggregate(AggregateMongoCommand settings) throws MorphiumDriverException;

    long count(CountMongoCommand settings) throws MorphiumDriverException;


    List<Object> distinct(DistinctMongoCommand settings) throws MorphiumDriverException;

    List<Map<String, Object>> mapReduce(MapReduceSettings settings) throws MorphiumDriverException;

    /**
     * @return number of deleted documents
     */
    int delete(DeleteMongoCommand settings) throws MorphiumDriverException;

    List<Map<String, Object>> find(FindCommand settings) throws MorphiumDriverException;

    Map<String, Object> findAndModify(FindAndModifyMongoCommand settings) throws MorphiumDriverException;

    void insert(InsertMongoCommand settings) throws MorphiumDriverException;

    Map<String, Object> store(StoreMongoCommand settings) throws MorphiumDriverException;

    Map<String, Object> update(UpdateMongoCommand settings) throws MorphiumDriverException;

    Map<String, Object> drop(DropMongoCommand settings) throws MorphiumDriverException;

    Map<String, Object> dropDatabase(DropMongoCommand settings) throws MorphiumDriverException;

    int clearCollection(ClearCollectionSettings settings) throws MorphiumDriverException;

    boolean exists(String db, String coll) throws MorphiumDriverException;

    boolean exists(String db) throws MorphiumDriverException;

    List<String> listCollections(String db, String pattern) throws MorphiumDriverException;

    BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered, WriteConcern wc);

    List<String> listDatabases() throws MorphiumDriverException;

    Map<String, Object> getDbStats(String db, boolean withStorage) throws MorphiumDriverException;

    Map<String, Object> getDbStats(String db) throws MorphiumDriverException;

    Map<String, Object> getCollStats(String db, String coll) throws MorphiumDriverException;

    Map<String, Object> getReplsetStatus() throws MorphiumDriverException;
}
