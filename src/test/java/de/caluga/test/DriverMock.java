package de.caluga.test;

import java.net.MalformedURLException;
import java.util.List;
import java.util.Map;

import org.slf4j.LoggerFactory;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.AggregatorImpl;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumTransactionContext;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.WriteConcern;
import de.caluga.morphium.driver.bulk.BulkRequestContext;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.wire.MongoConnection;

public class DriverMock implements MorphiumDriver {

    private int maxWaitTime = 99991000;

    @Override
    public String getName() {
        return "mock";
    }

    @Override
    public int getMaxBsonObjectSize() {
        return 16 * 1024 * 1024;
    }

    @Override
    public void setMaxBsonObjectSize(int maxBsonObjectSize) {

    }

    @Override
    public int getMaxMessageSize() {
        return 16 * 1024 * 1024;
    }

    @Override
    public void setMaxMessageSize(int maxMessageSize) {

    }

    @Override
    public int getMaxWriteBatchSize() {
        return 1000;
    }

    @Override
    public void setMaxWriteBatchSize(int maxWriteBatchSize) {

    }

    @Override
    public boolean isReplicaSet() {
        return false;
    }

    @Override
    public void setReplicaSet(boolean replicaSet) {

    }

    @Override
    public boolean getDefaultJ() {
        return false;
    }

    @Override
    public int getDefaultWriteTimeout() {
        return 1000;
    }

    @Override
    public void setDefaultWriteTimeout(int wt) {

    }

    @Override
    public int getMaxWaitTime() {

        return maxWaitTime;
    }

    @Override
    public void setMaxWaitTime(int maxWaitTime) {
        this.maxWaitTime = maxWaitTime;
    }

    @Override
    public String[] getCredentials(String db) {
        return null;
    }

    @Override
    public List<String> getHostSeed() {
        return List.of("localhost:27017");
    }

    @Override
    public void setHostSeed(List<String> hosts) {

    }

    @Override
    public void setHostSeed(String... host) {

    }

    @Override
    public void setConnectionUrl(String connectionUrl) throws MalformedURLException {

    }

    @Override
    public void connect() throws MorphiumDriverException {
        //do nothing
    }

    @Override
    public void connect(String replicaSetName) throws MorphiumDriverException {
        //do nothing
        LoggerFactory.getLogger(DriverMock.class).warn("Just a mock - not connecting!");
    }

    @Override
    public boolean isConnected() {
        return true;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean isReplicaset() {
        return false;
    }

    @Override
    public <T, R> Aggregator<T, R> createAggregator(Morphium morphium, Class<? extends T> type, Class<? extends R> resultType) {
        return new AggregatorImpl<>(morphium, type, resultType);
    }

    @Override
    public List<String> listDatabases() throws MorphiumDriverException {
        return null;
    }

    @Override
    public List<String> listCollections(String db, String pattern) throws MorphiumDriverException {
        return null;
    }

    @Override
    public String getReplicaSetName() {
        return null;
    }

    @Override
    public void setReplicaSetName(String replicaSetName) {

    }



    @Override
    public int getRetriesOnNetworkError() {
        return 2;
    }

    @Override
    public MorphiumDriver setRetriesOnNetworkError(int r) {
        return this;
    }

    @Override
    public int getSleepBetweenErrorRetries() {
        return 250;
    }

    @Override
    public MorphiumDriver setSleepBetweenErrorRetries(int s) {
        return this;
    }

    @Override
    public int getMaxConnections() {
        return 100;
    }

    @Override
    public MorphiumDriver setMaxConnections(int maxConnections) {
        return this;
    }

    @Override
    public int getMinConnections() {
        return 0;
    }

    @Override
    public MorphiumDriver setMinConnections(int minConnections) {
        return this;
    }

    @Override
    public boolean isRetryReads() {
        return true;
    }

    @Override
    public MorphiumDriver setRetryReads(boolean retryReads) {
        return this;
    }

    @Override
    public boolean isRetryWrites() {
        return true;
    }

    @Override
    public MorphiumDriver setRetryWrites(boolean retryWrites) {
        return this;
    }

    @Override
    public int getReadTimeout() {
        return 1000;
    }

    @Override
    public void setReadTimeout(int readTimeout) {

    }

    @Override
    public int getMinConnectionsPerHost() {
        return 0;
    }

    @Override
    public void setMinConnectionsPerHost(int minConnectionsPerHost) {

    }

    @Override
    public int getMaxConnectionsPerHost() {
        return 1000;
    }

    @Override
    public void setMaxConnectionsPerHost(int maxConnectionsPerHost) {

    }

    @Override
    public void setCredentials(String db, String login, String pwd) {

    }

    @Override
    public boolean isCapped(String db, String coll) throws MorphiumDriverException {
        return false;
    }

    @Override
    public Map<String, Integer> getNumConnectionsByHost() {
        return null;
    }

    @Override
    public MorphiumTransactionContext startTransaction(boolean autoCommit) {
        return null;
    }

    @Override
    public boolean isTransactionInProgress() {
        return false;
    }

    @Override
    public void commitTransaction() throws MorphiumDriverException {

    }

    @Override
    public MorphiumTransactionContext getTransactionContext() {
        return null;
    }

    @Override
    public void setTransactionContext(MorphiumTransactionContext ctx) {

    }

    @Override
    public void abortTransaction() throws MorphiumDriverException {

    }


    @Override
    public Map<String, Object> getReplsetStatus() throws MorphiumDriverException {
        return null;
    }

    @Override
    public Map<String, Object> getDBStats(String db) throws MorphiumDriverException {
        return null;
    }

    @Override
    public Map<String, Object> getCollStats(String db, String coll) throws MorphiumDriverException {
        return null;
    }

    @Override
    public int getMaxConnectionLifetime() {
        return 0;
    }

    @Override
    public void setMaxConnectionLifetime(int timeout) {

    }

    @Override
    public int getMaxConnectionIdleTime() {
        return 0;
    }

    @Override
    public void setMaxConnectionIdleTime(int time) {

    }

    @Override
    public int getConnectionTimeout() {
        return 0;
    }

    @Override
    public void setConnectionTimeout(int timeout) {

    }

    @Override
    public int getDefaultW() {
        return 0;
    }

    @Override
    public void setDefaultW(int w) {

    }

    @Override
    public int getHeartbeatFrequency() {
        return 0;
    }

    @Override
    public void setHeartbeatFrequency(int heartbeatFrequency) {

    }

    @Override
    public ReadPreference getDefaultReadPreference() {
        return null;
    }

    @Override
    public void setDefaultReadPreference(ReadPreference rp) {

    }

    @Override
    public int getDefaultBatchSize() {
        return 0;
    }

    @Override
    public void setDefaultBatchSize(int defaultBatchSize) {

    }

    @Override
    public boolean isUseSSL() {
        return false;
    }

    @Override
    public void setUseSSL(boolean useSSL) {

    }

    @Override
    public boolean isDefaultJ() {
        return false;
    }

    @Override
    public void setDefaultJ(boolean j) {

    }

    @Override
    public void watch(WatchCommand settings) throws MorphiumDriverException {

    }

    @Override
    public MongoConnection getReadConnection(ReadPreference rp) {
        return null;
    }

    @Override
    public MongoConnection getPrimaryConnection(WriteConcern wc) {
        return null;
    }

    @Override
    public void releaseConnection(MongoConnection con) {

    }

    @Override
    public boolean exists(String db, String coll) throws MorphiumDriverException {
        return false;
    }

    @Override
    public boolean exists(String db) throws MorphiumDriverException {
        return false;
    }

    @Override
    public BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered, WriteConcern wc) {
        return null;
    }

    @Override
    public Map<DriverStatsKey, Double> getDriverStats() {
        return null;
    }

    @Override
    public int getIdleSleepTime() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setIdleSleepTime(int sl) {
        // TODO Auto-generated method stub

    }

    @Override
    public String toString() {
        return "DriverMock{}";
    }

    @Override
    public void closeConnection(MongoConnection con) {
        // TODO Auto-generated method stub

    }
}
