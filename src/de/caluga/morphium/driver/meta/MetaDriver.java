package de.caluga.morphium.driver.meta;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.WriteConcern;
import de.caluga.morphium.driver.bulk.BulkRequestContext;
import de.caluga.morphium.driver.mongodb.Maximums;

import java.util.List;
import java.util.Map;
import java.util.Vector;

/**
 * User: Stephan Bösebeck
 * Date: 02.12.15
 * Time: 23:56
 * <p>
 * TODO: Add documentation here
 */
public class MetaDriver implements MorphiumDriver {

    Vector<MorphiumDriver> pool = new Vector<>();


    @Override
    public void setCredentials(String db, String login, char[] pwd) {

    }

    @Override
    public boolean isReplicaset() {
        return false;
    }

    @Override
    public String[] getCredentials(String db) {
        return new String[0];
    }

    @Override
    public boolean isDefaultFsync() {
        return false;
    }

    @Override
    public String[] getHostSeed() {
        return new String[0];
    }

    @Override
    public int getMaxConnectionsPerHost() {
        return 0;
    }

    @Override
    public int getMinConnectionsPerHost() {
        return 0;
    }

    @Override
    public int getMaxConnectionLifetime() {
        return 0;
    }

    @Override
    public int getMaxConnectionIdleTime() {
        return 0;
    }

    @Override
    public int getSocketTimeout() {
        return 0;
    }

    @Override
    public int getConnectionTimeout() {
        return 0;
    }

    @Override
    public int getDefaultW() {
        return 0;
    }

    @Override
    public int getMaxBlockintThreadMultiplier() {
        return 0;
    }

    @Override
    public int getHeartbeatFrequency() {
        return 0;
    }

    @Override
    public void setHeartbeatSocketTimeout(int heartbeatSocketTimeout) {

    }

    @Override
    public void setUseSSL(boolean useSSL) {

    }

    @Override
    public void setHeartbeatFrequency(int heartbeatFrequency) {

    }

    @Override
    public void setWriteTimeout(int writeTimeout) {

    }

    @Override
    public void setDefaultBatchSize(int defaultBatchSize) {

    }

    @Override
    public void setCredentials(Map<String, String[]> credentials) {

    }

    @Override
    public int getHeartbeatSocketTimeout() {
        return 0;
    }

    @Override
    public boolean isUseSSL() {
        return false;
    }

    @Override
    public boolean isDefaultJ() {
        return false;
    }

    @Override
    public int getWriteTimeout() {
        return 0;
    }

    @Override
    public int getLocalThreshold() {
        return 0;
    }

    @Override
    public void setHostSeed(String... host) {

    }

    @Override
    public void setMaxConnectionsPerHost(int mx) {

    }

    @Override
    public void setMinConnectionsPerHost(int mx) {

    }

    @Override
    public void setMaxConnectionLifetime(int timeout) {

    }

    @Override
    public void setMaxConnectionIdleTime(int time) {

    }

    @Override
    public void setSocketTimeout(int timeout) {

    }

    @Override
    public void setConnectionTimeout(int timeout) {

    }

    @Override
    public void setDefaultW(int w) {

    }

    @Override
    public void setMaxBlockingThreadMultiplier(int m) {

    }

    @Override
    public void heartBeatFrequency(int t) {

    }

    @Override
    public void heartBeatSocketTimeout(int t) {

    }

    @Override
    public void useSsl(boolean ssl) {

    }

    @Override
    public void connect() throws MorphiumDriverException {

    }

    @Override
    public void setDefaultReadPreference(ReadPreference rp) {

    }

    @Override
    public void connect(String replicasetName) throws MorphiumDriverException {

    }

    @Override
    public Maximums getMaximums() {
        return null;
    }

    @Override
    public boolean isConnected() {
        return false;
    }

    @Override
    public void setDefaultJ(boolean j) {

    }

    @Override
    public void setDefaultWriteTimeout(int wt) {

    }

    @Override
    public void setLocalThreshold(int thr) {

    }

    @Override
    public void setDefaultFsync(boolean j) {

    }

    @Override
    public void setRetriesOnNetworkError(int r) {

    }

    @Override
    public int getRetriesOnNetworkError() {
        return 0;
    }

    @Override
    public void setSleepBetweenErrorRetries(int s) {

    }

    @Override
    public int getSleepBetweenErrorRetries() {
        return 0;
    }

    @Override
    public void close() throws MorphiumDriverException {

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
    public Map<String, Object> getOps(long threshold) throws MorphiumDriverException {
        return null;
    }

    @Override
    public Map<String, Object> runCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException {
        return null;
    }

    @Override
    public List<Map<String, Object>> find(String db, String collection, Map<String, Object> query, Map<String, Integer> sort, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference rp, Map<String, Object> findMetaData) throws MorphiumDriverException {
        return null;
    }

    @Override
    public long count(String db, String collection, Map<String, Object> query, ReadPreference rp) throws MorphiumDriverException {
        return 0;
    }

    @Override
    public void insert(String db, String collection, List<Map<String, Object>> objs, WriteConcern wc) throws MorphiumDriverException {

    }

    @Override
    public void store(String db, String collection, List<Map<String, Object>> objs, WriteConcern wc) throws MorphiumDriverException {

    }

    @Override
    public Map<String, Object> update(String db, String collection, Map<String, Object> query, Map<String, Object> op, boolean multiple, boolean upsert, WriteConcern wc) throws MorphiumDriverException {
        return null;
    }

    @Override
    public Map<String, Object> delete(String db, String collection, Map<String, Object> query, boolean multiple, WriteConcern wc) throws MorphiumDriverException {
        return null;
    }

    @Override
    public void drop(String db, String collection, WriteConcern wc) throws MorphiumDriverException {

    }

    @Override
    public void drop(String db, WriteConcern wc) throws MorphiumDriverException {

    }

    @Override
    public boolean exists(String db) throws MorphiumDriverException {
        return false;
    }

    @Override
    public List<Object> distinct(String db, String collection, String field, Map<String, Object> filter) throws MorphiumDriverException {
        return null;
    }

    @Override
    public boolean exists(String db, String collection) throws MorphiumDriverException {
        return false;
    }

    @Override
    public List<Map<String, Object>> getIndexes(String db, String collection) throws MorphiumDriverException {
        return null;
    }

    @Override
    public List<String> getCollectionNames(String db) throws MorphiumDriverException {
        return null;
    }

    @Override
    public Map<String, Object> group(String db, String coll, Map<String, Object> query, Map<String, Object> initial, String jsReduce, String jsFinalize, ReadPreference rp, String... keys) {
        return null;
    }

    @Override
    public List<Map<String, Object>> aggregate(String db, String collection, List<Map<String, Object>> pipeline, boolean explain, boolean allowDiskUse, ReadPreference readPreference) throws MorphiumDriverException {
        return null;
    }

    @Override
    public boolean isSocketKeepAlive() {
        return false;
    }

    @Override
    public void setSocketKeepAlive(boolean socketKeepAlive) {

    }

    @Override
    public int getHeartbeatConnectTimeout() {
        return 0;
    }

    @Override
    public void setHeartbeatConnectTimeout(int heartbeatConnectTimeout) {

    }

    @Override
    public int getMaxWaitTime() {
        return 0;
    }

    @Override
    public void setMaxWaitTime(int maxWaitTime) {

    }

    @Override
    public boolean isCapped(String db, String coll) throws MorphiumDriverException {
        return false;
    }

    @Override
    public BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered, WriteConcern wc) {
        return null;
    }

    @Override
    public void createIndex(String db, String collection, Map<String, Object> index, Map<String, Object> options) throws MorphiumDriverException {

    }
}
