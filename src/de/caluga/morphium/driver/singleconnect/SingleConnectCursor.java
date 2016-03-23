package de.caluga.morphium.driver.singleconnect;

/**
 * User: Stephan Bösebeck
 * Date: 23.03.16
 * Time: 16:13
 * <p>
 * TODO: Add documentation here
 */
public class SingleConnectCursor {
    private String db;
    private String collection;
    private int batchSize;

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getDb() {

        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }
}
