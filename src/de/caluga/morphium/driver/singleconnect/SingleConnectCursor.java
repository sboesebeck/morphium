package de.caluga.morphium.driver.singleconnect;

/**
 * User: Stephan Bösebeck
 * Date: 23.03.16
 * Time: 16:13
 * <p>
 * Cursor implementation for the singleconnect drivers
 */
public class SingleConnectCursor {


    private final DriverBase driver;
    private String db;
    private String collection;
    private int batchSize;

    public SingleConnectCursor(DriverBase drv) {
        this.driver = drv;
    }

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

    public DriverBase getDriver() {
        return driver;
    }
}
