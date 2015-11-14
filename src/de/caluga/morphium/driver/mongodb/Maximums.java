package de.caluga.morphium.driver.mongodb;

/**
 * User: Stephan Bösebeck
 * Date: 14.11.15
 * Time: 22:57
 * <p>
 * TODO: Add documentation here
 */
public class Maximums {
    private Integer maxBsonSize;
    private Integer maxMessageSize;
    private Integer maxWriteBatchSize;

    public void setMaxBsonSize(Integer maxBsonSize) {
        this.maxBsonSize = maxBsonSize;
    }

    public Integer getMaxBsonSize() {
        return maxBsonSize;
    }

    public void setMaxMessageSize(Integer maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public Integer getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxWriteBatchSize(Integer maxWriteBatchSize) {
        this.maxWriteBatchSize = maxWriteBatchSize;
    }

    public Integer getMaxWriteBatchSize() {
        return maxWriteBatchSize;
    }
}
