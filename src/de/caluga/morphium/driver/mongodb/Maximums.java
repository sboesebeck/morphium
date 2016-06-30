package de.caluga.morphium.driver.mongodb;

/**
 * storing all maximums determined by driver / database
 */
public class Maximums {
    private Integer maxBsonSize;
    private Integer maxMessageSize;
    private Integer maxWriteBatchSize;

    @SuppressWarnings("unused")
    public Integer getMaxBsonSize() {
        return maxBsonSize;
    }

    public void setMaxBsonSize(Integer maxBsonSize) {
        this.maxBsonSize = maxBsonSize;
    }

    @SuppressWarnings("unused")
    public Integer getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(Integer maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public Integer getMaxWriteBatchSize() {
        return maxWriteBatchSize;
    }

    public void setMaxWriteBatchSize(Integer maxWriteBatchSize) {
        this.maxWriteBatchSize = maxWriteBatchSize;
    }
}
