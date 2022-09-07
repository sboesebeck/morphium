package de.caluga.morphium.driver.bson;


public class MongoTimestamp {
    private final long value;

    public MongoTimestamp() {
        this.value = 0L;
    }

    public MongoTimestamp(long value) {
        this.value = value;
    }

    public MongoTimestamp(int seconds, int increment) {
        this.value = (long) seconds << 32 | (long) increment & 4294967295L;
    }

    public long getValue() {
        return this.value;
    }

    public int getTime() {
        return (int) (this.value >> 32);
    }

    public int getInc() {
        return (int) this.value;
    }

    public String toString() {
        return "Timestamp{value=" + this.getValue() + ", seconds=" + this.getTime() + ", inc=" + this.getInc() + '}';
    }

    public int compareTo(MongoTimestamp ts) {
        return Long.compareUnsigned(this.value, ts.value);
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            MongoTimestamp timestamp = (MongoTimestamp) o;
            return this.value == timestamp.value;
        } else {
            return false;
        }
    }

    public int hashCode() {
        return (int) (this.value ^ this.value >>> 32);
    }
}
