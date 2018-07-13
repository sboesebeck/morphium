package de.caluga.morphium.changestream;

public interface ChangeStreamListener {
    /**
     * return true, if you want to continue getting events.
     *
     * @param evt
     * @return
     */
    public boolean incomingData(ChangeStreamEvent evt);
}
