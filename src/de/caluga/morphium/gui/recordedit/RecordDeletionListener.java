package de.caluga.morphium.gui.recordedit;

public interface RecordDeletionListener {
    public void recordDeleted(Object selectedRecord) throws RecordDeletionException;
}
