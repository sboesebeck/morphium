package de.caluga.morphium.gui.recordedit;


public interface RecordModificationListener {
    public void recordModified(Object r) throws RecordModificationException;
}
