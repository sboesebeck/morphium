package de.caluga.morphium.gui.recordedit;

/**
 * dieses Interface wird vom RecordEditDialog verwendet um bekannt zu geben,
 * dass sich ein Object geändert hat Das RecordChangedEvent beinhaltet die Daten
 * Wird verwendet um vor und nach einer Änderung Aktionen auszulösen
 * (addRecordChangedListener und addRecordModifiedListener in RecordEditDialog)
 *
 * @author stephan
 */
public interface RecordChangeListener {
    public void recordChanged(RecordChangedEvent e) throws RecordModificationException;
}
