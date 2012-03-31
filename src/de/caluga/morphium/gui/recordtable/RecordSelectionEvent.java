package de.caluga.morphium.gui.recordtable;

public class RecordSelectionEvent<T> {
    private T record;
    private int row;

    public RecordSelectionEvent(T record, int row) {
        this.record = record;
        this.row = row;
    }

    public T getRecord() {
        return record;
    }

    public int getRow() {
        return row;
    }
}
