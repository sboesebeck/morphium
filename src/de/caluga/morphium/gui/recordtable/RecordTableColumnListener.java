package de.caluga.morphium.gui.recordtable;

public interface RecordTableColumnListener {
    /**
     * Wird aufgerufen, wenn eine Programmierbare Spalte der MongoDbObject Table
     * angezeigt werden soll column ist der Headername der Spalte, MongoDbObject der
     * aktuelle MongoDbObject Rückgabewert ist das darzustellende ding...
     *
     * @param r: MongoDbObject
     * @return value to show
     */
    public Object getVisibleValue(Object r, String header);
}
