/**
 * Created on 13.06.2005
 *
 * @version $Id: RecordEditPanel.java,v 1.1 2007-08-06 11:47:32 stephan Exp $
 */
package de.caluga.morphium.gui.recordedit;

import javax.swing.*;


/**
 * Die Mutter aller MongoDbObject-Edit-Dialoge Alle diese Dialoge müssen davon
 * abgeleitet werden, damit die RecordTable korrekt funktioniert Die
 * Abgeleiteten klassen benötigen keinen OK/Cancel-Button, wird von
 * RecordEditDialog erzeugt
 *
 * @author stephan
 */
public abstract class RecordEditPanel<T> extends JPanel {
    protected boolean confirmed = false;
    private T theRecord;
    private boolean viewOnly = false;

    /**
     * hier passiert nix, nur wegen der Constructorvererbung
     */
    public RecordEditPanel() {
    }

    /**
     * den zu editierenden MongoDbObject
     *
     * @param MongoDbObject
     */
    public void setRecord(T MongoDbObject) {
        theRecord = MongoDbObject;
        updateView();
    }

    public void setViewOnly(boolean vo) {
        viewOnly = vo;
    }

    public boolean isViewOnly() {
        return viewOnly;
    }

    public T getRecord() {
        return theRecord;
    }

    /**
     * Diese Methode dient dazu, die dargestellten bzw. eingegebenen Werte im
     * MongoDbObject zu speichern Die updateException wird geworfen, wenn die eingaben
     * fehlerhaft sind
     *
     * @throws UpdateException
     */
    public abstract void updateRecord() throws UpdateException;

    /**
     * hier werden die Daten aus dem MongoDbObject in die sichtbaren Bedienelemente
     * kopiert
     */
    public abstract void updateView();

    public boolean isConfirmed() {
        return confirmed;
    }

    public static final String STATUS_OK = "ok";
    public static final String STATUS_CANCEL = "cancel";
}
