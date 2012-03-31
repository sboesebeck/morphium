/**
 * Created on 15.06.2005
 *
 * @version $Id: RecordEditDialog.java,v 1.2 2008-02-24 19:22:31 stephan Exp $
 */
package de.caluga.morphium.gui.recordedit;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.PanelClass;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Zeigt das für diesen Datentyp richtige Eingabefeld in einem Dialog an der
 * Klassenname des Eingabefeldes wird über Reflection bestimmt und muss
 * datapage.gui.CLASSENNAMEPanel lauten der Defaultconstructor
 * wird verwendet
 *
 * @author stephan
 */
public class RecordEditDialog extends JDialog {

    /**
     *
     */
    private static final long serialVersionUID = -6826395517605411119L;
    JButton okBtn, cancelBtn;
    private boolean confirmed;
    private RecordEditPanel pnl;
    private ArrayList listeners;
    private ArrayList modListeners;
    private ArrayList abortListeners;
    private static final Logger log = Logger.getLogger(RecordEditDialog.class);

    public RecordEditDialog(Object r, String title) {
        this(r, title, false);
    }

    public RecordEditDialog(Object r, String title, boolean viewOnly) {
        ObjectId id=Morphium.get().getId(r);

        okBtn = new JButton(RecordEditPanel.STATUS_OK.toString());
        cancelBtn = new JButton(RecordEditPanel.STATUS_CANCEL.toString());
        listeners = new ArrayList();
        modListeners = new ArrayList();
        abortListeners = new ArrayList();
        setTitle(title);
        getContentPane().setLayout(new BorderLayout());
        setModal(true);


        if (r==null) {
            throw new IllegalArgumentException("Object is null???");
        }

        Class<? extends Object> cls = r.getClass();
        PanelClass pcAn = cls.getAnnotation(PanelClass.class);
        if (pcAn == null) {
            throw new IllegalArgumentException("This type does not support gui editing!");
        }
        String clsName = cls.getSimpleName();
        String pack = cls.getPackage().getName();
        pnl = null;
        if (pcAn != null && pcAn.value() != null) {
            try {
                pnl = (RecordEditPanel) pcAn.value().newInstance();
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Could not instanciate " + pcAn.value(), e);
            } catch (InstantiationException e) {
                throw new RuntimeException(e);
            }
        }
        if (pnl == null) {
            String pnlClass = pack + "." + clsName + "Panel";
            try {
                pnl = (RecordEditPanel) Class.forName(pnlClass).newInstance();

            } catch (Exception e3) {
                log.warn("Could not instanciate " + pnlClass + ":" + e3.getMessage(), e3);
            }
            pack = pack + ".gui";
            pnlClass = pack + "." + clsName + "Panel";
            log.info("Trying to instanciate " + pnlClass);
            try {
                pnl = (RecordEditPanel) Class.forName(pnlClass).newInstance();

            } catch (Exception e3) {
                log.warn("Could not instanciate " + pnlClass + ":" + e3.getMessage(), e3);
            }
            if (pnl == null) {
                JOptionPane.showMessageDialog(RecordEditDialog.this, "Fehler beim laden des Config-Panels für " + clsName, "Fehler", JOptionPane.ERROR_MESSAGE);
                throw new RuntimeException("Fehler beim laden des Config-Panels für " + clsName);
            }
        }
        pnl.setViewOnly(viewOnly);
        okBtn.setVisible(!viewOnly);
        cancelBtn.setText(viewOnly ? "close" : "cancel");
        pnl.setRecord(r);
        getContentPane().add(pnl, BorderLayout.CENTER);
        JPanel bpnl = new JPanel();
        DateFormat df = DateFormat.getDateTimeInstance();
        String usr = "";

        if (r != null) {
            if (Morphium.get().storesLastChange(cls)) {
                final String lastChangeByField = Morphium.get().getLastChangeByField(cls);

                if (lastChangeByField != null) {
                    try {
                        usr = (String) Morphium.get().getValue(r, lastChangeByField);
                    } catch (IllegalAccessException e) {
                        log.error("Error accessing last change by-field - IllegalAccess!");
                    }
                }
            }
        }

        if (r != null && Morphium.get().storesLastChange(cls)) {
            try {
                bpnl.add(new JLabel("letzte Änderung: " + usr + " am "
                        + df.format(Morphium.get().getLongValue(r, Morphium.get().getLastChangeField(cls)))));
            } catch (IllegalAccessException e) {
                log.error("Error accessing last_change!!! Illegal access");
            }
        }


        List<String> flds = Morphium.get().getFields(cls);

        if (flds.contains("modified")) {
            try {
                if (Morphium.get().getValue(r, "modified") != null) {
                    String von = "";
                    if (flds.contains("modified_by_user")) {
                        von = "von " + (String) Morphium.get().getValue(r, "modified_by_user");
                    }
                    bpnl.add(new JLabel("letzte Änderung " + von + " am " + df.format(Morphium.get().getValue(r, "modified"))));
                } else {
                    bpnl.add(new JLabel("Neuer Datensatz"));
                }
            } catch (IllegalAccessException e) {
                log.error("Illegal Access!");
            }
        }
        bpnl.add(okBtn);
        bpnl.add(cancelBtn);
        getContentPane().add(bpnl, BorderLayout.SOUTH);
        okBtn.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent e) {
                boolean isNew = false;
                if (Morphium.get().getId(pnl.getRecord()) == null) {
                    isNew = true;
                }
                try {
                    pnl.updateRecord();
                } catch (UpdateException e2) {
                    JOptionPane.showMessageDialog(RecordEditDialog.this, e2.getMessage(), "Fehler",
                            JOptionPane.ERROR_MESSAGE);
                    log.warn("Error:" + e2.getMessage(), e2);
                    return;
                }
                try {
                    fireRecordModifiedEvent(pnl.getRecord(), isNew);
                } catch (RecordModificationException ex) {
                    JOptionPane.showMessageDialog(RecordEditDialog.this, ex.getMessage(), "Fehler",
                            JOptionPane.ERROR_MESSAGE);
                    return;
                }

                Morphium.get().store(pnl.getRecord());


                dispose();
                confirmed = true;
                try {
                    fireRecordChangedEvent(pnl.getRecord(), isNew);
                } catch (RecordModificationException e1) {
                    log.warn("Error:" + e1.getMessage(), e1);
                }

            }
        });
        cancelBtn.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent e) {
                confirmed = false;
                fireAbortEvent(pnl.getRecord());
                dispose();
            }
        });
        pack();
        setLocation((int) (Toolkit.getDefaultToolkit().getScreenSize().getWidth() / 2 - getWidth() / 2), (int) (Toolkit.getDefaultToolkit().getScreenSize().getHeight() / 2 - getHeight() / 2));
    }

    public Component add(Component c) {
        return getContentPane().add(c);
    }

    /**
     * die ModifiedListener werde darüber informiert, dass sich der MongoDbObject im
     * Speicher geändert hat _bevor_ die Änderungen in der DB gespeichert werden
     *
     * @param l
     */
    public void addRecordModifiedListener(RecordChangeListener l) {
        modListeners.add(l);
    }

    /**
     * die ModifiedListener werde darüber informiert, dass sich der MongoDbObject im
     * Speicher geändert hat _bevor_ die Änderungen in der DB gespeichert werden,
     * über eine RecordModifiedException kann die speicherung noch verhindert
     * werden
     *
     * @param l
     */
    public void removeRecordModifiedListener(RecordChangeListener l) {
        modListeners.remove(l);
    }

    /**
     * die ModifiedListener werde darüber informiert, dass sich der MongoDbObject im
     * Speicher geändert hat _bevor_ die Änderungen in der DB gespeichert werden,
     * über eine RecordModifiedException kann die speicherung noch verhindert
     * werden
     *
     * @param r -
     *          der zu speichernde MongoDbObject
     * @param n -
     *          neu oder nicht neu
     * @throws RecordModificationException
     */
    public void fireRecordModifiedEvent(Object r, boolean n) throws RecordModificationException {
        RecordChangedEvent evt = new RecordChangedEvent(r, n);
        for (Object modListener : modListeners) {
            RecordChangeListener l = (RecordChangeListener) modListener;
            l.recordChanged(evt);
        }
    }

    /**
     * Die RecordChangeListener werden _nach_ speicherung der Daten in der DB
     * informiert
     *
     * @param l
     */
    public void addRecordChangedListener(RecordChangeListener l) {
        listeners.add(l);
    }

    /**
     * Die RecordChangeListener werden _nach_ speicherung der Daten in der DB
     * informiert
     *
     * @param l
     */
    public void removeRecordChangeListener(RecordChangeListener l) {
        listeners.remove(l);
    }

    public void addEditAbortListener(EditAbortListener l) {
        abortListeners.add(l);
    }

    public void fireAbortEvent(Object r) {
        for (Object abortListener : abortListeners) {
            EditAbortListener l = (EditAbortListener) abortListener;
            l.editAborted(r);
        }
    }

    public void removeEditAbortListener(EditAbortListener l) {
        abortListeners.remove(l);
    }

    /**
     * Die RecordChangeListener werden _nach_ speicherung der Daten in der DB
     * informiert
     *
     * @param r -
     *          der gespeicherte MongoDbObject
     * @param n -
     *          neu oder nicht
     * @throws RecordModificationException
     */
    public void fireRecordChangedEvent(Object r, boolean n) throws RecordModificationException {
        RecordChangedEvent evt = new RecordChangedEvent(r, n);
        for (Object listener : listeners) {
            RecordChangeListener l = (RecordChangeListener) listener;
            l.recordChanged(evt);
        }
    }

    public boolean isConfirmed() {
        return confirmed;
    }
}
