/*
 * Created by JFormDesigner on Mon Feb 27 22:05:33 CET 2012
 */

package de.caluga.test.mongo.suite.gui;

import com.jgoodies.forms.factories.Borders;
import de.caluga.morphium.MongoDbMode;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.gui.recordtable.RecordTable;
import de.caluga.morphium.gui.recordtable.RecordTableState;
import de.caluga.morphium.secure.DefaultSecurityManager;
import de.caluga.test.mongo.suite.CachedObject;
import org.apache.log4j.Logger;
import org.jdesktop.swingx.HorizontalLayout;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;

/**
 * @author unknown
 */
public class TestRecordTab extends JFrame {
    private static Logger log = Logger.getLogger(TestRecordTab.class);
    private RecordTableState<CachedObject> state;

    public TestRecordTab() {
        initComponents();

    }

    public static void main(String args[]) throws Exception {
        MorphiumConfig cfg = new MorphiumConfig("testdb", MongoDbMode.SINGLE, 5, 50000, 5000, new DefaultSecurityManager(), "logging_test.properties");
        cfg.addAddress("localhost", 27017);
        cfg.setWriteCacheTimeout(100);

        if (!Morphium.isConfigured()) {
            Morphium.setConfig(cfg);
        }

        Morphium.get();
        Morphium.get().dropCollection(CachedObject.class);
        Morphium.get().ensureIndex(CachedObject.class,"counter");
        Morphium.get().ensureIndex(CachedObject.class,"value");

        for (int i =0 ; i<100; i++) {
            CachedObject o=new CachedObject();
            o.setCounter(i);
            o.setValue("Counter: "+i);
            Morphium.get().store(o);
        }

        TestRecordTab t = new TestRecordTab();
        t.addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent windowEvent) {
                System.exit(0);
            }
        });
        t.setVisible(true);

    }

    private RecordTableState<CachedObject> getInitialState() {
        state = new RecordTableState<CachedObject>(CachedObject.class);
        state.setFieldsToShow(Morphium.get().getFields(CachedObject.class));
        state.setSearchable(true);
        state.setPaging(true);
        state.setPageLength(50);
        state.setPreCacheAll(true);

        return state;
    }

    private RecordTable<CachedObject> getRecordTable() {
        RecordTable<CachedObject> ret = new RecordTable<CachedObject>(CachedObject.class, getInitialState());
        return ret;
    }

    private void createUIComponents() {
        log.info("Creating recordTable");
        recordTable1 = getRecordTable();

    }

    private void okButtonActionPerformed(ActionEvent e) {
        System.exit(0);
    }

    private void checkBoxMenuItem1ActionPerformed(ActionEvent e) {
        state.setPaging(checkBoxMenuItem1.isSelected());
        recordTable1.updateView();
    }

    private void checkBoxMenuItem2ActionPerformed(ActionEvent e) {
        state.setEditable(checkBoxMenuItem2.isSelected());
        recordTable1.updateView();
    }

    private void checkBoxMenuItem3ActionPerformed(ActionEvent e) {
        state.setSearchable(checkBoxMenuItem3.isSelected());
        recordTable1.updateView();
    }

    private void checkBoxMenuItem4ActionPerformed(ActionEvent e) {
        state.setDeleteable(checkBoxMenuItem4.isSelected());
        recordTable1.updateView();
    }


    private void initComponents() {
        // JFormDesigner - Component initialization - DO NOT MODIFY  //GEN-BEGIN:initComponents
        createUIComponents();

        menuBar1 = new JMenuBar();
        menu1 = new JMenu();
        checkBoxMenuItem3 = new JCheckBoxMenuItem();
        checkBoxMenuItem1 = new JCheckBoxMenuItem();
        checkBoxMenuItem2 = new JCheckBoxMenuItem();
        checkBoxMenuItem4 = new JCheckBoxMenuItem();
        menuItem1 = new JMenuItem();
        dialogPane = new JPanel();
        contentPanel = new JPanel();
        buttonBar = new JPanel();
        okButton = new JButton();

        //======== this ========
        Container contentPane = getContentPane();
        contentPane.setLayout(new BorderLayout());

        //======== menuBar1 ========
        {

            //======== menu1 ========
            {
                menu1.setText("File");

                //---- checkBoxMenuItem3 ----
                checkBoxMenuItem3.setText("searchable");
                checkBoxMenuItem3.setSelected(true);
                checkBoxMenuItem3.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        checkBoxMenuItem3ActionPerformed(e);
                    }
                });
                menu1.add(checkBoxMenuItem3);

                //---- checkBoxMenuItem1 ----
                checkBoxMenuItem1.setText("paging");
                checkBoxMenuItem1.setSelected(true);
                checkBoxMenuItem1.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        checkBoxMenuItem1ActionPerformed(e);
                    }
                });
                menu1.add(checkBoxMenuItem1);

                //---- checkBoxMenuItem2 ----
                checkBoxMenuItem2.setText("editable");
                checkBoxMenuItem2.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        checkBoxMenuItem2ActionPerformed(e);
                    }
                });
                menu1.add(checkBoxMenuItem2);

                //---- checkBoxMenuItem4 ----
                checkBoxMenuItem4.setText("deleteable");
                checkBoxMenuItem4.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        checkBoxMenuItem4ActionPerformed(e);
                    }
                });
                menu1.add(checkBoxMenuItem4);

                //---- menuItem1 ----
                menuItem1.setText("exit");
                menu1.add(menuItem1);
            }
            menuBar1.add(menu1);
        }
        setJMenuBar(menuBar1);

        //======== dialogPane ========
        {
            dialogPane.setBorder(Borders.DIALOG_BORDER);
            dialogPane.setLayout(new BorderLayout());

            //======== contentPanel ========
            {
                contentPanel.setLayout(new BorderLayout());
                contentPanel.add(recordTable1, BorderLayout.CENTER);
            }
            dialogPane.add(contentPanel, BorderLayout.CENTER);

            //======== buttonBar ========
            {
                buttonBar.setBorder(Borders.BUTTON_BAR_GAP_BORDER);
                buttonBar.setLayout(new HorizontalLayout());

                //---- okButton ----
                okButton.setText("OK");
                okButton.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        okButtonActionPerformed(e);
                    }
                });
                buttonBar.add(okButton);
            }
            dialogPane.add(buttonBar, BorderLayout.SOUTH);
        }
        contentPane.add(dialogPane, BorderLayout.CENTER);
        pack();
        setLocationRelativeTo(getOwner());
        // JFormDesigner - End of component initialization  //GEN-END:initComponents
    }

    // JFormDesigner - Variables declaration - DO NOT MODIFY  //GEN-BEGIN:variables
    private JMenuBar menuBar1;
    private JMenu menu1;
    private JCheckBoxMenuItem checkBoxMenuItem3;
    private JCheckBoxMenuItem checkBoxMenuItem1;
    private JCheckBoxMenuItem checkBoxMenuItem2;
    private JCheckBoxMenuItem checkBoxMenuItem4;
    private JMenuItem menuItem1;
    private JPanel dialogPane;
    private JPanel contentPanel;
    private RecordTable<CachedObject> recordTable1;
    private JPanel buttonBar;
    private JButton okButton;
    // JFormDesigner - End of variables declaration  //GEN-END:variables
}
