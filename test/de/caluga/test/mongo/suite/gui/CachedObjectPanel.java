/*
 * Created by JFormDesigner on Sat Mar 31 15:49:57 CEST 2012
 */

package de.caluga.test.mongo.suite.gui;

import javax.swing.*;
import com.jgoodies.forms.factories.*;
import com.jgoodies.forms.layout.*;
import de.caluga.morphium.gui.recordedit.*;
import de.caluga.test.mongo.suite.CachedObject;

/**
 * @author Stephan Bösebeck
 */
public class CachedObjectPanel extends RecordEditPanel<CachedObject> {
    public CachedObjectPanel() {
        initComponents();
    }

    @Override
    public void updateRecord() throws UpdateException {
        getRecord().setCounter(slider1.getValue());
        getRecord().setValue(textField1.getText());
    }

    @Override
    public void updateView() {
        slider1.setValue(getRecord().getCounter());
        textField1.setText(getRecord().getValue());
    }

    private void initComponents() {
        // JFormDesigner - Component initialization - DO NOT MODIFY  //GEN-BEGIN:initComponents
        label1 = new JLabel();
        slider1 = new JSlider();
        label2 = new JLabel();
        textField1 = new JTextField();
        label3 = new JLabel();
        CellConstraints cc = new CellConstraints();

        //======== this ========
        setLayout(new FormLayout(
            new ColumnSpec[] {
                FormFactory.DEFAULT_COLSPEC,
                FormFactory.LABEL_COMPONENT_GAP_COLSPEC,
                new ColumnSpec(ColumnSpec.FILL, Sizes.DEFAULT, FormSpec.DEFAULT_GROW)
            },
            new RowSpec[] {
                FormFactory.DEFAULT_ROWSPEC,
                FormFactory.LINE_GAP_ROWSPEC,
                FormFactory.DEFAULT_ROWSPEC,
                FormFactory.LINE_GAP_ROWSPEC,
                FormFactory.DEFAULT_ROWSPEC,
                FormFactory.LINE_GAP_ROWSPEC,
                FormFactory.DEFAULT_ROWSPEC,
                FormFactory.LINE_GAP_ROWSPEC,
                FormFactory.DEFAULT_ROWSPEC,
                FormFactory.LINE_GAP_ROWSPEC,
                FormFactory.DEFAULT_ROWSPEC
            }));

        //---- label1 ----
        label1.setText("Counter:");
        add(label1, cc.xy(1, 3));

        //---- slider1 ----
        slider1.setPaintTicks(true);
        slider1.setMaximum(1000);
        add(slider1, cc.xy(3, 3, CellConstraints.FILL, CellConstraints.FILL));

        //---- label2 ----
        label2.setText("Value");
        add(label2, cc.xy(1, 7));
        add(textField1, cc.xy(3, 7));

        //---- label3 ----
        label3.setText("Id: ");
        add(label3, cc.xywh(1, 11, 3, 1));
        // JFormDesigner - End of component initialization  //GEN-END:initComponents
    }

    // JFormDesigner - Variables declaration - DO NOT MODIFY  //GEN-BEGIN:variables
    private JLabel label1;
    private JSlider slider1;
    private JLabel label2;
    private JTextField textField1;
    private JLabel label3;
    // JFormDesigner - End of variables declaration  //GEN-END:variables
}
