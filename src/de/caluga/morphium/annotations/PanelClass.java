package de.caluga.morphium.annotations;

import javax.swing.*;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * User: Stephan Bösebeck
 * Date: 13.03.12
 * Time: 21:57
 * <p/>
 * TODO: Add documentation here
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface PanelClass {
    Class<? extends JPanel> value();
}
