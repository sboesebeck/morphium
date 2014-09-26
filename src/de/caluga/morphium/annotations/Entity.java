package de.caluga.morphium.annotations;

import de.caluga.morphium.DefaultNameProvider;
import de.caluga.morphium.NameProvider;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 11:14
 * <p/>
 */
@Target({TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface Entity {
    String collectionName() default ".";

    boolean translateCamelCase() default true;

    /**
     * use Full Qualified Name as collection name
     *
     * @return
     */
    boolean useFQN() default false;

    /**
     * several different objects of same type stored in one collection
     * if set, className is  stored in object
     */
    boolean polymorph() default false;

    String typeId() default ".";

    Class<? extends NameProvider> nameProvider() default DefaultNameProvider.class;
}
