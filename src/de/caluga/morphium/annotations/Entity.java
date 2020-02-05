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
     * if true, use auto versioning. then you also need a field of type long with a version annotation
     *
     * @return
     */
    boolean autoVersioning() default false;

    // specify type id, if . classname is used - migration problem on type change
    // either help migrating or force using id
    String typeId() default ".";

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

    boolean validate() default false;

    Class<? extends NameProvider> nameProvider() default DefaultNameProvider.class;
}
