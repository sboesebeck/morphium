package de.caluga.morphium.validation;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumStorageAdapter;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import org.apache.log4j.Logger;

import javax.validation.*;
import java.lang.reflect.Field;
import java.util.*;

/**
 * User: martinstolz
 * Date: 29.08.12
 */
@SuppressWarnings({"ConstantConditions", "unchecked"})
public class JavaxValidationStorageListener extends MorphiumStorageAdapter<Object> {

    private Validator validator;

    public JavaxValidationStorageListener() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    @Override
    public void preStore(Morphium m, Object r, boolean isNew) {
        if (r == null) return;
        AnnotationAndReflectionHelper annotationHelper = m.getARHelper();
        if (!annotationHelper.isAnnotationPresentInHierarchy(r.getClass(), Entity.class) && !annotationHelper.isAnnotationPresentInHierarchy(r.getClass(), Embedded.class)) {
            return;
        }
        Set<ConstraintViolation<Object>> violations = validator.validate(r);
        List<String> flds = annotationHelper.getFields(r.getClass());
        for (String f : flds) {
            Field field = annotationHelper.getField(r.getClass(), f);
            if (annotationHelper.isAnnotationPresentInHierarchy(field.getType(), Embedded.class) ||
                    annotationHelper.isAnnotationPresentInHierarchy(field.getType(), Entity.class)) {
                //also check it
                try {
                    if (field.get(r) == null) continue;
                    Set<ConstraintViolation<Object>> v = validator.validate(field.get(r));
                    violations.addAll(v);
                } catch (IllegalAccessException e) {
                    Logger.getLogger(JavaxValidationStorageListener.class).error("Could not access Field: " + f);
                }
            } else if (Collection.class.isAssignableFrom(field.getType())) {
                //list handling
                try {
                    Collection<Object> lst = (List<Object>) field.get(r);
                    if (lst != null) {
                        for (Object o : lst) {
                            try {
                                preStore(m, o, isNew);
                            } catch (ConstraintViolationException e) {
                                Set<ConstraintViolation<?>> constraintViolations = e.getConstraintViolations();
                                for (ConstraintViolation v : constraintViolations) {
                                    violations.add(v);
                                }
                            }
                        }
                    }

                } catch (IllegalAccessException e) {
                    Logger.getLogger(JavaxValidationStorageListener.class).error("Could not access list field: " + f);
                }
            } else if (Map.class.isAssignableFrom(field.getType())) {
                //usually only strings are allowed as keys - especially no embedded or entitiy types
                //just checking values
                try {
                    Map map = (Map) field.get(r);
                    if (map != null) {
                        for (Object val : map.values()) {
                            try {
                                preStore(m, val, isNew);
                            } catch (ConstraintViolationException e) {
                                Set<ConstraintViolation<?>> constraintViolations = e.getConstraintViolations();
                                for (ConstraintViolation v : constraintViolations) {
                                    violations.add(v);
                                }
                            }
                        }
                    }
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        if (violations.size() > 0) {
            throw new ConstraintViolationException(new HashSet<ConstraintViolation<?>>(violations));
        }
    }

}
