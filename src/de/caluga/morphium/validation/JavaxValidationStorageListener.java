package de.caluga.morphium.validation;

import de.caluga.morphium.*;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;

import javax.validation.*;
import java.lang.reflect.Field;
import java.util.*;

/**
 * User: martinstolz
 * Date: 29.08.12
 */
@SuppressWarnings({"ConstantConditions", "unchecked"})
public class JavaxValidationStorageListener extends MorphiumStorageAdapter<Object> {

    private final Validator validator;

    public JavaxValidationStorageListener() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }


    @Override
    public void preStore(Morphium m, Map<Object, Boolean> isNew) throws MorphiumAccessVetoException {
        for (Object b : isNew.keySet()) {
            preStore(m, b, isNew.get(b));
        }
    }

    @Override
    public void preStore(Morphium m, Object r, boolean isNew) {
        if (r == null) {
            return;
        }
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
                    if (field.get(r) == null) {
                        continue;
                    }
                    Set<ConstraintViolation<Object>> v = validator.validate(field.get(r));
                    violations.addAll(v);
                } catch (IllegalAccessException e) {
                    new Logger(JavaxValidationStorageListener.class).error("Could not access Field: " + f);
                }
            } else if (Collection.class.isAssignableFrom(field.getType())) {
                //list handling
                try {
                    Collection<Object> lst = (List<Object>) field.get(r);
                    if (lst != null) {
                        Map<Object, Boolean> map = new HashMap<>();
                        for (Object o : lst) {
                            map.put(o, isNew);
                        }
                        validatePrestor(m, violations, map);

                    }

                } catch (IllegalAccessException e) {
                    new Logger(JavaxValidationStorageListener.class).error("Could not access list field: " + f);
                }
            } else if (Map.class.isAssignableFrom(field.getType())) {
                //usually only strings are allowed as keys - especially no embedded or entitiy types
                //just checking values
                try {
                    Map map = (Map) field.get(r);
                    if (map != null) {
                        Map<Object, Boolean> lst = new HashMap<>();
                        for (Object val : map.values()) {
                            lst.put(val, isNew);
                        }
                        validatePrestor(m, violations, lst);
                    }

                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        if (!violations.isEmpty()) {
            throw new ConstraintViolationException(new HashSet<>(violations));
        }
    }

    private void validatePrestor(Morphium m, Set<ConstraintViolation<Object>> violations, Map<Object, Boolean> map) {
        try {
            preStore(m, map);
        } catch (ConstraintViolationException e) {
            Set<ConstraintViolation<?>> constraintViolations = e.getConstraintViolations();
            //ADDall not possible due to generics mess
            //noinspection Convert2streamapi
            for (ConstraintViolation v : constraintViolations) violations.add(v);
        }
    }

}
