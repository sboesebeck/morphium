package de.caluga.morphium.validation;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumAccessVetoException;
import de.caluga.morphium.MorphiumStorageAdapter;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import org.slf4j.LoggerFactory;

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
    @SuppressWarnings({"CanBeFinal", "FieldMayBeFinal"})
    private String uuid;

    public JavaxValidationStorageListener() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
        uuid = UUID.randomUUID().toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JavaxValidationStorageListener that = (JavaxValidationStorageListener) o;
        return Objects.equals(uuid, that.uuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid);
    }

    @Override
    public void preStore(Morphium m, Map<Object, Boolean> isNew) throws MorphiumAccessVetoException {
        for (Map.Entry<Object, Boolean> e : isNew.entrySet()) {
            preStore(m, e.getKey(), e.getValue());
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
        violations=new HashSet<>(violations);
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
                    LoggerFactory.getLogger(JavaxValidationStorageListener.class).error("Could not access Field: " + f);
                }

            } else if (Set.class.isAssignableFrom(field.getType())) {
                try {
                    Collection<Object> lst = (Set<Object>) field.get(r);
                    if (lst != null) {
                        Map<Object, Boolean> map = new HashMap<>();
                        for (Object o : lst) {
                            map.put(o, isNew);
                        }
                        validatePrestore(m, violations, map);

                    }

                } catch (IllegalAccessException e) {
                    LoggerFactory.getLogger(JavaxValidationStorageListener.class).error("Could not access list field: " + f);
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
                        validatePrestore(m, violations, map);

                    }

                } catch (IllegalAccessException e) {
                    LoggerFactory.getLogger(JavaxValidationStorageListener.class).error("Could not access list field: " + f);
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
                        validatePrestore(m, violations, lst);
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

    private void validatePrestore(Morphium m, Set<ConstraintViolation<Object>> violations, Map<Object, Boolean> map) {
        try {
            preStore(m, map);
        } catch (ConstraintViolationException e) {
            Set<ConstraintViolation<?>> constraintViolations = e.getConstraintViolations();
            //ADDall not possible due to generics mess
            //noinspection Convert2streamapi
            for (ConstraintViolation v : constraintViolations) {
                violations.add(v);
            }
        }
    }

}
