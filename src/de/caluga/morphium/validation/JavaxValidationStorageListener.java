package de.caluga.morphium.validation;

import de.caluga.morphium.MorphiumStorageListener;
import de.caluga.morphium.Query;

import javax.validation.*;
import java.util.HashSet;
import java.util.Set;

/**
 * User: martinstolz
 * Date: 29.08.12
 */
public class JavaxValidationStorageListener implements MorphiumStorageListener<Object> {

    private Validator validator;

    public JavaxValidationStorageListener() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    @Override
    public void preStore(Object r, boolean isNew) {
        Set<ConstraintViolation<Object>> violations = validator.validate(r);
        if (violations.size() > 0) {
            throw new ConstraintViolationException(new HashSet<ConstraintViolation<?>>(violations));
        }
    }

    // do nothing methods down below

    @Override
    public void postStore(Object r, boolean isNew) {
    }

    @Override
    public void postRemove(Object r) {
    }

    @Override
    public void preDelete(Object r) {
    }

    @Override
    public void postDrop(Class<Object> cls) {
    }

    @Override
    public void preDrop(Class<Object> cls) {
    }

    @Override
    public void preRemove(Query<Object> q) {
    }

    @Override
    public void postRemove(Query<Object> q) {
    }

    @Override
    public void postLoad(Object o) {
    }

    @Override
    public void preUpdate(Class<Object> cls, Enum updateType) {
    }

    @Override
    public void postUpdate(Class<Object> cls, Enum updateType) {
    }
}
