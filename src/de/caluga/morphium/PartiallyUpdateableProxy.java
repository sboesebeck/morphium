package de.caluga.morphium;

import de.caluga.morphium.annotations.PartialUpdate;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PartiallyUpdateableProxy<T> implements MethodInterceptor, PartiallyUpdateable, Serializable {
    private static final long serialVersionUID = -1277417045334974980L;
    //    private final transient Morphium morphium;
    private final AnnotationAndReflectionHelper ah;

    private final List<String> updateableFields;
    private final T reference;

    public PartiallyUpdateableProxy(Morphium m, T o) {
        updateableFields = Collections.synchronizedList(new ArrayList<>());
        reference = o;
        //        morphium = m;
        ah = m.getARHelper();
    }

    public T __getDeref() {
        //do nothing - will be intercepted
        return reference;
    }

    @SuppressWarnings("unused")
    public T __getPureDeref() {
        //do nothing - will be intercepted
        return reference;
    }

    @Override
    public List<String> getAlteredFields() {
        return updateableFields;
    }

    @Override
    public void clearAlteredFields() {
        updateableFields.clear();
    }

    @Override
    public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy) throws Throwable {
        if (method.getName().startsWith("set") || method.isAnnotationPresent(PartialUpdate.class)) {
            PartialUpdate up = method.getAnnotation(PartialUpdate.class);
            if (up != null) {
                //noinspection unchecked
                if (!ah.getFields(o.getClass()).contains(up.value())) {
                    throw new IllegalArgumentException("Field " + up.value() + " is not known to Type " + o.getClass().getName());
                }
                updateableFields.add(up.value());
            } else {
                String n = method.getName().substring(3);
                n = n.substring(0, 1).toLowerCase() + n.substring(1);
                updateableFields.add(n);
            }
        }
        if (method.getName().equals("getAlteredFields")) {
            return getAlteredFields();
        }
        if (method.getName().equals("clearAlteredFields")) {
            clearAlteredFields();
            return null;
        }
        if (method.getName().equals("__getType")) {
            return reference.getClass();
        }
        if (method.getName().equals("__getDeref")) {
            return reference;
        }
        return method.invoke(reference, objects);
        //            return methodProxy.invokeSuper(reference, objects);
    }
}
