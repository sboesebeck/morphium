package de.caluga.morphium;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

public class LazyDeReferencingProxy<T> implements InvocationHandler, Serializable {
    private static final long serialVersionUID = 3777709000906217075L;
    private static final Logger log = LoggerFactory.getLogger(LazyDeReferencingProxy.class);
    private final transient Morphium morphium;
    private final String collectionName;
    private final Class<? extends T> cls;
    private final Object id;
    private T deReferenced;

    public LazyDeReferencingProxy(Morphium m, Class<? extends T> type, Object id, String collectionName) {
        cls = type;
        this.id = id;
        morphium = m;
        this.collectionName = collectionName;
    }

    @SuppressWarnings("unused")
    public T __getPureDeref() {
        return deReferenced;
    }

    public T __getDeref() {
        try {
            dereference();
        } catch (Throwable throwable) {
            throw (new RuntimeException(throwable));
        }
        return deReferenced;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getName().equals("getClass")) {
            return cls;
        }
        if (method.getName().equals("__getType")) {
            return cls;
        }
        if (method.getName().equals("finalize")) {
            return null;
        }
        if (method.getName().equals("__getPureDeref")) {
            return deReferenced;
        }

        dereference();
        if (method.getName().equals("__getDeref")) {
            return deReferenced;
        }
        if (deReferenced != null) {
            method.setAccessible(true);
            return method.invoke(deReferenced, args);
        }
        return getDefaultValue(method.getReturnType());
    }

    private Object getDefaultValue(Class<?> type) {
        if (!type.isPrimitive()) return null;
        if (type == boolean.class) return false;
        if (type == int.class) return 0;
        if (type == long.class) return 0L;
        if (type == double.class) return 0.0d;
        if (type == float.class) return 0.0f;
        if (type == short.class) return (short) 0;
        if (type == byte.class) return (byte) 0;
        if (type == char.class) return '\0';
        return null;
    }

    private void dereference() {
        if (deReferenced == null) {
            if (log.isDebugEnabled()) {
                log.debug("DeReferencing due to first access");
            }
            try {
                deReferenced = morphium.findById(cls, id, collectionName);
            } catch (MorphiumAccessVetoException e) {
                log.info("did not dereference due to VetoException from listener", e);
            }
        }
    }

}
