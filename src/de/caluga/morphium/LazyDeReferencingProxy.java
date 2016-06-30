package de.caluga.morphium;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import java.io.Serializable;
import java.lang.reflect.Method;

public class LazyDeReferencingProxy<T> implements MethodInterceptor, Serializable {
    private static final long serialVersionUID = 3777709000906217075L;
    private static final Logger log = new Logger(LazyDeReferencingProxy.class);
    private final transient Morphium morphium;
    private final String fieldname;
    private final Object container;
    private final String collectionName;
    private final Class<? extends T> cls;
    private final Object id;
    private T deReferenced;

    public LazyDeReferencingProxy(Morphium m, Class<? extends T> type, Object id, Object container, String fieldname, String collectionName) {
        cls = type;
        this.id = id;
        morphium = m;
        this.container = container;
        this.fieldname = fieldname;
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
    public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy) throws Throwable {
        //        log.error("MEthod trigger "+method.getName());
        if (method.getName().equals("getClass")) {
            return cls;
        }
        if (method.getName().equals("__getType")) {
            return cls;
        }
        if (method.getName().equals("finalize")) {
            return methodProxy.invokeSuper(o, objects);
        }
        if (method.getName().equals("__getPureDeref")) {
            return deReferenced;
        }

        dereference();
        if (method.getName().equals("__getDeref")) {
            return deReferenced;
        }
        if (deReferenced != null) {
            return method.invoke(deReferenced, objects);
        }
        return methodProxy.invokeSuper(o, objects);

    }

    private void dereference() {
        if (deReferenced == null) {
            if (log.isDebugEnabled()) {
                log.debug("DeReferencing due to first access");
            }
            try {
                morphium.fireWouldDereference(container, fieldname, id, cls, true);
                deReferenced = morphium.findById(cls, id, collectionName);
                morphium.fireDidDereference(container, fieldname, deReferenced, true);
            } catch (MorphiumAccessVetoException e) {
                log.info("did not dereference due to VetoException from listener", e);
            }
        }
    }

}
