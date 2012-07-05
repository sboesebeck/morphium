package de.caluga.morphium;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import java.io.Serializable;
import java.lang.reflect.Method;

public class LazyDeReferencingProxy<T> implements MethodInterceptor, Serializable {
    private static final long serialVersionUID = 3777709000906217075L;
    private transient final Morphium morphium;
    private T deReferenced;
    private Class<T> cls;
    private ObjectId id;

    private final static Logger log = Logger.getLogger(LazyDeReferencingProxy.class);

    public LazyDeReferencingProxy(Morphium m, Class<T> type, ObjectId id) {
        cls = type;
        this.id = id;
        morphium = m;
    }

    public T __getDeref() {
        try {
            dereference();
        } catch (Throwable throwable) {
        }
        return deReferenced;
    }

    @Override
    public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy) throws Throwable {
//            if (method.getName().equals("getClass")) {
//                return cls;
//            }
        if (method.getName().equals("__getType")) {
            return cls;
        }
        if (method.getName().equals("finalize")) {
            return methodProxy.invokeSuper(o, objects);
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
            if (log.isDebugEnabled())
                log.debug("DeReferencing due to first access");
            deReferenced = (T) morphium.findById(cls, id);
        }
    }

}
