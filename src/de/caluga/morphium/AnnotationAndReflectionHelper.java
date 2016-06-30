package de.caluga.morphium;

import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.AsyncWrites;
import de.caluga.morphium.annotations.caching.WriteBuffer;
import de.caluga.morphium.annotations.lifecycle.Lifecycle;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: Stephan Bösebeck
 * Date: 07.03.13
 * Time: 11:10
 * <p/>
 * This class will capsulate all calls to the reflection API. Espeically getting all the annotations from
 * entities is done here. For performance increase (and because the structure of the code usually does not
 * change during runtime) those results are being cached.
 * <p>
 * this class is ThreadSafe!
 */
@SuppressWarnings("unchecked")
public class AnnotationAndReflectionHelper {
    private final Annotation annotationNotPresent = () -> null;
    private final Logger log = new Logger(AnnotationAndReflectionHelper.class);
    private final Map<Class<?>, Class<?>> realClassCache = new ConcurrentHashMap<>();
    private final Map<Class<?>, List<Field>> fieldListCache = new ConcurrentHashMap<>();
    private final Map<Class<?>, Map<Class<? extends Annotation>, Annotation>> annotationCache;
    private final Map<Class<?>, Map<String, String>> fieldNameCache;
    private Map<String, Field> fieldCache = new ConcurrentHashMap<>();
    private Map<String, List<String>> fieldAnnotationListCache = new ConcurrentHashMap<>();
    private Map<Class<?>, Map<Class<? extends Annotation>, Method>> lifeCycleMethods;
    private Map<Class<?>, Boolean> hasAdditionalData;
    private boolean ccc = true;

    public AnnotationAndReflectionHelper(boolean convertCamelCase) {
        this.ccc = convertCamelCase;
        lifeCycleMethods = new ConcurrentHashMap<>();
        hasAdditionalData = new ConcurrentHashMap<>();
        annotationCache = new ConcurrentHashMap<>();
        fieldNameCache = new ConcurrentHashMap<>();
    }

    public <T extends Annotation> boolean isAnnotationPresentInHierarchy(Class<?> cls, Class<? extends T> anCls) {
        return getAnnotationFromHierarchy(cls, anCls) != null;
    }

    public <T> Class<? extends T> getRealClass(Class<? extends T> sc) {
        if (realClassCache.containsKey(sc)) {
            return (Class<? extends T>) realClassCache.get(sc);
        }
        if (sc.getName().contains("$$EnhancerByCGLIB$$")) {

            try {
                Class ret = Class.forName(sc.getName().substring(0, sc.getName().indexOf("$$")));
                realClassCache.put(sc, ret);
                sc = ret;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return sc;
    }

    public boolean isBufferedWrite(Class<?> cls) {
        WriteBuffer wb = getAnnotationFromHierarchy(cls, WriteBuffer.class);
        return wb != null && wb.value();
    }


    /**
     * returns annotations, even if in class hierarchy or
     * lazyloading proxy
     *
     * @param cls class
     * @return the Annotation
     */
    public <T extends Annotation> T getAnnotationFromHierarchy(Class<?> cls, Class<? extends T> anCls) {
        cls = getRealClass(cls);
        if (annotationCache.get(cls) != null && annotationCache.get(cls).get(anCls) != null) {
            if (annotationCache.get(cls).get(anCls).equals(annotationNotPresent)) {
                return null;
            }
            return (T) annotationCache.get(cls).get(anCls);
        }
        T ret;
        annotationCache.putIfAbsent(cls, new HashMap<>());

        ret = cls.getAnnotation(anCls);
        if (ret == null) {

            //class hierarchy?
            Class<?> z = cls;
            while (!z.equals(Object.class)) {
                if (z.isAnnotationPresent(anCls)) {
                    ret = z.getAnnotation(anCls); //found it on the "downmost" inheritence level
                    break;
                }
                z = z.getSuperclass();
                if (z == null) {
                    break;
                }
            }

            if (ret == null) {
                //check interfaces if nothing was found yet
                Queue<Class<?>> interfaces = new LinkedList<>();
                Collections.addAll(interfaces, cls.getInterfaces());
                while (!interfaces.isEmpty()) {
                    Class<?> iface = interfaces.poll();
                    if (iface.isAnnotationPresent(anCls)) {
                        ret = iface.getAnnotation(anCls);
                        break; //no need to look further, found annotation
                    }
                    interfaces.addAll(Arrays.asList(iface.getInterfaces()));
                }
            }

        }
        if (ret == null) {
            //annotation not present in hierarchy, store marker
            annotationCache.get(cls).put(anCls, annotationNotPresent);
        } else {
            //found it, keep it in cache
            annotationCache.get(cls).put(anCls, ret);
        }
        return ret;
    }

    public boolean hasAdditionalData(Class clz) {
        if (hasAdditionalData.get(clz) == null) {
            List<String> lst = getFields(clz, AdditionalData.class);
            Map m = hasAdditionalData; // (HashMap) ((HashMap) hasAdditionalData).clone();
            m.put(clz, (lst != null && !lst.isEmpty()));
            hasAdditionalData = m;
        }

        return hasAdditionalData.get(clz);
    }

    @SuppressWarnings("StatementWithEmptyBody")
    public String getFieldName(Class clz, String field) {
        Class cls = getRealClass(clz);
        if (field.contains(".") || field.contains("(") || field.contains("$")) {
            //searching for a sub-element?
            //no check possible
            return field;
        }
        if (fieldNameCache.containsKey(clz) && fieldNameCache.get(clz).get(field) != null) {
            return fieldNameCache.get(clz).get(field);
        }


        String ret = field;

        List<Class> inf = Arrays.asList(clz.getInterfaces());
        if ((inf.contains(List.class)) || inf.contains(Map.class) || inf.contains(Collection.class) || inf.contains(Set.class) || clz.isArray()) {
            //not diving into maps

        } else {

            Field f = getField(cls, field);
            if (f == null && hasAdditionalData(clz)) {
                return field;
            }
            if (f == null) {
                throw new RuntimeException("Field not found " + field + " in cls: " + clz.getName());
            }
            if (f.isAnnotationPresent(Property.class)) {
                Property p = f.getAnnotation(Property.class);
                if (!p.fieldName().equals(".")) {
                    return p.fieldName();
                }
            }

            if (f.isAnnotationPresent(Reference.class)) {
                Reference p = f.getAnnotation(Reference.class);
                if (!p.fieldName().equals(".")) {
                    return p.fieldName();
                }
            }
            if (f.isAnnotationPresent(Id.class)) {
                return "_id";
            }


            ret = f.getName();
            Entity ent = getAnnotationFromHierarchy(cls, Entity.class); //(Entity) cls.getAnnotation(Entity.class);
            Embedded emb = getAnnotationFromHierarchy(cls, Embedded.class);//(Embedded) cls.getAnnotation(Embedded.class);
            if ((ccc && ent != null && ent.translateCamelCase())
                    || (ccc && emb != null && emb.translateCamelCase())) {
                ret = convertCamelCase(ret);
            }
        }
        Map<Class<?>, Map<String, String>> m = fieldNameCache; // (HashMap) ((HashMap) fieldNameCache).clone();
        if (!m.containsKey(cls)) {
            m.put(cls, new HashMap<>());
        }
        m.get(cls).put(field, ret);
        return ret;

    }

    /**
     * converts a sql/javascript-Name to Java, e.g. converts document_id to
     * documentId.
     *
     * @param n          - string to convert
     * @param capitalize : if true, first letter will be capitalized
     * @return the translated name (capitalized or camel_case => camelCase)
     */
    public String createCamelCase(String n, boolean capitalize) {
        n = n.toLowerCase();
        String f[] = n.split("_");
        StringBuilder sb = new StringBuilder(f[0].substring(0, 1).toLowerCase());
        //String ret =
        sb.append(f[0].substring(1));
        for (int i = 1; i < f.length; i++) {
            sb.append(f[i].substring(0, 1).toUpperCase());
            sb.append(f[i].substring(1));
        }
        String ret = sb.toString();
        if (capitalize) {
            ret = ret.substring(0, 1).toUpperCase() + ret.substring(1);
        }
        return ret;
    }

    /**
     * turns documentId into document_id
     *
     * @param n - string to convert
     * @return converted string (camelCase becomes camel_case)
     */
    @SuppressWarnings("StringBufferMayBeStringBuilder")
    public String convertCamelCase(String n) {
        if (!ccc) {
            return n;
        }
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < n.length() - 1; i++) {
            if (Character.isUpperCase(n.charAt(i)) && i > 0) {
                b.append("_");
            }
            b.append(n.substring(i, i + 1).toLowerCase());
        }
        b.append(n.substring(n.length() - 1));
        return b.toString();
    }

    /**
     * return list of fields in class - including hierachy!!!
     *
     * @param clz class to get all fields for
     * @return list of fields in that class
     */
    public List<Field> getAllFields(Class clz) {
        if (fieldListCache.containsKey(clz)) {
            return fieldListCache.get(clz);
        }
        Class<?> cls = getRealClass(clz);

        List<Field> ret = new ArrayList<>();
        Class sc = cls;
        //getting class hierachy
        List<Class> hierachy = new ArrayList<>();
        while (!sc.equals(Object.class)) {
            hierachy.add(sc);
            sc = sc.getSuperclass();
        }
        Collections.addAll(hierachy, cls.getInterfaces());
        //now we have a list of all classed up to Object
        //we need to run through it in the right order
        //in order to allow Inheritance to "shadow" fields
        for (Class c : hierachy) {
            //            Class c = hierachy.get(i);
            Collections.addAll(ret, c.getDeclaredFields());
        }
        fieldListCache.put(clz, ret);
        return ret;
    }


    /**
     * extended logic: Fld may be, the java field name, the name of the specified value in Property-Annotation or
     * the translated underscored lowercase name (mongoId => mongo_id) or a name specified in the Aliases-Annotation of this field
     *
     * @param clz - class to search
     * @param fld - field name
     * @return field, if found, null else
     */
    public Field getField(Class clz, String fld) {
        String key = clz.toString() + "->" + fld;
        Field val = fieldCache.get(key);
        if (val != null) {
            return val;
        }
        Map<String, Field> fc = fieldCache; //(HashMap) ((HashMap) fieldCache).clone();
        Class cls = getRealClass(clz);
        List<Field> flds = getAllFields(cls);
        Field ret = null;
        for (Field f : flds) {
            if (f.isAnnotationPresent(Property.class) && !".".equals(f.getAnnotation(Property.class).fieldName()) && f.getAnnotation(Property.class).fieldName().equals(fld)) {
                f.setAccessible(true);

                fc.put(key, f);
                ret = f;

            }
            if (ret == null && f.isAnnotationPresent(Reference.class) && !".".equals(f.getAnnotation(Reference.class).fieldName()) && f.getAnnotation(Reference.class).fieldName().equals(fld)) {
                f.setAccessible(true);

                fc.put(key, f);
                ret = f;

            }

            if (ret == null && f.isAnnotationPresent(Aliases.class)) {
                Aliases aliases = f.getAnnotation(Aliases.class);
                String[] v = aliases.value();
                for (String field : v) {
                    if (field.equals(fld)) {
                        f.setAccessible(true);
                        fc.put(key, f);
                        ret = f;
                    }
                }
            }
            if (ret == null && fld.equals("_id") && f.isAnnotationPresent(Id.class)) {
                f.setAccessible(true);
                fc.put(key, f);
                ret = f;
            }
            if (ret == null && f.getName().equals(fld)) {
                f.setAccessible(true);
                fc.put(key, f);
                ret = f;
            }
            if (ret == null && ccc && convertCamelCase(f.getName()).equals(fld)) {
                f.setAccessible(true);
                fc.put(key, f);
                ret = f;
            }

            if (ret != null) {
                break;
            }

        }
        fieldCache = fc;

        //unknown field
        return ret;
    }


    public boolean isEntity(Object o) {
        Class cls;
        if (o == null) {
            return false;
        }

        if (o instanceof Class) {
            cls = getRealClass((Class) o);
        } else {
            cls = getRealClass(o.getClass());
        }
        return isAnnotationPresentInHierarchy(cls, Entity.class) || isAnnotationPresentInHierarchy(cls, Embedded.class);
    }

    public Object getValue(Object o, String fld) {
        if (o == null) {
            return null;
        }
        try {
            Field f = getField(o.getClass(), fld);
            if (!Modifier.isStatic(f.getModifiers())) {
                o = getRealObject(o);
                return f.get(o);
            }
        } catch (IllegalAccessException e) {
            log.fatal("Illegal access to field " + fld + " of type " + o.getClass().getSimpleName());

        }
        return null;
    }

    public void setValue(Object o, Object value, String fld) {
        if (o == null) {
            return;
        }
        try {
            Field f = getField(getRealClass(o.getClass()), fld);
            if (!Modifier.isStatic(f.getModifiers())) {
                o = getRealObject(o);
                try {
                    f.set(o, value);
                } catch (Exception e) {

                    if (value != null) {
                        if (log.isDebugEnabled()) {
                            log.debug("Setting of value (" + value.getClass().getSimpleName() + ") failed for field " + f.getName() + "- trying type-conversion");
                        }
                        //Doing some type conversions... lots of :-(
                        if (value instanceof Double) {
                            //maybe some kind of Default???
                            Double d = (Double) value;
                            if (f.getType().equals(Integer.class) || f.getType().equals(int.class)) {
                                f.set(o, d.intValue());
                            } else if (f.getType().equals(Long.class) || f.getType().equals(long.class)) {
                                f.set(o, d.longValue());
                            } else if (f.getType().equals(Date.class)) {
                                //Fucking date / timestamp mixup
                                f.set(o, new Date(d.longValue()));
                            } else if (f.getType().equals(Float.class) || f.getType().equals(float.class)) {
                                f.set(o, d.floatValue());
                            } else if (f.getType().equals(Boolean.class) || f.getType().equals(boolean.class)) {
                                f.set(o, d == 1.0);
                            } else if (f.getType().equals(String.class)) {
                                f.set(o, d.toString());
                            } else {
                                throw new RuntimeException("could not set field " + fld + ": Field has type " + f.getType().toString() + " got type " + value.getClass().toString());
                            }
                        } else if (value instanceof Float) {
                            //maybe some kind of Default???
                            Float d = (Float) value;
                            if (f.getType().equals(Integer.class) || f.getType().equals(int.class)) {
                                f.set(o, d.intValue());
                            } else if (f.getType().equals(Long.class) || f.getType().equals(long.class)) {
                                f.set(o, d.longValue());
                            } else if (f.getType().equals(Date.class)) {
                                //Fucking date / timestamp mixup
                                f.set(o, new Date(d.longValue()));
                            } else if (f.getType().equals(Float.class) || f.getType().equals(float.class)) {
                                f.set(o, d);
                            } else if (f.getType().equals(Boolean.class) || f.getType().equals(boolean.class)) {
                                f.set(o, d == 1.0f);
                            } else if (f.getType().equals(String.class)) {
                                f.set(o, d.toString());
                            } else {
                                throw new RuntimeException("could not set field " + fld + ": Field has type " + f.getType().toString() + " got type " + value.getClass().toString());
                            }
                        } else if (value instanceof Date) {
                            //Date/String mess-up?
                            Date d = (Date) value;
                            if (f.getType().equals(Long.class) || f.getType().equals(long.class)) {
                                f.set(o, d.getTime());
                            } else if (f.getType().equals(GregorianCalendar.class)) {
                                GregorianCalendar cal = new GregorianCalendar();
                                cal.setTimeInMillis(d.getTime());
                                f.set(o, cal);
                            } else if (f.getType().equals(String.class)) {
                                SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
                                f.set(o, df.format(d));
                            }
                        } else if (value instanceof String) {
                            //String->Number conversion necessary????
                            try {
                                String s = (String) value;
                                if (f.getType().equals(Long.class) || f.getType().equals(long.class)) {
                                    f.set(o, Long.parseLong(s));
                                } else if (f.getType().equals(Integer.class) || f.getType().equals(int.class)) {
                                    f.set(o, Integer.parseInt(s));
                                } else if (f.getType().equals(Double.class) || f.getType().equals(double.class)) {
                                    f.set(o, Double.parseDouble(s));
                                } else if (f.getType().equals(Date.class)) {
                                    //Fucking date / timestamp mixup
                                    if (s.length() == 8) {
                                        //probably time-string 20120812
                                        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
                                        f.set(o, df.parse(s));
                                    } else if (s.indexOf("-") > 0) {
                                        //maybe a date-String?
                                        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                                        f.set(o, df.parse(s));
                                    } else if (s.indexOf(".") > 0) {
                                        //maybe a date-String?
                                        SimpleDateFormat df = new SimpleDateFormat("dd.MM.yyyy");
                                        f.set(o, df.parse(s));
                                    } else {
                                        f.set(o, new Date(Long.parseLong(s)));
                                    }
                                } else if (f.getType().equals(Boolean.class) || f.getType().equals(boolean.class)) {
                                    f.set(o, s.equalsIgnoreCase("true"));
                                } else if (f.getType().equals(Float.class) || f.getType().equals(float.class)) {
                                    f.set(o, Float.parseFloat(s));
                                } else {
                                    throw new RuntimeException("could not set field " + fld + ": Field has type " + f.getType().toString() + " got type " + value.getClass().toString());
                                }
                            } catch (ParseException e1) {
                                throw new RuntimeException(e1);
                            }
                        } else if (value instanceof Integer) {
                            Integer i = (Integer) value;
                            if (f.getType().equals(Long.class) || f.getType().equals(long.class)) {
                                f.set(o, i.longValue());
                            } else if (f.getType().equals(Double.class) || f.getType().equals(double.class)) {
                                f.set(o, i.doubleValue());
                            } else if (f.getType().equals(Date.class)) {
                                //Fucking date / timestamp mixup
                                f.set(o, new Date(i.longValue()));
                            } else if (f.getType().equals(String.class)) {
                                f.set(o, i.toString());
                            } else if (f.getType().equals(Float.class) || f.getType().equals(float.class)) {
                                f.set(o, i.floatValue());
                            } else if (f.getType().equals(Boolean.class) || f.getType().equals(boolean.class)) {
                                f.set(o, i == 1);
                            } else {
                                throw new RuntimeException("could not set field " + fld + ": Field has type " + f.getType().toString() + " got type " + value.getClass().toString());
                            }
                        } else if (value instanceof Long) {
                            Long l = (Long) value;
                            if (f.getType().equals(Integer.class) || f.getType().equals(int.class)) {
                                f.set(o, l.intValue());
                            } else if (f.getType().equals(Double.class) || f.getType().equals(double.class)) {
                                f.set(o, l.doubleValue());
                            } else if (f.getType().equals(Date.class)) {
                                //Fucking date / timestamp mixup
                                f.set(o, new Date(l));
                            } else if (f.getType().equals(Float.class) || f.getType().equals(float.class)) {
                                f.set(o, l.floatValue());
                            } else if (f.getType().equals(Boolean.class) || f.getType().equals(boolean.class)) {
                                f.set(o, l == 1L);
                            } else if (f.getType().equals(String.class)) {
                                f.set(o, l.toString());
                            } else {
                                throw new RuntimeException("could not set field " + fld + ": Field has type " + f.getType().toString() + " got type " + value.getClass().toString());
                            }
                        } else if (value instanceof Boolean) {
                            Boolean b = (Boolean) value;
                            if (f.getType().equals(Integer.class) || f.getType().equals(int.class)) {
                                f.set(o, b ? 1 : 0);
                            } else if (f.getType().equals(Double.class) || f.getType().equals(double.class)) {
                                f.set(o, b ? 1.0 : 0.0);
                            } else if (f.getType().equals(Float.class) || f.getType().equals(float.class)) {
                                f.set(o, b ? 1.0f : 0.0f);
                            } else if (f.getType().equals(String.class)) {
                                f.set(o, b ? "true" : "false");
                            } else {
                                throw new RuntimeException("could not set field " + fld + ": Field has type " + f.getType().toString() + " got type " + value.getClass().toString());
                            }
                        } else if (f.getType().isArray() && value instanceof List) {
                            Object arr = Array.newInstance(f.getType(), ((List) value).size());
                            int idx = 0;
                            for (Object io : ((List) value)) {
                                try {
                                    Array.set(arr, idx, io);
                                } catch (Exception e1) {
                                    Array.set(arr, idx, ((Integer) io).byteValue());
                                }
                            }
                            f.set(o, arr);
                        } else {
                            log.error("Could not set value!!!");
                        }
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("Type conversion was successful");
                    }
                }
            }
        } catch (IllegalAccessException e) {
            log.fatal("Illegal access to field " + fld + " of toype " + o.getClass().getSimpleName());
        }
    }


    public Object getId(Object o) {
        if (o == null) {
            throw new IllegalArgumentException("Object cannot be null");
        }

        Field f = getIdField(o);
        if (f == null) {
            throw new IllegalArgumentException("Object ID field not found " + o.getClass().getSimpleName());
        }
        try {
            o = getRealObject(o);
            if (o != null) {
                return f.get(o);
            } else {
                log.warn("Illegal reference?");
            }

            return null;
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public String getIdFieldName(Object o) {
        Class<?> cls = getRealClass(o.getClass());
        List<String> flds = getFields(cls, Id.class);
        if (flds == null || flds.isEmpty()) {
            throw new IllegalArgumentException("Object has no id defined: " + o.getClass().getSimpleName());
        }
        return flds.get(0);
    }

    public Field getIdField(Object o) {
        Class<?> cls;
        if (o instanceof Class) {
            cls = getRealClass((Class<?>) o);
        } else {
            cls = getRealClass(o.getClass());
        }

        List<String> flds = getFields(cls, Id.class);
        if (flds == null || flds.isEmpty()) {
            throw new IllegalArgumentException("Object has no id defined: " + o.getClass().getSimpleName());
        }
        return getField(cls, flds.get(0));
    }

    /**
     * get a list of valid fields of a given record as they are in the MongoDB
     * so, if you have a field Mapping, the mapped Property-name will be used
     * returns all fields, which have at least one of the given annotations
     * if no annotation is given, all fields are returned
     * Does not take the @Aliases-annotation int account
     *
     * @param cls - the class to geht ghe Fields from
     * @return List of Strings, each a field name (as described in @Property or determined by name)
     */
    public List<String> getFields(Class cls, Class<? extends Annotation>... annotations) {
        StringBuilder stringBuilder = new StringBuilder(cls.toString());
        for (Class<? extends Annotation> a : annotations) {
            stringBuilder.append("/");
            stringBuilder.append(a.toString());
        }
        List<String> strings = fieldAnnotationListCache.get(stringBuilder.toString());
        if (strings != null) {
            return strings;
        }
        Map<String, List<String>> fa = fieldAnnotationListCache; // (HashMap) ((HashMap) fieldAnnotationListCache).clone();
        List<String> ret = new ArrayList<>();
        Class sc = cls;
        sc = getRealClass(sc);
        Entity entity = getAnnotationFromHierarchy(sc, Entity.class); //(Entity) sc.getAnnotation(Entity.class);
        Embedded embedded = getAnnotationFromHierarchy(sc, Embedded.class);//(Embedded) sc.getAnnotation(Embedded.class);
        if (embedded != null && entity != null) {
            log.warn("Class " + cls.getName() + " does have both @Entity and @Embedded Annotations - not allowed! Assuming @Entity is right");
        }

        if (embedded == null && entity == null) {
            throw new IllegalArgumentException("This class " + cls.getName() + " does not have @Entity or @Embedded set, not even in hierachy - illegal!");
        }
        boolean tcc = entity == null ? embedded.translateCamelCase() : entity.translateCamelCase();
        //getting class hierachy
        List<Field> fld = getAllFields(cls);
        for (Field f : fld) {
            if (annotations.length > 0) {
                boolean found = false;
                for (Class<? extends Annotation> a : annotations) {
                    if (f.isAnnotationPresent(a)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    //no annotation found
                    continue;
                }
            }

            if (f.isAnnotationPresent(Reference.class) && !".".equals(f.getAnnotation(Reference.class).fieldName())) {
                ret.add(f.getAnnotation(Reference.class).fieldName());
                continue;
            }
            if (f.isAnnotationPresent(Property.class) && !".".equals(f.getAnnotation(Property.class).fieldName())) {
                ret.add(f.getAnnotation(Property.class).fieldName());
                continue;
            }
            //            if (f.isAnnotationPresent(Id.class)) {
            //                ret.add(f.getName());
            //                continue;
            //            }
            if (f.isAnnotationPresent(Transient.class)) {
                continue;
            }

            if (tcc && ccc) {
                ret.add(convertCamelCase(f.getName()));
            } else {
                ret.add(f.getName());
            }
        }

        fa.put(stringBuilder.toString(), ret);
        fieldAnnotationListCache = fa;
        return ret;
    }


    public <T> T getRealObject(T o) {
        if (o.getClass().getName().contains("$$EnhancerByCGLIB$$")) {
            //not stored or Proxy?
            try {
                Field f1 = o.getClass().getDeclaredField("CGLIB$CALLBACK_0");
                f1.setAccessible(true);
                Object delegate = f1.get(o);
                Method m = delegate.getClass().getMethod("__getDeref");
                o = (T) m.invoke(delegate);
            } catch (Exception e) {
                //throw new RuntimeException(e);
                log.error("Exception: ", e);
            }
        }
        return o;
    }

    public final Class getTypeOfField(Class<?> cls, String fld) {
        Field f = getField(cls, fld);
        if (f == null) {
            return null;
        }
        return f.getType();
    }

    public boolean storesLastChange(Class<?> cls) {
        return isAnnotationPresentInHierarchy(cls, LastChange.class);
    }


    public boolean storesLastAccess(Class<?> cls) {
        return isAnnotationPresentInHierarchy(cls, LastAccess.class);
    }

    public boolean storesCreation(Class<?> cls) {
        return isAnnotationPresentInHierarchy(cls, CreationTime.class);
    }


    @SuppressWarnings("unused")
    public Long getLongValue(Object o, String fld) {
        return (Long) getValue(o, fld);
    }

    @SuppressWarnings("unused")
    public String getStringValue(Object o, String fld) {
        return (String) getValue(o, fld);
    }

    @SuppressWarnings("unused")
    public Date getDateValue(Object o, String fld) {
        return (Date) getValue(o, fld);
    }

    @SuppressWarnings("unused")
    public Double getDoubleValue(Object o, String fld) {
        return (Double) getValue(o, fld);
    }

    public List<Annotation> getAllAnnotationsFromHierachy(Class<?> cls, Class<? extends Annotation>... anCls) {
        cls = getRealClass(cls);
        List<Annotation> ret = new ArrayList<>();
        Class<?> z = cls;
        while (!z.equals(Object.class)) {
            if (z.getAnnotations() != null && z.getAnnotations().length != 0) {
                if (anCls.length == 0) {
                    ret.addAll(Arrays.asList(z.getAnnotations()));
                } else {
                    for (Annotation a : z.getAnnotations()) {
                        for (Class<? extends Annotation> ac : anCls) {
                            if (a.annotationType().equals(ac)) {
                                ret.add(a);
                            }
                        }
                    }
                }
            }
            z = z.getSuperclass();

            if (z == null) {
                break;
            }
        }

        return ret;
    }


    @SuppressWarnings({"unchecked", "unused"})
    public String getLastChangeField(Class<?> cls) {
        if (!storesLastChange(cls)) {
            return null;
        }
        List<String> lst = getFields(cls, LastChange.class);
        if (lst == null || lst.isEmpty()) {
            return null;
        }
        return lst.get(0);
    }


    @SuppressWarnings({"unchecked", "unused"})
    public String getLastAccessField(Class<?> cls) {
        if (!storesLastAccess(cls)) {
            return null;
        }
        List<String> lst = getFields(cls, LastAccess.class);
        if (lst == null || lst.isEmpty()) {
            return null;
        }
        return lst.get(0);
    }


    @SuppressWarnings({"unchecked", "unused"})
    public String getCreationTimeField(Class<?> cls) {
        if (!storesCreation(cls)) {
            return null;
        }
        List<String> lst = getFields(cls, CreationTime.class);
        if (lst == null || lst.isEmpty()) {
            return null;
        }
        return lst.get(0);
    }


    public void callLifecycleMethod(Class<? extends Annotation> type, Object on) {
        callLifecycleMethod(type, on, new ArrayList());
    }

    private void callLifecycleMethod(Class<? extends Annotation> type, Object on, List calledOn) {
        if (on == null) {
            return;
        }
        if (on.getClass().getName().contains("$$EnhancerByCGLIB$$")) {
            try {
                Field f1 = on.getClass().getDeclaredField("CGLIB$CALLBACK_0");
                f1.setAccessible(true);
                Object delegate = f1.get(on);
                Method m = delegate.getClass().getMethod("__getPureDeref");
                on = m.invoke(delegate);
                if (on == null) {
                    return;
                }
            } catch (Exception e) {
                //throw new RuntimeException(e);
                log.error("Exception: ", e);
            }
        }

        if (calledOn.contains(on)) {
            return;
        }
        calledOn.add(on);
        //No synchronized block - might cause the methods to be put twice into the
        //hashtabel - but for performance reasons, it's ok...
        Class<?> cls = on.getClass();
        //No Lifecycle annotation - no method calling
        if (!isAnnotationPresentInHierarchy(cls, Lifecycle.class)) {//cls.isAnnotationPresent(Lifecycle.class)) {
            return;
        }
        List<String> flds = getFields(on.getClass());
        for (String f : flds) {
            Field field = getField(on.getClass(), f);
            if ((isAnnotationPresentInHierarchy(field.getType(), Entity.class) || isAnnotationPresentInHierarchy(field.getType(), Embedded.class)) && isAnnotationPresentInHierarchy(field.getType(), Lifecycle.class)) {
                field.setAccessible(true);
                try {
                    callLifecycleMethod(type, field.get(on), calledOn);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }
        //Already stored - should not change during runtime
        if (lifeCycleMethods.get(cls) != null) {
            if (lifeCycleMethods.get(cls).get(type) != null) {
                try {
                    lifeCycleMethods.get(cls).get(type).invoke(on);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                } catch (InvocationTargetException e) {
                    if (e.getCause().getClass().equals(MorphiumAccessVetoException.class)) {
                        throw (RuntimeException) e.getCause();
                    }
                    throw new RuntimeException(e);
                }
            }
            return;
        }

        Map<Class<? extends Annotation>, Method> methods = new HashMap<>();
        //Methods must be public
        for (Method m : cls.getMethods()) {
            for (Annotation a : m.getAnnotations()) {
                methods.put(a.annotationType(), m);
            }
        }
        Map<Class<?>, Map<Class<? extends Annotation>, Method>> lc = lifeCycleMethods;  //(HashMap) ((HashMap) lifeCycleMethods).clone();
        lc.put(cls, methods);
        if (methods.get(type) != null) {
            try {
                methods.get(type).invoke(on);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }
        lifeCycleMethods = lc;
    }

    public boolean isAsyncWrite(Class<?> cls) {
        AsyncWrites wb = getAnnotationFromHierarchy(cls, AsyncWrites.class);
        return wb != null && wb.value();
    }
}
