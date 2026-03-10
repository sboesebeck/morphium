package de.caluga.morphium;

import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.AsyncWrites;
import de.caluga.morphium.annotations.caching.WriteBuffer;
import de.caluga.morphium.annotations.lifecycle.Lifecycle;
import de.caluga.morphium.driver.MorphiumId;
import io.github.classgraph.*;
import org.slf4j.Logger;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * User: Stephan Bösebeck
 * Date: 07.03.13
 * Time: 11:10
 * <p>
 * This class will encapsulate all calls to the reflection API. Specially getting all the annotations from
 * entities is done here. For performance increase (and because the structure of the code usually does not
 * change during runtime) those results are being cached.
 * <p>
 * this class is ThreadSafe!
 */
@SuppressWarnings({"unchecked", "CatchMayIgnoreException", "DuplicateExpressions"})
public class AnnotationAndReflectionHelper {

    private final Logger logger = getLogger(AnnotationAndReflectionHelper.class);
    private final Map < Class<?>, Class<? >> realClassCache;
    private final Map < Class<?>, List<Field >> fieldListCache;
    private final ConcurrentHashMap < Class<?>, Map<Class<? extends Annotation >, Annotation >> annotationCache;
    private final Map < Class<?>, Map<String, String >> fieldNameCache;
    private static ConcurrentHashMap<String, String> classNameByType;
    private Map<String, Field> fieldCache;
    private Map<String, List<String>> fieldAnnotationListCache;
    private Map<Class<?>, Map < Class<? extends Annotation >, Method >> lifeCycleMethods;
    private Map < Class<?>, Boolean > hasAdditionalData;
    private boolean ccc;

    public AnnotationAndReflectionHelper(boolean convertCamelCase) {
        this(convertCamelCase, new HashMap<>());
    }

    public AnnotationAndReflectionHelper(boolean convertCamelCase, Map < Class<?>, Class<? >> realClassCache) {
        this.ccc = convertCamelCase;
        this.realClassCache = realClassCache;
        this.fieldListCache = new ConcurrentHashMap<>();
        this.fieldCache = new ConcurrentHashMap<>();
        this.fieldAnnotationListCache = new ConcurrentHashMap<>();
        this.lifeCycleMethods = new ConcurrentHashMap<>();
        this.hasAdditionalData = new ConcurrentHashMap<>();
        this.annotationCache = new ConcurrentHashMap<>();
        this.fieldNameCache = new ConcurrentHashMap<>();

        if (classNameByType == null) {
            classNameByType = new ConcurrentHashMap<>();
            init();
        }
    }

    public void disableConvertCamelCase(boolean flag) {
        ccc = flag;
    }
    public boolean isConvertCamelcase() {
        return ccc;
    }
    public void enableConvertCamelCase() {
        ccc = true;
    }
    public void disableConvertCamelCase() {
        ccc = false;
    }
    private void init() {
        //initializing type IDs
        try (ScanResult scanResult =
                                    new ClassGraph()
            //                     .verbose()             // Enable verbose logging
            .enableAnnotationInfo()
            //                             .enableAllInfo()       // Scan classes, methods, fields, annotations
            .scan()) {
            ClassInfoList entities =
                            scanResult.getClassesWithAnnotation(Entity.class.getName());
            entities.addAll(scanResult.getClassesWithAnnotation(Embedded.class.getName()));
            logger.info("Found " + entities.size() + " entities in classpath");

            for (String cn : entities.getNames()) {
                ClassInfo ci = scanResult.getClassInfo(cn);
                AnnotationInfoList an = ci.getAnnotationInfo();

                for (AnnotationInfo ai : an) {
                    String name = ai.getName();

                    if (name.equals(Entity.class.getName()) || name.equals(Embedded.class.getName())) {
                        for (AnnotationParameterValue param : ai.getParameterValues()) {
                            //logger.info("Class " + cn + "   Param " + param.getName() + " = " + param.getValue());
                            if (param.getName().equals("typeId")) {
                                classNameByType.put(param.getValue().toString(), cn);
                            }

                            classNameByType.put(cn, cn);
                        }
                    }
                }
            }
        }
    }

    /**
     * Loads a class by name using the thread context classloader first, falling back to Class.forName().
     * This is necessary for compatibility with frameworks like Quarkus that use isolated classloaders.
     */
    public static Class<?> classForName(String className) throws ClassNotFoundException {
        ClassLoader tcl = Thread.currentThread().getContextClassLoader();
        if (tcl != null) {
            try {
                return Class.forName(className, true, tcl);
            } catch (ClassNotFoundException ignored) {
                // fall through to default
            }
        }
        return Class.forName(className);
    }

    public String getTypeIdForClassName(String n) throws ClassNotFoundException {
        return getTypeIdForClass(classForName(n));
    }

    public String getTypeIdForClass(Class cls) {
        cls = getRealClass(cls);
        Entity e = (Entity) cls.getAnnotation(Entity.class);
        Embedded em = (Embedded) cls.getAnnotation(Embedded.class);

        if (e != null && !e.typeId().equals(".")) {
            return e.typeId();
        } else if (em != null && !em.typeId().equals(".")) {
            return em.typeId();
        }

        return cls.getName();
    }

    public Class getClassForTypeId(String typeId) throws ClassNotFoundException {
        if (classNameByType.containsKey(typeId)) {
            return classForName(classNameByType.get(typeId));
        }

        return classForName(typeId);
    }

    public <T extends Annotation> boolean isAnnotationPresentInHierarchy(final Class<?> aClass, final Class <? extends T > annotationClass) {
        return getAnnotationFromHierarchy(aClass, annotationClass) != null;
    }

    public boolean isAnnotationOnAnyField(final Class<?> aClass, final Class <? extends Annotation > annotationClass) {
        if (aClass == null || Map.class.isAssignableFrom(aClass)) return false;

        for (Field f : getAllFields(aClass)) {
            if (f.getAnnotation(annotationClass) != null) { return true; }
        }

        return false;
    }

    /**
     * returns annotations, even if in class hierarchy or
     * lazyloading proxy
     *
     * @param superClass class
     * @return the Annotation
     */
    public <T extends Annotation> T getAnnotationFromHierarchy(final Class<?> superClass, final Class <? extends T > annotationClass) {
        if (superClass == null) { return null; }

        final Class<?> aClass = getRealClass(superClass);
        Map < Class <? extends Annotation>, Annotation > cacheForClass = annotationCache.get(aClass);

        if (cacheForClass != null) {
            // Must be done with containsKey to enable caching of null
            if (cacheForClass.containsKey(annotationClass)) {
                return (T) cacheForClass.get(annotationClass);
            }
        }

        T annotation = aClass.getAnnotation(annotationClass);

        if (annotation == null) {
            annotation = annotationOfClassHierarchy(aClass, annotationClass);
        }

        annotationCache.computeIfAbsent(aClass, k->new HashMap<>()).put(annotationClass, annotation);
        return annotation;
    }

    private <T extends Annotation> T annotationOfClassHierarchy(Class<?> aClass, Class <? extends T > annotationClass) {
        T annotation = null;
        Class<?> tmpClass = aClass;

        while (!tmpClass.equals(Object.class)) {
            if ((annotation = tmpClass.getAnnotation(annotationClass)) != null) {
                return annotation; //found it on the "downmost" inheritence level
            }

            tmpClass = tmpClass.getSuperclass();

            if (tmpClass == null) {
                break;
            }
        }

        //check interfaces if nothing was found yet
        ArrayDeque < Class<?>> interfaces = new ArrayDeque<>();
        Collections.addAll(interfaces, aClass.getInterfaces());

        while (!interfaces.isEmpty()) {
            Class<?> anInterface = interfaces.pollFirst();

            if (anInterface != null) {
                if ((annotation = anInterface.getAnnotation(annotationClass)) != null) {
                    return annotation; //no need to look further, found annotation
                }

                Collections.addAll(interfaces, anInterface.getInterfaces());
            }
        }
        return null;
    }

    public <T extends Annotation> T getAnnotationFromClass(final Class<?> cls, final Class <? extends T > annotationClass) {
        final Class<?> aClass = getRealClass(cls);
        return aClass.getAnnotation(annotationClass);
    }

    public <T> Class <? extends T > getRealClass(final Class <? extends T > superClass) {
        Class realClass = realClassCache.get(superClass);

        if (realClass != null) {
            return (Class <? extends T > ) realClass;
        }

        if (isProxy(superClass)) {
            realClass = realClassOf(superClass);
        } else {
            realClass = superClass;
        }

        realClassCache.put(superClass, realClass);
        return realClass;
    }

    public boolean isBufferedWrite(Class<?> aClass) {
        WriteBuffer wb = getAnnotationFromHierarchy(aClass, WriteBuffer.class);
        return wb != null && wb.value();
    }

    public boolean hasAdditionalData(Class aClass) {
        if (hasAdditionalData.get(aClass) == null) {
            Collection<String> fields = getFields(aClass, AdditionalData.class);
            Map m = hasAdditionalData;
            m.put(aClass, (fields != null && !fields.isEmpty()));
            hasAdditionalData = m;
        }

        return hasAdditionalData.get(aClass);
    }

    /**
     * get the fieldname used in Mongo for the corresponding field
     * depending on whether camelcase-conversion is enabled globally
     * and whether it is enabled for this specific property
     * also takes "." as path separator into account
     *
     * @param clz
     * @param field
     * @return
     */
    @SuppressWarnings("StatementWithEmptyBody")
    public String getMongoFieldName(Class clz, String field) {
        return getMongoFieldName(clz, field, isAnnotationOnAnyField(clz, AdditionalData.class));
    }
    public String getMongoFieldName(Class clz, String field, boolean ignoreUnknownField) {
        if (clz == null || Map.class.isAssignableFrom(clz)) return field;

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
                if (ignoreUnknownField) {
                    if (ccc) {
                        return createCamelCase(field, false);
                    } else {
                        return field;
                    }
                }

                throw new AnnotationAndReflectionException("Field not found " + field + " in cls: " + clz.getName());
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

            if (f.isAnnotationPresent(Version.class)) {
                Version v = f.getAnnotation(Version.class);

                if (!v.fieldName().equals(".")) {
                    return v.fieldName();
                }
            }

            if (f.isAnnotationPresent(Id.class)) {
                return "_id";
            }

            ret = f.getName();
            Entity ent = getAnnotationFromHierarchy(cls, Entity.class);
            Embedded emb = getAnnotationFromHierarchy(cls, Embedded.class);

            if ((ccc && ent != null && ent.translateCamelCase())
                    || (ccc && emb != null && emb.translateCamelCase())) {
                ret = convertCamelCase(ret);
            }
        }

        fieldNameCache.computeIfAbsent(cls, k->new HashMap<>()).put(field, ret);
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
        String[] f = n.split("_");
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

        for (int i = 0; i < n.length(); i++) {
            if (Character.isUpperCase(n.charAt(i)) && i > 0) {
                b.append("_");
            }

            b.append(n.substring(i, i + 1).toLowerCase());
        }

        //b.append(n.substring(n.length() - 1));
        return b.toString();
    }

    /**
     * return list of fields in class - including hierachy!!!
     *
     * @param clz class to get all fields for
     * @return list of fields in that class
     */
    public List<Field> getAllFields(Class clz) {
        if (clz == null || Map.class.isAssignableFrom(clz)) return new ArrayList<Field>();

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
            Field[] declaredFields = c.getDeclaredFields();

            if (declaredFields.length > 0) {
                for (Field declaredField : declaredFields) {
                    if (!declaredField.getName().startsWith("$jacoco")) {
                        ret.add(declaredField);
                    }
                }
            }
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
        if (clz == null || Map.class.isAssignableFrom(clz)) return null;

        String key = clz.toString() + "->" + fld;
        Field val = fieldCache.get(key);

        if (val != null) {
            return val;
        }

        Map<String, Field> fc = fieldCache;
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

            if (f == null) { return null; }

            if (!Modifier.isStatic(f.getModifiers())) {
                o = getRealObject(o);
                return f.get(o);
            }
        } catch (IllegalAccessException e) {
            logger.error("Illegal access to field " + fld + " of type " + o.getClass().getSimpleName());
        }

        return null;
    }

    public void setValue(Object o, Object value, String fld) {
        if (o == null) {
            return;
        }

        try {
            Field field = getField(getRealClass(o.getClass()), fld);

            if (!Modifier.isStatic(field.getModifiers())) {
                o = getRealObject(o);

                try {
                    field.set(o, value);
                } catch (Exception e) {
                    if (value != null) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Setting of value (" + value.getClass().getSimpleName() + ") failed for field " + field.getName() + "- trying type-conversion");
                        }

                        field.set(o, convertType(value, fld, field.getType()));
                    }

                    if (logger.isDebugEnabled()) {
                        logger.debug("Type conversion was successful");
                    }
                }
            }
        } catch (IllegalAccessException e) {
            logger.error("Illegal access to field " + fld + " of toype " + o.getClass().getSimpleName());
        }
    }

    @SuppressWarnings("CatchMayIgnoreException")
    public static Object convertType(Object value, String fieldName, Class<?> fieldType) {
        //Doing some type conversions... lots of :-(
        if (value instanceof Number) {
            Number n = (Number) value;

            if (fieldType.equals(Integer.class) || fieldType.equals(int.class)) {
                return n.intValue();
            } else if (fieldType.equals(Long.class) || fieldType.equals(long.class)) {
                return n.longValue();
            } else if (fieldType.equals(Double.class) || fieldType.equals(double.class)) {
                return n.doubleValue();
            } else if (fieldType.equals(Float.class) || fieldType.equals(float.class)) {
                return n.floatValue();
            } else if (fieldType.equals(Byte.class) || fieldType.equals(byte.class)) {
                return n.byteValue();
            } else if (fieldType.equals(Short.class) || fieldType.equals(short.class)) {
                return n.shortValue();
            } else if (fieldType.equals(AtomicInteger.class)) {
                return new AtomicInteger(n.intValue());
            } else if (fieldType.equals(AtomicLong.class)) {
                return new AtomicLong(n.intValue());
            } else if (fieldType.equals(Date.class)) {
                //Fucking date / timestamp mixup
                return new Date(n.longValue());
            } else if (fieldType.equals(Boolean.class) || fieldType.equals(boolean.class)) {
                return n.intValue() == 1;
            } else if (fieldType.equals(String.class)) {
                return n.toString();
            } else {
                throw AnnotationAndReflectionException.wrongFieldType(fieldName, fieldType.toString(), value.getClass().toString());
            }
        } else if (value instanceof Boolean) {
            Boolean b = (Boolean) value;

            if (fieldType.equals(Integer.class) || fieldType.equals(int.class)) {
                return b ? 1 : 0;
            } else if (fieldType.equals(Long.class) || fieldType.equals(long.class)) {
                return b ? 1L : 0L;
            } else if (fieldType.equals(Double.class) || fieldType.equals(double.class)) {
                return b ? 1.0 : 0.0;
            } else if (fieldType.equals(Float.class) || fieldType.equals(float.class)) {
                return b ? 1.0f : 0.0f;
            } else if (fieldType.equals(Byte.class) || fieldType.equals(byte.class)) {
                return b ? (byte) 1 : 0;
            } else if (fieldType.equals(Short.class) || fieldType.equals(short.class)) {
                return b ? (short) 1 : 0;
            } else if (fieldType.equals(String.class)) {
                return b ? "true" : "false";
            } else if (fieldType.equals(AtomicBoolean.class)) {
                return new AtomicBoolean(b);
            } else {
                throw AnnotationAndReflectionException.wrongFieldType(fieldName, fieldType.toString(), value.getClass().toString());
            }
        } else if (value instanceof Date) {
            //Date/String mess-up?
            Date d = (Date) value;

            if (fieldType.equals(Long.class) || fieldType.equals(long.class)) {
                return d.getTime();
            } else if (fieldType.equals(GregorianCalendar.class)) {
                GregorianCalendar cal = new GregorianCalendar();
                cal.setTimeInMillis(d.getTime());
                return cal;
            } else if (fieldType.equals(String.class)) {
                SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
                return df.format(d);
            } else if (fieldType.equals(Instant.class)) {
                return d.toInstant();
            } else if (fieldType.equals(LocalDate.class)) {
                return d.toInstant().atZone(ZoneOffset.UTC).toLocalDate();
            } else if (fieldType.equals(LocalTime.class)) {
                return d.toInstant().atZone(ZoneOffset.UTC).toLocalTime();
            } else if (fieldType.equals(LocalDateTime.class)) {
                return d.toInstant().atZone(ZoneOffset.UTC).toLocalDateTime();
            }
        } else if (value instanceof String) {
            String s = (String) value;

            try {
                if (fieldType.equals(Date.class)) {
                    //Fucking date / timestamp mixup
                    if (s.length() == 8) {
                        //probably time-string 20120812
                        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
                        return df.parse(s);
                    } else if (s.indexOf('-') > 0) {
                        //maybe a date-String?
                        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                        return df.parse(s);
                    } else if (s.indexOf('.') > 0) {
                        //maybe a date-String?
                        SimpleDateFormat df = new SimpleDateFormat("dd.MM.yyyy");
                        return df.parse(s);
                    } else {
                        return new Date(Long.parseLong(s));
                    }
                } else if (fieldType.equals(MorphiumId.class)) {
                    return new MorphiumId(s);
                }
            } catch (Exception e) {
            }

            Method convertMethod = getConvertMethod(fieldType);

            if (convertMethod != null) {
                try {
                    return convertMethod.invoke(null, s);
                } catch (Exception e) {
                }
            }
        } else if (fieldType.isArray() && value instanceof List) {
            Object arr = Array.newInstance(fieldType, ((List) value).size());
            int idx = 0;

            for (Object io : ((List) value)) {
                try {
                    Array.set(arr, idx, io);
                } catch (Exception e1) {
                    Array.set(arr, idx, ((Integer) io).byteValue());
                }
            }

            return arr;
        } else if (value instanceof byte[]) {
            if (fieldType.equals(UUID.class)) {
                // Convert Java Legacy UUID from byte array to object
                ByteBuffer bb = ByteBuffer.wrap((byte[]) value);
                return new UUID(bb.getLong(), bb.getLong());
            }
        }

        throw AnnotationAndReflectionException.wrongFieldType(fieldName, fieldType.toString(), value.getClass().toString());
    }

    @SuppressWarnings("CatchMayIgnoreException")
    public static Method getConvertMethod(Class<?> fieldType) {
        try {
            return fieldType.getMethod("valueOf", String.class);
        } catch (NoSuchMethodException e) {
            try {
                return fieldType.getMethod("valueOf", CharSequence.class);
            } catch (NoSuchMethodException e1) {
                try {
                    return fieldType.getMethod("valueOf", Object.class);
                } catch (NoSuchMethodException e2) {
                    try {
                        return fieldType.getMethod("parse", String.class);
                    } catch (NoSuchMethodException e3) {
                        try {
                            return fieldType.getMethod("parse", CharSequence.class);
                        } catch (NoSuchMethodException e4) {
                            try {
                                return fieldType.getMethod("parse", Object.class);
                            } catch (NoSuchMethodException e5) {
                                try {
                                    return fieldType.getMethod("fromString", String.class); // for UUID
                                } catch (NoSuchMethodException e6) {
                                }
                            }
                        }
                    }
                }
            }
        }

        return null;
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
                logger.warn("Illegal reference?");
            }

            return null;
        } catch (IllegalAccessException e) {
            throw AnnotationAndReflectionException.of(e);
        }
    }

    public String getIdFieldName(Object o) {
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
    public List<String> getFields(Class cls, Class <? extends Annotation > ... annotations) {
        return getFields(cls, false, annotations);
    }

    @SuppressWarnings("CommentedOutCode")
    public List<String> getFields(Class cls, boolean ignoreEntity, Class <? extends Annotation > ... annotations) {
        if (cls == null) { return new ArrayList<>(); }

        StringBuilder stringBuilder = new StringBuilder(cls.toString());

        for (Class <? extends Annotation > a : annotations) {
            stringBuilder.append("/");
            stringBuilder.append(a.toString());
        }

        List<String> strings = fieldAnnotationListCache.get(stringBuilder.toString());

        if (strings != null) {
            return strings;
        }

        Map<String, List<String>> fa = fieldAnnotationListCache;
        List<String> ret = new ArrayList<>();
        Class sc = cls;
        sc = getRealClass(sc);
        Entity entity = getAnnotationFromHierarchy(sc, Entity.class);
        Embedded embedded = getAnnotationFromHierarchy(sc, Embedded.class);

        if (embedded != null && entity != null && !ignoreEntity) {
            logger.warn("Class " + cls.getName() + " does have both @Entity and @Embedded Annotations - not allowed! Assuming @Entity is right");
        }
        //
        //        if (embedded == null && entity == null && !ignoreEntity) {
        //            throw new IllegalArgumentException("This class " + cls.getName() + " does not have @Entity or @Embedded set, not even in hierachy - illegal!");
        //        }
        boolean tcc = true;

        if (embedded != null) {
            tcc = embedded.translateCamelCase();
        }
        if (entity != null) { tcc = entity.translateCamelCase(); }
        IgnoreFields ignoreFields = getAnnotationFromHierarchy(sc, IgnoreFields.class);
        LimitToFields limitToFields = getAnnotationFromHierarchy(sc, LimitToFields.class);
        List<String> fieldsToIgnore = new ArrayList<>();
        List<String> ignoreContains = new ArrayList<>();
        List<Pattern> ignoreRexex = new ArrayList<>();

        if (ignoreFields != null && ignoreFields.value().length != 0) {
            for (String f : ignoreFields.value()) {
                if (f.startsWith("~")) {
                    ignoreContains.add(f.substring(1));
                    continue;
                }

                if (f.startsWith("/") && f.endsWith("/")) {
                    ignoreRexex.add(Pattern.compile(f.substring(1).substring(0, f.length() - 2)));
                    continue;
                }

                fieldsToIgnore.add(f);
            }
        }
        List<String> fieldsToLimitTo = new ArrayList<>();

        if (limitToFields != null && limitToFields.value().length != 0) {
            fieldsToLimitTo.addAll(Arrays.asList(limitToFields.value()));
        }
        if (limitToFields != null && !limitToFields.type().equals(Object.class)) {
            List<Field> flds = getAllFields(limitToFields.type());

            for (Field f : flds) {
                fieldsToLimitTo.add(getMongoFieldName(limitToFields.type(), f.getName()));
            }
        }
        //getting class hierachy
        List<Field> fld = getAllFields(cls);

        for (Field f : fld) {
            if (annotations.length > 0) {
                boolean found = false;

                for (Class <? extends Annotation > a : annotations) {
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

            if (f.isAnnotationPresent(Transient.class)) {
                continue;
            }

            //ignoring fields...
            boolean ignore = false;
            String conv = f.getName().replaceAll("\\$", "");

            if (tcc && ccc) {
                conv = convertCamelCase(f.getName());
            }

            if (fieldsToIgnore.contains(conv) || fieldsToIgnore.contains(f.getName())) {
                ignore = true;
            }

            if (!ignore) {
                for (String ign : ignoreContains) {
                    if (f.getName().contains(ign) || conv.contains(ign)) {
                        ignore = true;
                        break;
                    }
                }
            }

            if (!ignore) {
                for (Pattern reg : ignoreRexex) {
                    if (reg.matcher(f.getName()).matches() || reg.matcher(conv).matches()) {
                        ignore = true;
                    }
                }
            }

            if (!ignore && !fieldsToLimitTo.isEmpty() && !fieldsToLimitTo.contains(conv) && !fieldsToLimitTo.contains(f.getName())) {
                ignore = true;
            }

            if (!ignore) {
                ret.add(conv);
            }
        }
        fa.put(stringBuilder.toString(), ret);
        fieldAnnotationListCache = fa;
        return ret;
    }

    public <T> T getRealObject(T o) {
        if (isProxy(o.getClass())) {
            //not stored or Proxy?
            try {
                Field f1 = o.getClass().getDeclaredField("CGLIB$CALLBACK_0");
                f1.setAccessible(true);
                Object delegate = f1.get(o);
                Method m = delegate.getClass().getMethod("__getDeref");
                o = (T) m.invoke(delegate);
            } catch (Exception e) {
                logger.error("Exception: ", e);
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

    public List<Annotation> getAllAnnotationsFromHierachy(Class<?> cls, Class <? extends Annotation > ... anCls) {
        cls = getRealClass(cls);
        List<Annotation> ret = new ArrayList<>();
        Class<?> z = cls;

        while (!z.equals(Object.class)) {
            if (z.getAnnotations().length != 0) {
                if (anCls.length == 0) {
                    ret.addAll(Arrays.asList(z.getAnnotations()));
                } else {
                    for (Annotation a : z.getAnnotations()) {
                        for (Class <? extends Annotation > ac : anCls) {
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

    public String getAdditionalDataField(Class<?> cls) {
        List<String> lst = getFields(cls, AdditionalData.class);

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

    public void callLifecycleMethod(Class <? extends Annotation > type, Object on) {
        callLifecycleMethod(type, on, new ArrayList());
    }

    private void callLifecycleMethod(Class <? extends Annotation > type, Object on, List calledOn) {
        if (on == null) {
            return;
        }

        if (isProxy(on.getClass())) {
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
                logger.error("Exception: ", e);
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
        if (!isAnnotationPresentInHierarchy(cls, Lifecycle.class)) {
            return;
        }

        List<String> flds = getFields(on.getClass());

        for (String f : flds) {
            Field field = getField(on.getClass(), f);

            if ((isAnnotationPresentInHierarchy(field.getType(), Entity.class) || isAnnotationPresentInHierarchy(field.getType(), Embedded.class)) &&
                    isAnnotationPresentInHierarchy(field.getType(), Lifecycle.class)) {
                field.setAccessible(true);

                try {
                    callLifecycleMethod(type, field.get(on), calledOn);
                } catch (IllegalAccessException e) {
                    logger.error("Exception: ", e);
                }
            }
        }

        //Already stored - should not change during runtime
        if (lifeCycleMethods.get(cls) != null) {
            if (lifeCycleMethods.get(cls).get(type) != null) {
                try {
                    lifeCycleMethods.get(cls).get(type).invoke(on);
                } catch (IllegalAccessException e) {
                    throw AnnotationAndReflectionException.of(e);
                } catch (InvocationTargetException e) {
                    if (e.getCause().getClass().equals(MorphiumAccessVetoException.class)) {
                        throw (RuntimeException) e.getCause();
                    }

                    throw AnnotationAndReflectionException.of(e);
                }
            }

            return;
        }

        Map < Class <? extends Annotation>, Method> methods = new HashMap<>();

        //Methods must be public
        for (Method m : cls.getMethods()) {
            for (Annotation a : m.getAnnotations()) {
                methods.put(a.annotationType(), m);
            }
        }

        Map < Class<?>, Map < Class <? extends Annotation >, Method >> lc = lifeCycleMethods;
        lc.put(cls, methods);
        if (methods.get(type) != null) {
            try {
                methods.get(type).invoke(on);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw AnnotationAndReflectionException.of(e);
            }
        }
        lifeCycleMethods = lc;
    }

    public boolean isAsyncWrite(Class<?> cls) {
        AsyncWrites wb = getAnnotationFromHierarchy(cls, AsyncWrites.class);
        return wb != null && wb.value();
    }

    private <T> boolean isProxy(Class <? extends T > aClass) {
        if (aClass == null) { return false; }

        return aClass.getName().contains("$$EnhancerByCGLIB$$");
    }

    private <T> Class<?> realClassOf(Class <? extends T > superClass) {
        try {
            return classForName(superClass.getName().substring(0, superClass.getName().indexOf("$$")));
        } catch (ClassNotFoundException e) {
            throw AnnotationAndReflectionException.of(e);
        }
    }

    private static final class AnnotationAndReflectionException extends RuntimeException {

        private AnnotationAndReflectionException(Exception e) {
            super(e);
        }

        private AnnotationAndReflectionException(String message) {
            super(message);
        }

        private static AnnotationAndReflectionException of(Exception e) {
            return new AnnotationAndReflectionException(e);
        }

        private static AnnotationAndReflectionException wrongFieldType(String fieldName, String expectedFieldType, String actualFieldType) {
            final String message = format("could not set field %s: Field has type %s got type %s", fieldName, expectedFieldType, actualFieldType);
            return new AnnotationAndReflectionException(message);
        }
    }
}
