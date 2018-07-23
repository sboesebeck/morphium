package de.caluga.morphium.objectmapper;

import de.caluga.morphium.*;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.driver.MorphiumId;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Decoder;
import sun.reflect.ReflectionFactory;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.lang.reflect.*;
import java.util.*;

public class UnmarshallHelper {

    private final List<Object> mongoTypes;
    private final AnnotationAndReflectionHelper anhelper;
    private final Map<Class<?>, NameProvider> nameProviderByClass;
    private final Morphium morphium;
    private final ObjectMapper objectMapper;

    private final ReflectionFactory reflection = ReflectionFactory.getReflectionFactory();
    private final Logger log = LoggerFactory.getLogger(MarshallHelper.class);
    private boolean ignoreReadOnly = false;
    private boolean ignoreEntity = false;

    public UnmarshallHelper(AnnotationAndReflectionHelper anhelper, Map<Class<?>, NameProvider> nameProviderByClass, Morphium morphium, ObjectMapper objectMapper) {
        mongoTypes = Collections.synchronizedList(new ArrayList<>());

        mongoTypes.add(String.class);
        mongoTypes.add(Character.class);
        mongoTypes.add(Integer.class);
        mongoTypes.add(Long.class);
        mongoTypes.add(Float.class);
        mongoTypes.add(Double.class);
        mongoTypes.add(Date.class);
        mongoTypes.add(Boolean.class);
        mongoTypes.add(Byte.class);
        this.anhelper = anhelper;
        this.nameProviderByClass = nameProviderByClass;
        this.morphium = morphium;

        this.objectMapper = objectMapper;
    }

    public <T> T unmarshall(Class<? extends T> theClass, Map<String, Object> o) {
        if (o == null) return null;
        if (o.get("_b64data") != null) {
            //binarySerializedObject

            ObjectInputStream in = null;
            try {
                in = new ObjectInputStream(new ByteArrayInputStream(new BASE64Decoder().decodeBuffer((String) ((Map) o).get("_b64data"))));
                return (T) in.readObject();
            } catch (Exception e) {
                throw new RuntimeException("De-serialization failed", e);
            }

        }
        /////////////////////////////////////////
        ///getting type
        //

        Class cls = theClass;

        try {
            String cN = (String) o.get("class_name");
            if (cN == null) {
                cN = (String) o.get("className");
            }
            if (cN != null) {
                cls = Class.forName(cN);
            }

        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        if (cls == null) {
//            log.warn("Could not find type for map, assuming it is just a map");
            Map ret = new LinkedHashMap();
            for (Map.Entry entry : o.entrySet()) {
                ret.put(entry.getKey(), unmarshallIfPossible(null, entry.getValue()));
            }
            return (T) ret;
        }
        if (cls.isEnum()) {
            T[] en = (T[]) cls.getEnumConstants();
            for (Enum e : ((Enum[]) en)) {
                if (o instanceof Map) {
                    if (e.name().equals(o.get("name"))) {
                        return (T) e;
                    }
                } else {
                    if (e.name().equals(o)) {
                        return (T) e;
                    }
                }
            }
        }
        if (Map.class.isAssignableFrom(cls)) {
            Map ret = new LinkedHashMap();
            for (Map.Entry entry : (Set<Map.Entry>) ((Map) o).entrySet()) {
                ret.put(entry.getKey(), unmarshallIfPossible(null, entry.getValue()));
            }
            return (T) ret;
        }

        T result = null;
        try {
            result = (T) cls.newInstance();
        } catch (Exception ignored) {
        }
        if (result == null) {
            final Constructor<Object> constructor;
            try {
                constructor = (Constructor<Object>) reflection.newConstructorForSerialization(
                        cls, Object.class.getDeclaredConstructor());
                result = (T) constructor.newInstance();
            } catch (Exception e) {
                throw new RuntimeException("could not instanciate class " + cls.getSimpleName(), e);
            }
        }

        ///////////////////////////////////////
        ///recursing through fields
        //
        //
        List<Field> fields = anhelper.getAllFields(cls);
        for (Field fld : fields) {
            try {
                if (fld.isAnnotationPresent(WriteOnly.class)) {
                    continue; ///not read in write-only fields
                }
                fld.setAccessible(true);
                Class fieldType = fld.getType();
                String fName = anhelper.getFieldName(cls, fld.getName());

                if (o.get(fName) == null) {
//                    if (!fieldType.isPrimitive()) {
//                        fld.set(result, null);
//                    }
                    continue;
                }
                Reference r = fld.getAnnotation(Reference.class);
                boolean isentity = anhelper.isAnnotationPresentInHierarchy(fieldType, Embedded.class) || anhelper.isAnnotationPresentInHierarchy(fieldType, Entity.class);
                if (fieldType.isArray() && fieldType.getComponentType().isPrimitive()) {
                    //should get Primitives from DB as well
                    Object valueFromDb = o.get(fName);
                    if (valueFromDb instanceof Map && ((Map) valueFromDb).isEmpty()) {
                        fld.set(result, null);
                    } else {
                        if (valueFromDb.getClass().isArray() && valueFromDb.getClass().getComponentType().isPrimitive()) {
                            fld.set(result, valueFromDb);

                        } else {
                            Object arr = Array.newInstance(fieldType.getComponentType(), ((List) valueFromDb).size());

                            int count = 0;
                            if (fieldType.getComponentType().equals(int.class)) {
                                for (Number i : (List<Number>) valueFromDb) {
                                    Array.set(arr, count++, i.intValue());
                                }
                            } else if (fieldType.getComponentType().equals(double.class)) {
                                for (Number i : (List<Number>) valueFromDb) {
                                    Array.set(arr, count++, i.doubleValue());
                                }
                            } else if (fieldType.getComponentType().equals(float.class)) {
                                for (Number i : (List<Number>) valueFromDb) {
                                    Array.set(arr, count++, i.floatValue());
                                }
                            } else if (fieldType.getComponentType().equals(boolean.class)) {
                                for (Boolean i : (List<Boolean>) valueFromDb) {
                                    Array.set(arr, count++, i.booleanValue());
                                }
                            } else if (fieldType.getComponentType().equals(byte.class)) {
                                for (Number i : (List<Number>) valueFromDb) {
                                    Array.set(arr, count++, i.byteValue());
                                }
                            } else if (fieldType.getComponentType().equals(char.class)) {
                                for (Character i : (List<Character>) valueFromDb) {
                                    Array.set(arr, count++, i.charValue());
                                }
                            } else if (fieldType.getComponentType().equals(short.class)) {
                                for (Number i : (List<Number>) valueFromDb) {
                                    Array.set(arr, count++, i.shortValue());
                                }
                            } else if (fieldType.getComponentType().equals(long.class)) {
                                for (Number i : (List<Number>) valueFromDb) {
                                    Array.set(arr, count++, i.longValue());
                                }

                            }
                            fld.set(result, arr);
                        }
                    }
                    ///////////////////
                    ///////
                    //// List handling
                    //
                } else if (List.class.isAssignableFrom(fieldType) || fieldType.isArray()) {
                    List lst = (List) o.get(fName);
                    List resList = getUnmarshalledList(result, fld.getGenericType(), fName, r, lst);
                    if (fieldType.isArray()) {
                        fld.set(result, resList.toArray((Object[]) Array.newInstance(fieldType.getComponentType(), 0)));
                    } else {
                        fld.set(result, resList);
                    }

                    ////////////////////////////////////////
                    ///////
                    ///   Map Value
                    //
                } else if (Map.class.isAssignableFrom(fieldType)) {
                    Map map = (Map) o.get(fName);
                    Map resMap = new LinkedHashMap();
                    for (Map.Entry en : (Set<Map.Entry>) map.entrySet()) {
                        resMap.put(unmarshallIfPossible(null, en.getKey()), unmarshallIfPossible(null, en.getValue()));
                    }
                    fld.set(result, resMap);
                } else {

                    //////////////////////////////
                    ////////
                    ///// just a field
                    ///
                    Object value = unmarshallIfPossible(fieldType, o.get(fName));
                    if (isentity) {
                        if (fld.isAnnotationPresent(Reference.class)) {
                            Map ref = (Map) o.get(fName);
                            //MorphiumReference mr=unmarshall(MorphiumReference.class,ref);
                            if (r.lazyLoading()) {
                                value = morphium.createLazyLoadedEntity(fld.getType(), ref.get("id"), ref.get("collection_name").toString());
                            } else {
                                value = morphium.findById(fld.getType(), ref.get("id"), ref.get("collection_name").toString());
                            }

                        } else {
                            try {
                                Field idf = anhelper.getIdField(fieldType);
                                idf.setAccessible(true);
                                idf.set(value, null);
                            } catch (Exception e) {
                                //swallow
                            }
                        }
                    } else if (value != null && !fieldType.isAssignableFrom(value.getClass())) {
                        if (value instanceof String) {
                            log.info("Got String but have somethign different");
                            if (fieldType.equals(Integer.class) || fieldType.equals(int.class)) {
                                value = Integer.valueOf((String) value);
                            } else if (fieldType.equals(Double.class) || fieldType.equals(Double.class)) {
                                value = Double.valueOf((String) value);
                            } else if (fieldType.equals(Long.class) || fieldType.equals(long.class)) {
                                value = Long.valueOf((String) value);

                            } else if (fieldType.equals(Float.class) || fieldType.equals(float.class)) {
                                value = Float.valueOf((String) value);
                            } else if (fieldType.equals(ObjectId.class)) {
                                value = new ObjectId((String) value);
                            } else if (fieldType.equals(MorphiumId.class)) {
                                value = new MorphiumId((String) value);
                            }
                        } else if (value instanceof Long) {
                            if (fieldType.equals(Integer.class) || fieldType.equals(int.class)) {
                                value = ((Long) value).intValue();
                            } else if (fieldType.equals(Double.class) || fieldType.equals(Double.class)) {
                                value = ((Long) value).doubleValue();
                            } else if (fieldType.equals(Float.class) || fieldType.equals(float.class)) {
                                value = ((Long) value).floatValue();
                            } else {
                                //log.warn("Try string conversion to Long");
                                value = Long.valueOf(value.toString());
                            }
                        } else if (value instanceof Integer) {
                            if (fieldType.equals(Long.class) || fieldType.equals(long.class)) {
                                value = ((Integer) value).longValue();
                            } else if (fieldType.equals(Double.class) || fieldType.equals(double.class)) {
                                value = ((Integer) value).doubleValue();
                            } else if (fieldType.equals(Float.class) || fieldType.equals(float.class)) {
                                value = ((Integer) value).floatValue();
                            } else {
                                //log.warn("Try string conversion to Integer");
                                value = Integer.valueOf(value.toString());
                            }
                        } else if (value instanceof Float) {
                            if (fieldType.equals(Long.class) || fieldType.equals(long.class)) {
                                value = ((Float) value).longValue();
                            } else if (fieldType.equals(Integer.class) || fieldType.equals(int.class)) {
                                value = ((Float) value).intValue();
                            } else if (fieldType.equals(Double.class) || fieldType.equals(double.class)) {
                                value = ((Float) value).doubleValue();
                            } else {
                                //log.warn("Try string conversion to Float");
                                value = Float.valueOf(value.toString());
                            }
                        } else if (value instanceof Double) {
                            if (fieldType.equals(Long.class) || fieldType.equals(long.class)) {
                                value = ((Double) value).longValue();
                            } else if (fieldType.equals(Integer.class) || fieldType.equals(int.class)) {
                                value = ((Double) value).intValue();
                            } else if (fieldType.equals(Float.class) || fieldType.equals(float.class)) {
                                value = ((Double) value).floatValue();
                            } else {
                                //log.warn("Try string conversion to Double");
                                value = Double.valueOf(value.toString());
                            }
                        }
                    }
                    fld.set(result, value);
                }
            } catch (IllegalAccessException e) {
                //TODO: Implement Handling
                throw new RuntimeException(e);

            }
        }
        if (anhelper.isAnnotationPresentInHierarchy(cls, Embedded.class)) {
            try {
                Field f = anhelper.getIdField(result);
                if (f != null) {
                    f.setAccessible(true);
                    try {
                        f.set(result, null);
                    } catch (IllegalAccessException e) {
                        log.error("Could not erase id from embedded entity", e);
                    }
                }
            } catch (Exception e) {
                //e.printStackTrace();
                //swallow
            }
        }
        if (anhelper.isAnnotationPresentInHierarchy(cls, PartialUpdate.class) || cls.isInstance(PartiallyUpdateable.class)) {
            return morphium.createPartiallyUpdateableEntity(result);
        }
        return result;
    }


    private <T> List getUnmarshalledList(T result, Type type, String fName, Reference r, List lst) {
        List resList = new ArrayList();

        for (Object listElement : lst) {
            if (listElement == null) {
                resList.add(null);
                continue;
            }
            Class elementType = null;

            Type typeOfListElement = null;
            if (type != null && type instanceof ParameterizedType) {
                typeOfListElement = ((ParameterizedType) type).getActualTypeArguments()[0];
            }

            if (listElement instanceof List) {
                ///////////////////
                ///// List in a list
                ////

//                Type elt = null;
//                if (typeOfListElement instanceof ParameterizedType) {
//                    ParameterizedType p = (ParameterizedType) typeOfListElement;
//                    elt = (ParameterizedType) p.getActualTypeArguments()[0];
//                } else {
//
//                }

                List inner = getUnmarshalledList(result, typeOfListElement, fName, r, (List) listElement);

                resList.add(inner);
                continue;
            }
            if (listElement instanceof Map) {
                ////////////
                //// map in a list
                ///
                ////// Entity in this list
                if (((Map) listElement).get("class_name") != null) {
                    try {
                        elementType = Class.forName((String) ((Map) listElement).get("class_name"));
                        if (anhelper.isEntity(elementType) && r != null) {
                            //MorphiumReference ref = unmarshall(MorphiumReference.class, (Map<String, Object>) o.get(fName));
                            Map<String, Object> map = (Map<String, Object>) listElement;
                            if (r.lazyLoading()) {
                                resList.add(morphium.createLazyLoadedEntity(elementType, map.get("id"), (String) map.get("collection_name")));
                            } else {
                                Object deref = morphium.findById(elementType, map.get("id"), (String) map.get("collection_name"));
                                resList.add(deref);
                            }
                        } else {
                            resList.add(unmarshall(elementType, (Map<String, Object>) listElement));

                        }
                    } catch (ClassNotFoundException e) {
                        //TODO: Implement Handling
                        throw new RuntimeException(e);
                    }
                    continue;
                }

                if (((Map) listElement).get("_b64data") != null) {
                    //binarySerializedObject

                    ObjectInputStream in = null;
                    try {
                        in = new ObjectInputStream(new ByteArrayInputStream(new BASE64Decoder().decodeBuffer((String) ((Map) listElement).get("_b64data"))));
                        resList.add(in.readObject());
                        continue;
                    } catch (Exception e) {
                        throw new RuntimeException("De-serialization failed", e);
                    }

                }

                //Type of key should be String - ALWAYS
                //Type of value is
                Type typeOfValue = null;

                if (typeOfListElement instanceof ParameterizedType) {
                    typeOfValue = ((ParameterizedType) typeOfListElement).getActualTypeArguments()[1];
                } else {
                    //should be class
                    if (anhelper.isEntity((Class) typeOfListElement)) {
                        if (r != null) {
                            String collectionName = r.targetCollection();
                            if (collectionName.equals(".")) {
                                collectionName = morphium.getMapper().getCollectionName((Class) typeOfListElement);
                            }
                            if (r.lazyLoading()) {
                                resList.add(morphium.createLazyLoadedEntity((Class) typeOfListElement, ((Map) listElement).get("id"), collectionName));
                            } else if (!r.lazyLoading()) {
                                resList.add(morphium.findById((Class) typeOfListElement, ((Map) listElement).get("id"), collectionName));
                            }
                        } else {
                            resList.add(unmarshall((Class) typeOfListElement, (Map) listElement));
                        }
                        continue;
                    }

                }

                if (typeOfValue instanceof Class) {
                    if (r != null) {
                        String collectionName = r.targetCollection();
                        if (collectionName.equals(".")) {
                            collectionName = morphium.getMapper().getCollectionName(elementType);
                        }
                        if (r.lazyLoading()) {
                            resList.add(morphium.createLazyLoadedEntity(elementType, ((Map) listElement).get("id"), collectionName));
                        } else if (!r.lazyLoading()) {
                            resList.add(morphium.findById(elementType, ((Map) listElement).get("id"), collectionName));
                        }
                    } else {
                        resList.add(unmarshallIfPossible((Class) typeOfValue, listElement));
                    }
                    continue;
                } else {
                    //recursing
                    Map m = new LinkedHashMap();

                    for (Map.Entry e : (Set<Map.Entry>) ((Map) listElement).entrySet()) {
                        m.put(e.getKey(), unmarshallIfPossible(elementType, listElement));
                    }
                    resList.add(m);
                    continue;
                }
            }


                resList.add(unmarshallIfPossible(elementType, listElement));


        }
        return resList;
    }

    private Object unmarshallIfPossible(Class fieldType, Object o) {
        if (o == null) return null;
//        for (TypeMapper m : customTypeMapper.values()) {
//            if (m.matches(o)) {
//                return m.unmarshall(o);
//            }
//        }

        if (o instanceof Map) {
            if (((Map) o).get("_b64data") != null) {
                //binarySerializedObject
                ObjectInputStream in = null;
                try {
                    in = new ObjectInputStream(new ByteArrayInputStream(new BASE64Decoder().decodeBuffer((String) ((Map) o).get("_b64data"))));
                    return in.readObject();
                } catch (Exception e) {
                    throw new RuntimeException("De-serialization failed", e);
                }

            }
            String cname = (String) ((Map) o).get("class_name");
            if (cname != null) {
                try {
                    fieldType = Class.forName(cname);
                } catch (ClassNotFoundException e) {
                    //TODO: Implement Handling
                    throw new RuntimeException(e);
                }
            }
            return unmarshall(fieldType, (Map) o);
        }
        if (fieldType != null && fieldType.isEnum()) {
            String n = o.toString();
            if (o instanceof Map) {
                //in list or map
                n = (String) ((Map) o).get("name");
            }
            Enum en[] = (Enum[]) fieldType.getEnumConstants();
            for (Enum e : en) {
                if (e.name().equals(n)) {
                    return e;
                }
            }
            throw new IllegalArgumentException("Enum constant not found: " + n);
        }

        if (o instanceof List) {
            List ret = new ArrayList();
            for (Object elem : (List) o) {
                ret.add(unmarshallIfPossible(null, elem));
            }
            return ret;


        }
        if (fieldType != null && fieldType.isPrimitive() && o == null) {
            return 0;
        }
        //conversion
        if (fieldType != null && o != null) {
            if (fieldType.equals(String.class) && o instanceof Integer) {
                return o.toString();
            }
        }
        return o;
    }

}
