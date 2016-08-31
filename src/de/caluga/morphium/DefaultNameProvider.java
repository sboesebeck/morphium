package de.caluga.morphium;

import java.util.Hashtable;

/**
 * User: Stephan Bösebeck
 * Date: 19.06.12
 * Time: 12:00
 * <p/>
 * Name Providers define the name of a given collection. Can be set in config for any type
 */
public final class DefaultNameProvider implements NameProvider {
    private Hashtable<Class<?>, String> collectionNameCache = new Hashtable<>();

    @Override
    public String getCollectionName(Class<?> type, ObjectMapper om, boolean translateCamelCase, boolean useFQN, String specifiedName, Morphium morphium) {
        String name = collectionNameCache.get(type);
        if (name == null) {

            name = type.getSimpleName();


            if (useFQN) {
                name = type.getName().replaceAll("\\.", "_");
            }
            if (specifiedName != null) {
                name = specifiedName;
            } else {

                if (translateCamelCase) {
                    AnnotationAndReflectionHelper ar;
                    if (morphium != null) {
                        ar = morphium.getARHelper();
                    } else {
                        ar = new AnnotationAndReflectionHelper(true);
                    }
                    name = ar.convertCamelCase(name);
                }
            }
            try {
                collectionNameCache.put(type, name);
            } catch (Exception e) {
                //swallow exception. If add did not work, it will eventually
                new Logger(DefaultNameProvider.class).debug("Could not store name in cache!");
            }
        }
        return name;
    }
}
