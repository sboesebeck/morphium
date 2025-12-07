package de.caluga.morphium;

import de.caluga.morphium.objectmapping.MorphiumObjectMapper;
import org.slf4j.LoggerFactory;

import java.util.Hashtable;

/**
 * User: Stephan Bösebeck
 * Date: 19.06.12
 * Time: 12:00
 * <p>
 * Name Providers define the name of a given collection. Can be set in config for any type
 */
public final class DefaultNameProvider implements NameProvider {
    private final Hashtable<Class<?>, String> collectionNameCache = new Hashtable<>();

    @Override
    public String getCollectionName(Class<?> type, MorphiumObjectMapper om, boolean translateCamelCase, boolean useFQN, String specifiedName, Morphium morphium) {
        String name = specifiedName;
        if (name == null) {
            name = collectionNameCache.get(type);
        }
        if (name == null) {
            name = type.getSimpleName();
            if (useFQN) {
                name = type.getName().replaceAll("\\.", "_");
            }

            if (translateCamelCase) {
                AnnotationAndReflectionHelper ar;
                if (morphium != null) {
                    ar = morphium.getARHelper();
                } else {
                    ar = new AnnotationAndReflectionHelper(true);
                }
                name = ar.convertCamelCase(name);
            }

            try {
                collectionNameCache.put(type, name);
            } catch (Exception e) {
                //swallow exception. If add did not work, it will eventually
                LoggerFactory.getLogger(DefaultNameProvider.class).debug("Could not store name in cache!");
            }
        }
        return name;
    }
}
