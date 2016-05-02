package de.caluga.morphium;

/**
 * User: Stephan Bösebeck
 * Date: 19.06.12
 * Time: 12:00
 * <p/>
 * Name Providers define the name of a given collection. Can be set in config for any type
 */
public final class DefaultNameProvider implements NameProvider {

    @Override
    public String getCollectionName(Class<?> type, ObjectMapper om, boolean translateCamelCase, boolean useFQN, String specifiedName, Morphium morphium) {

        String name = type.getSimpleName();


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
        return name;
    }
}
