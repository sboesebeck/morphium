package de.caluga.morphium;

/**
 * User: Stephan Bösebeck
 * Date: 19.06.12
 * Time: 12:00
 * <p/>
 * TODO: Add documentation here
 */
public final class DefaultNameProvider implements NameProvider {
    public static final DefaultNameProvider def = new DefaultNameProvider();

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
                name = om.convertCamelCase(name);
            }
        }
        return name;
    }
}
