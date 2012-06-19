package de.caluga.morphium;

/**
 * User: Stephan Bösebeck
 * Date: 19.06.12
 * Time: 11:47
 * <p/>
 * TODO: Add documentation here
 */
public interface NameProvider {
    public String getCollectionName(Class<?> type, ObjectMapper om, boolean translateCamelCase, boolean useFQN, String specifiedName, Morphium morphium);
}
