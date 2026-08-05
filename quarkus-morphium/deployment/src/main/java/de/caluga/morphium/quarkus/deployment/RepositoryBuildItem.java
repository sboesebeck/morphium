package de.caluga.morphium.quarkus.deployment;

import io.quarkus.builder.item.MultiBuildItem;

/**
 * Build item carrying metadata about a discovered {@code @Repository} interface.
 * One instance per repository interface, consumed by the code-generation step.
 */
public final class RepositoryBuildItem extends MultiBuildItem {

    private final String interfaceName;
    private final String entityClassName;
    private final String idClassName;
    private final String idFieldName;

    public RepositoryBuildItem(String interfaceName,
                               String entityClassName,
                               String idClassName,
                               String idFieldName) {
        this.interfaceName = interfaceName;
        this.entityClassName = entityClassName;
        this.idClassName = idClassName;
        this.idFieldName = idFieldName;
    }

    public String getInterfaceName()  { return interfaceName; }
    public String getEntityClassName() { return entityClassName; }
    public String getIdClassName()    { return idClassName; }
    public String getIdFieldName()    { return idFieldName; }
}
