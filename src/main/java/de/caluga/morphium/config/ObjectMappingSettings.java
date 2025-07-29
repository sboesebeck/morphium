package de.caluga.morphium.config;

import de.caluga.morphium.annotations.Embedded;

@Embedded
public class ObjectMappingSettings {

    private boolean checkForNew = true;
    private boolean autoValues = true;
    private boolean objectSerializationEnabled = true;
    private boolean camelCaseConversionEnabled = true;
    private boolean warnOnNoEntitySerialization = false;
}
