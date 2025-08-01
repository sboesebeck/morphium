package de.caluga.morphium.config;

import de.caluga.morphium.annotations.Embedded;

@Embedded
public class ObjectMappingSettings {

    private boolean checkForNew = true;
    private boolean autoValues = true;
    private boolean objectSerializationEnabled = true;
    private boolean camelCaseConversionEnabled = true;
    private boolean warnOnNoEntitySerialization = false;
    public boolean isCheckForNew() {
        return checkForNew;
    }
    public ObjectMappingSettings setCheckForNew(boolean checkForNew) {
        this.checkForNew = checkForNew;
        return this;
    }
    public boolean isAutoValuesEnabled() {
        return autoValues;
    }
    public boolean isAutoValues() {
        return autoValues;
    }
    public ObjectMappingSettings setAutoValues(boolean autoValues) {
        this.autoValues = autoValues;
        return this;
    }
    public boolean isObjectSerializationEnabled() {
        return objectSerializationEnabled;
    }
    public ObjectMappingSettings setObjectSerializationEnabled(boolean objectSerializationEnabled) {
        this.objectSerializationEnabled = objectSerializationEnabled;
        return this;
    }
    public boolean isCamelCaseConversionEnabled() {
        return camelCaseConversionEnabled;
    }
    public ObjectMappingSettings setCamelCaseConversionEnabled(boolean camelCaseConversionEnabled) {
        this.camelCaseConversionEnabled = camelCaseConversionEnabled;
        return this;
    }
    public boolean isWarnOnNoEntitySerialization() {
        return warnOnNoEntitySerialization;
    }
    public ObjectMappingSettings setWarnOnNoEntitySerialization(boolean warnOnNoEntitySerialization) {
        this.warnOnNoEntitySerialization = warnOnNoEntitySerialization;
        return this;
    }
}
