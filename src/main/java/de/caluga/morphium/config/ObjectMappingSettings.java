package de.caluga.morphium.config;

import de.caluga.morphium.annotations.Embedded;

@Embedded
public class ObjectMappingSettings extends Settings {

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
    public ObjectMappingSettings enableAutoValues() {
        autoValues = true;
        return this;
    }
    public ObjectMappingSettings disableAutoValues() {
        autoValues = false;
        return this;
    }
    public boolean isAutoValues() {
        return autoValues;
    }
    public ObjectMappingSettings setAutoValues(boolean autoValues) {
        this.autoValues = autoValues;
        return this;
    }
    public ObjectMappingSettings disableObjectSerialization() {
        objectSerializationEnabled = false;
        return this;
    }
    public ObjectMappingSettings enableObjectSerialization() {
        objectSerializationEnabled = true;
        return this;
    }
    public boolean isObjectSerializationEnabled() {
        return objectSerializationEnabled;
    }
    public ObjectMappingSettings setObjectSerializationEnabled(boolean objectSerializationEnabled) {
        this.objectSerializationEnabled = objectSerializationEnabled;
        return this;
    }
    public ObjectMappingSettings disableCamelCaseConversion() {
        camelCaseConversionEnabled = false;
        return this;
    }
    public ObjectMappingSettings enableCamelCaseConversion() {
        camelCaseConversionEnabled = true;
        return this;
    }
    public boolean isCamelCaseConversionEnabled() {
        return camelCaseConversionEnabled;
    }
    public ObjectMappingSettings setCamelCaseConversionEnabled(boolean camelCaseConversionEnabled) {
        this.camelCaseConversionEnabled = camelCaseConversionEnabled;
        return this;
    }
    public ObjectMappingSettings disableWarningOnnoEntitySerialization() {
        warnOnNoEntitySerialization = false;
        return this;
    }
    public ObjectMappingSettings enableWarningOnnoEntitySerialization() {
        warnOnNoEntitySerialization = true;
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
