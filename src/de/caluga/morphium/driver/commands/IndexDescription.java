package de.caluga.morphium.driver.commands;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public class IndexDescription {

    private static AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(false);
    private Doc key;
    private String name;
    private Boolean background;
    private Boolean unique;
    private Doc partialFilterExpression;
    private Boolean sparse;
    private Integer expireAfterSeconds;
    private Boolean hidden;
    private Doc storageEngine;
    private Doc weights;
    private String defaultLanguage;
    private String languageOverride;
    private Integer textIndexVersion;
    private Integer _2dsphereIndexVersion;
    private Integer bits;
    private Integer min;
    private Integer max;
    private Doc collation;
    private Doc wildcardProjection;

    public Doc getKey() {
        return key;
    }

    public IndexDescription setKey(Doc key) {
        this.key = key;
        return this;
    }

    public String getName() {
        return name;
    }

    public IndexDescription setName(String name) {
        this.name = name;
        return this;
    }

    public Boolean getBackground() {
        return background;
    }

    public IndexDescription setBackground(Boolean background) {
        this.background = background;
        return this;
    }

    public Boolean getUnique() {
        return unique;
    }

    public IndexDescription setUnique(Boolean unique) {
        this.unique = unique;
        return this;
    }

    public Doc getPartialFilterExpression() {
        return partialFilterExpression;
    }

    public IndexDescription setPartialFilterExpression(Doc partialFilterExpression) {
        this.partialFilterExpression = partialFilterExpression;
        return this;
    }

    public Boolean getSparse() {
        return sparse;
    }

    public IndexDescription setSparse(Boolean sparse) {
        this.sparse = sparse;
        return this;
    }

    public Integer getExpireAfterSeconds() {
        return expireAfterSeconds;
    }

    public IndexDescription setExpireAfterSeconds(Integer expireAfterSeconds) {
        this.expireAfterSeconds = expireAfterSeconds;
        return this;
    }

    public Boolean getHidden() {
        return hidden;
    }

    public IndexDescription setHidden(Boolean hidden) {
        this.hidden = hidden;
        return this;
    }

    public Doc getStorageEngine() {
        return storageEngine;
    }

    public IndexDescription setStorageEngine(Doc storageEngine) {
        this.storageEngine = storageEngine;
        return this;
    }

    public Doc getWeights() {
        return weights;
    }

    public IndexDescription setWeights(Doc weights) {
        this.weights = weights;
        return this;
    }

    public String getDefaultLanguage() {
        return defaultLanguage;
    }

    public IndexDescription setDefaultLanguage(String defaultLanguage) {
        this.defaultLanguage = defaultLanguage;
        return this;
    }

    public String getLanguageOverride() {
        return languageOverride;
    }

    public IndexDescription setLanguageOverride(String languageOverride) {
        this.languageOverride = languageOverride;
        return this;
    }

    public Integer getTextIndexVersion() {
        return textIndexVersion;
    }

    public IndexDescription setTextIndexVersion(Integer textIndexVersion) {
        this.textIndexVersion = textIndexVersion;
        return this;
    }

    public Integer get_2dsphereIndexVersion() {
        return _2dsphereIndexVersion;
    }

    public IndexDescription set_2dsphereIndexVersion(Integer _2dsphereIndexVersion) {
        this._2dsphereIndexVersion = _2dsphereIndexVersion;
        return this;
    }

    public Integer getBits() {
        return bits;
    }

    public IndexDescription setBits(Integer bits) {
        this.bits = bits;
        return this;
    }

    public Integer getMin() {
        return min;
    }

    public IndexDescription setMin(Integer min) {
        this.min = min;
        return this;
    }

    public Integer getMax() {
        return max;
    }

    public IndexDescription setMax(Integer max) {
        this.max = max;
        return this;
    }

    public Doc getCollation() {
        return collation;
    }

    public IndexDescription setCollation(Doc collation) {
        this.collation = collation;
        return this;
    }

    public Doc getWildcardProjection() {
        return wildcardProjection;
    }

    public IndexDescription setWildcardProjection(Doc wildcardProjection) {
        this.wildcardProjection = wildcardProjection;
        return this;
    }

    public Doc asMap() {
        Object o;
        Doc map = new Doc();

        for (Field f : an.getAllFields(this.getClass())) {
            if (Modifier.isStatic(f.getModifiers())) {
                continue;
            }

            f.setAccessible(true);

            try {
                Object v = f.get(this);
                if (v != null) {
                    String fn = f.getName();
                    if (fn.startsWith("_")) {
                        fn = fn.substring(1);
                    }
                    map.put(fn, v);
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }

        return map;
    }

}
