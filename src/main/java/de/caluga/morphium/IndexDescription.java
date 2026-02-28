package de.caluga.morphium;

import de.caluga.morphium.driver.Doc;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Objects;
import org.slf4j.LoggerFactory;

public class IndexDescription {

    private static AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(false);
    private Map<String, Object> key;
    private String name;
    private Boolean background;
    private Boolean unique;
    private Map<String, Object> partialFilterExpression;
    private Boolean sparse;
    private Integer expireAfterSeconds;
    private Boolean hidden;
    private Map<String, Object> storageEngine;
    private Map<String, Object> weights;
    private String defaultLanguage;
    private String languageOverride;
    private Integer textIndexVersion;
    private Integer _2dsphereIndexVersion;
    private Integer bits;
    private Integer min;
    private Integer max;
    private Map<String, Object> collation;
    private Map<String, Object> wildcardProjection;

    public static IndexDescription fromMaps(Map<String, Object> key, Map<String, Object> options) {
        var idx = Doc.of("key", key);
        if (options != null) {
            idx.putAll(options);
        }
        return fromMap(idx);
    }

    public static IndexDescription fromMap(Map<String, Object> incoming) {
        if (incoming.get("name") == null || incoming.get("name").equals("")) {
            StringBuilder sb = new StringBuilder();
            @SuppressWarnings("unchecked")
            Map<String, Object> keymap = (Map<String, Object>) incoming.get("key");
            for (var k : keymap.keySet()) {
                sb.append(k);
                sb.append("_");
                sb.append(keymap.get(k).toString());
                sb.append("_");
            }
            incoming.put("name", sb.toString());
        }
        IndexDescription idx = new IndexDescription();
        for (String n : incoming.keySet()) {
            var fld = an.getField(IndexDescription.class, n);
            if (fld == null) continue;
            try {
                if (fld.getType().equals(Boolean.class) && incoming != null && incoming.get(n) != null && incoming.get(n).getClass().equals(Integer.class)) {
                    incoming.put(n, incoming.get(n).equals(Integer.valueOf(1)));
                }
                fld.set(idx, incoming.get(n));
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        return idx;
    }

    public Map<String, Object> getKey() {
        return key;
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

    public IndexDescription setKey(Map<String, Object> key) {
        this.key = key;
        return this;
    }

    public Map<String, Object> getPartialFilterExpression() {
        return partialFilterExpression;
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

    public IndexDescription setPartialFilterExpression(Map<String, Object> partialFilterExpression) {
        this.partialFilterExpression = partialFilterExpression;
        return this;
    }

    public Map<String, Object> getStorageEngine() {
        return storageEngine;
    }

    public IndexDescription setStorageEngine(Map<String, Object> storageEngine) {
        this.storageEngine = storageEngine;
        return this;
    }

    public Map<String, Object> getWeights() {
        return weights;
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

    public IndexDescription setWeights(Map<String, Object> weights) {
        this.weights = weights;
        return this;
    }

    public Map<String, Object> getCollation() {
        return collation;
    }

    public IndexDescription setCollation(Map<String, Object> collation) {
        this.collation = collation;
        return this;
    }

    public Map<String, Object> getWildcardProjection() {
        return wildcardProjection;
    }

    public IndexDescription setWildcardProjection(Map<String, Object> wildcardProjection) {
        this.wildcardProjection = wildcardProjection;
        return this;
    }

    public Map<String, Object> asMap() {
        Object o;
        Map<String, Object> map = new Doc();

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
                //e.printStackTrace();
                LoggerFactory.getLogger(this.getClass()).error("IllegalAccess",e);
            }
        }

        return map;
    }

    @Override
    public String toString() {
        return Utils.toJsonString(asMap());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexDescription that = (IndexDescription) o;
        boolean ret = Objects.equals(key, that.key)
                && Objects.equals(background, that.background)
                && Objects.equals(unique, that.unique)
                && Objects.equals(partialFilterExpression, that.partialFilterExpression)
                && Objects.equals(sparse, that.sparse) && Objects.equals(expireAfterSeconds, that.expireAfterSeconds)
                && Objects.equals(hidden, that.hidden) && Objects.equals(storageEngine, that.storageEngine)
                && Objects.equals(weights, that.weights) && Objects.equals(defaultLanguage, that.defaultLanguage)
                && Objects.equals(languageOverride, that.languageOverride) && Objects.equals(textIndexVersion, that.textIndexVersion)
                && Objects.equals(_2dsphereIndexVersion, that._2dsphereIndexVersion) && Objects.equals(bits, that.bits)
                && Objects.equals(min, that.min) && Objects.equals(max, that.max) && Objects.equals(collation, that.collation)
                && Objects.equals(wildcardProjection, that.wildcardProjection);
        LoggerFactory.getLogger(IndexDescription.class).info("Comparing {} with {} -> {}", this, that, ret);
        return ret;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, background, unique, partialFilterExpression, sparse, expireAfterSeconds,
                hidden, storageEngine, weights, defaultLanguage, languageOverride, textIndexVersion, _2dsphereIndexVersion,
                bits, min, max, collation, wildcardProjection);
    }
}
