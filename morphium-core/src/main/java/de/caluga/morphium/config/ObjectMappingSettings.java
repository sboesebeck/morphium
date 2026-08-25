package de.caluga.morphium.config;

import de.caluga.morphium.annotations.Embedded;

@Embedded
public class ObjectMappingSettings extends Settings {

    private boolean checkForNew = true;
    private boolean autoValues = true;
    private boolean objectSerializationEnabled = true;
    private boolean camelCaseConversionEnabled = true;
    private boolean warnOnNoEntitySerialization = false;
    private boolean translateAggregationFieldNames = false;
    // volatile unlike its siblings above: read on every marshall() call via a BooleanSupplier from
    // already-constructed mapper instances (see ObjectMapperImpl's java.time mapper registration),
    // so a runtime setUseBsonDateForJavaTime(...) from another thread has to be visible to them.
    private volatile boolean useBsonDateForJavaTime = false;
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

    /**
     * if enabled, Java property names in aggregation stages are translated to Mongo
     * field names. Covered: group operator $-references and id values, project(Map)
     * and addFields/set keys and values, sort(Map) keys, graphLookup connect fields
     * and startWith. NOT covered (see issue #221): stages taking a raw Expr, i.e.
     * match(Expr), sortByCount, replaceRoot/replaceWith, redact, bucket, facetExpr,
     * unwind(Expr) - use Mongo field names or Expr.field(Enum) there. Default false =
     * legacy behavior (everything passed through verbatim). Overridable per
     * aggregator, see Aggregator.setTranslateAggregationFieldNames.
     */
    public boolean isTranslateAggregationFieldNames() {
        return translateAggregationFieldNames;
    }

    public ObjectMappingSettings setTranslateAggregationFieldNames(boolean translateAggregationFieldNames) {
        this.translateAggregationFieldNames = translateAggregationFieldNames;
        return this;
    }

    public ObjectMappingSettings enableTranslateAggregationFieldNames() {
        translateAggregationFieldNames = true;
        return this;
    }

    public ObjectMappingSettings disableTranslateAggregationFieldNames() {
        translateAggregationFieldNames = false;
        return this;
    }

    /**
     * if enabled, {@code LocalDate}, {@code LocalTime}, {@code LocalDateTime}, and
     * {@code Instant} are marshalled as native BSON Date (type {@code 0x09}) instead of
     * Morphium's legacy per-type formats (epoch-day/nano-of-day longs, or {@code Doc}
     * sub-documents). This is bit-compatible with the official MongoDB Java driver's
     * {@code org.bson.codecs.jsr310} codecs. Values are stored to millisecond
     * precision (sub-millisecond precision is lost, same trade-off the official driver makes
     * for these types). {@code LocalDate}/{@code LocalTime} are anchored to
     * {@link java.time.ZoneOffset#UTC} (date-only values at start-of-day, time-only values on
     * epoch day 0) -- same convention the official driver's codecs use.
     * <p>
     * Default {@code false} = legacy behavior (unchanged for existing data/tests). Read live
     * on every marshall call (not cached at mapper-construction time), so this may be toggled
     * at runtime and takes effect immediately, including for already-constructed
     * {@code ObjectMapperImpl} instances.
     * <p>
     * <b>Scalar fields only.</b> A scalar field is written as a bare BSON Date, so
     * {@code mongosh} shows {@code ISODate} and native date sort/range queries work directly on
     * it. Elements of a {@code List}/array/{@code Map} field are NOT: they keep the
     * {@code {"value": ...}} wrapper that the generic serialization path produces for every
     * custom mapper returning a scalar, with a native Date inside it. Such values round-trip
     * correctly, but a native date query against a container has to address
     * {@code field.value}, and an index has to be declared on that sub-path.
     * <p>
     * Only affects the {@code ObjectMapperImpl}-driven marshalling path: entity persistence
     * ({@code store()}) and the type-safe {@code Query<T>} API, e.g.
     * {@code query.f("field").eq(...)} -- see {@code MongoFieldImpl#checkValue}. NOT covered:
     * <ul>
     *   <li>the update APIs -- {@code set()}, {@code push()}, {@code addToSet()} and friends
     *       route through {@code MorphiumWriterImpl#marshallIfNecessary}, which never consults
     *       the custom mappers, so the value reaches the driver unmapped and keeps the legacy
     *       format at either setting of this flag (pre-existing behaviour, tracked separately);</li>
     *   <li>raw {@code Doc.of("field", someLocalDateTime)} calls that go directly through
     *       {@code de.caluga.morphium.driver.bson.BsonEncoder}; that low-level encoder writes
     *       the legacy format regardless of this setting.</li>
     * </ul>
     */
    public boolean isUseBsonDateForJavaTime() {
        return useBsonDateForJavaTime;
    }

    public ObjectMappingSettings setUseBsonDateForJavaTime(boolean useBsonDateForJavaTime) {
        this.useBsonDateForJavaTime = useBsonDateForJavaTime;
        return this;
    }

    public ObjectMappingSettings enableBsonDateForJavaTime() {
        useBsonDateForJavaTime = true;
        return this;
    }

    public ObjectMappingSettings disableBsonDateForJavaTime() {
        useBsonDateForJavaTime = false;
        return this;
    }
}
