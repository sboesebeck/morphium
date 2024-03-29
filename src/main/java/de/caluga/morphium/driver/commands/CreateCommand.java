package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreateCommand extends MongoCommand<CreateCommand> {
    private Boolean capped;
    private Map<String, String> timeseries;
    private Integer expireAfterSeconds;
    private Integer size;
    private Integer max;
    private Map<String, Object> storageEngine;
    private Map<String, Object> validator;
    private String validationLevel;
    private String validationAction;
    private Map<String, Object> indexOptionDefaults;
    private String viewOn;
    private List<Map<String, Object>> pipeline;
    private Map<String, Object> collation;
    private Map<String, Object> writeConcern;

    public CreateCommand(MongoConnection d) {
        super(d);
    }

    @Override
    public String getCommandName() {
        return "create";
    }

    public Boolean getCapped() {
        return capped;
    }

    public CreateCommand setCapped(Boolean capped) {
        this.capped = capped;
        return this;
    }

    public Map<String, String> getTimeseries() {
        return timeseries;
    }

    public CreateCommand setTimeseries(Map<String, String> timeseries) {
        this.timeseries = timeseries;
        return this;
    }

    public CreateCommand setTimeseriesTimeField(String field) {
        if (timeseries == null) {
            timeseries = new HashMap<>();
        }
        timeseries.put("timeField", field);
        return this;
    }

    public CreateCommand setTimeseriesMetaField(String field) {
        if (timeseries == null) {
            timeseries = new HashMap<>();
        }
        timeseries.put("metaField", field);
        return this;
    }

    public CreateCommand setTimeseriesGranularity(Granularity g) {
        if (timeseries == null) {
            timeseries = new HashMap<>();
        }
        timeseries.put("granularity", g.name());
        return this;
    }

    public Integer getExpireAfterSeconds() {
        return expireAfterSeconds;
    }

    public CreateCommand setExpireAfterSeconds(Integer expireAfterSeconds) {
        this.expireAfterSeconds = expireAfterSeconds;
        return this;
    }

    public Integer getSize() {
        return size;
    }

    public CreateCommand setSize(Integer size) {
        this.size = size;
        return this;
    }

    public Integer getMax() {
        return max;
    }

    public CreateCommand setMax(Integer max) {
        this.max = max;
        return this;
    }

    public Map<String, Object> getStorageEngine() {
        return storageEngine;
    }

    public CreateCommand setStorageEngine(Map<String, Object> storageEngine) {
        this.storageEngine = storageEngine;
        return this;
    }

    public Map<String, Object> getValidator() {
        return validator;
    }

    public CreateCommand setValidator(Map<String, Object> validator) {
        this.validator = validator;
        return this;
    }

    public String getValidationLevel() {
        return validationLevel;
    }

    public CreateCommand setValidationLevel(String validationLevel) {
        this.validationLevel = validationLevel;
        return this;
    }

    public String getValidationAction() {
        return validationAction;
    }

    public CreateCommand setValidationAction(String validationAction) {
        this.validationAction = validationAction;
        return this;
    }

    public Map<String, Object> getIndexOptionDefaults() {
        return indexOptionDefaults;
    }

    public CreateCommand setIndexOptionDefaults(Map<String, Object> indexOptionDefaults) {
        this.indexOptionDefaults = indexOptionDefaults;
        return this;
    }

    public String getViewOn() {
        return viewOn;
    }

    public CreateCommand setViewOn(String viewOn) {
        this.viewOn = viewOn;
        return this;
    }

    public List<Map<String, Object>> getPipeline() {
        return pipeline;
    }

    public CreateCommand setPipeline(List<Map<String, Object>> pipeline) {
        this.pipeline = pipeline;
        return this;
    }

    public Map<String, Object> getCollation() {
        return collation;
    }

    public CreateCommand setCollation(Map<String, Object> collation) {
        this.collation = collation;
        return this;
    }

    public Map<String, Object> getWriteConcern() {
        return writeConcern;
    }

    public CreateCommand setWriteConcern(Map<String, Object> writeConcern) {
        this.writeConcern = writeConcern;
        return this;
    }

    public Map<String, Object> execute() throws MorphiumDriverException {
        MongoConnection connection = getConnection();

        setMetaData("server", connection.getConnectedTo());
        long start = System.currentTimeMillis();
        var msg = connection.sendCommand(this);
        var crs = connection.getAnswerFor(msg, connection.getDriver().getDefaultBatchSize());
        long dur = System.currentTimeMillis() - start;
        setMetaData("duration", dur);
        return crs.next();
    }

    public enum Granularity {
        seconds, minutes, hours
    }
}
