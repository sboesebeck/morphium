package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.bson.MongoJSScript;
import de.caluga.morphium.driver.commands.ExplainCommand.ExplainVerbosity;
import de.caluga.morphium.driver.wire.MongoConnection;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MapReduceCommand extends ReadMongoCommand<MapReduceCommand> {
    private String map;
    private String reduce;
    private String finalize;
    private String outColl;
    private Doc outConfig;

    private Doc query;
    private Doc sort;
    private Integer limit;
    private Doc scope;
    private Boolean jsMode;
    private Boolean verbose;

    private Boolean bypassDocumentValidation;
    private Doc collation;
    private Doc writeConcern;
    private String comment;

    public MapReduceCommand(MongoConnection d) {
        super(d);
    }


    public String getMap() {
        return map;
    }

    public MapReduceCommand setMap(String map) {
        this.map = map;
        return this;
    }

    public String getReduce() {
        return reduce;
    }

    public MapReduceCommand setReduce(String reduce) {
        this.reduce = reduce;
        return this;
    }

    public String getFinalize() {
        return finalize;
    }

    public MapReduceCommand setFinalize(String finalize) {
        this.finalize = finalize;
        return this;
    }

    public String getOutColl() {
        return outColl;
    }

    public MapReduceCommand setOutColl(String outColl) {
        this.outColl = outColl;
        return this;
    }

    public Doc getOutConfig() {
        return outConfig;
    }

    public MapReduceCommand setOutConfig(Doc outConfig) {
        this.outConfig = outConfig;
        return this;
    }

    public Doc getQuery() {
        return query;
    }

    public MapReduceCommand setQuery(Doc query) {
        this.query = query;
        return this;
    }

    public Doc getSort() {
        return sort;
    }

    public MapReduceCommand setSort(Doc sort) {
        this.sort = sort;
        return this;
    }

    public Integer getLimit() {
        return limit;
    }

    public MapReduceCommand setLimit(Integer limit) {
        this.limit = limit;
        return this;
    }

    public Doc getScope() {
        return scope;
    }

    public MapReduceCommand setScope(Doc scope) {
        this.scope = scope;
        return this;
    }

    public Boolean getJsMode() {
        return jsMode;
    }

    public MapReduceCommand setJsMode(Boolean jsMode) {
        this.jsMode = jsMode;
        return this;
    }

    public Boolean getVerbose() {
        return verbose;
    }

    public MapReduceCommand setVerbose(Boolean verbose) {
        this.verbose = verbose;
        return this;
    }

    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    public MapReduceCommand setBypassDocumentValidation(Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    public Doc getCollation() {
        return collation;
    }

    public MapReduceCommand setCollation(Doc collation) {
        this.collation = collation;
        return this;
    }

    public Doc getWriteConcern() {
        return writeConcern;
    }

    public MapReduceCommand setWriteConcern(Doc writeConcern) {
        this.writeConcern = writeConcern;
        return this;
    }

    @Override
    public String getComment() {
        return comment;
    }

    @Override
    public MapReduceCommand setComment(String comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public String getCommandName() {
        return "mapReduce";
    }

    public Map<String,Object> explain(ExplainVerbosity verbosity) throws MorphiumDriverException{
        ExplainCommand explainCommand = new ExplainCommand(getConnection());
        explainCommand.setVerbosity(verbosity);
        var m=asMap();
        m.remove("$db");
        m.remove("$coll");
        explainCommand.setCommand(m);
        explainCommand.setDb(getDb()).setColl(getColl());
        int msg=explainCommand.executeAsync();
        return explainCommand.getConnection().readSingleAnswer(msg);
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> m = super.asMap();
        if (m.containsKey("outColl") && m.containsKey("outConfig")) {
            LoggerFactory.getLogger(MapReduceCommand.class).error("Cannot specify out coll and out config! Ignoring outConfig");
            m.remove("outConfig");
        }
        String k = "outColl";
        if (m.containsKey("outConfig")) {
            k = "outConfig";
        }
        if (m.containsKey(k))
            m.put("out", m.remove(k));
        if (!m.containsKey("out")) {
            m.put("out", Doc.of("inline", 1));
        }
        return m;
    }

    @Override
    public MapReduceCommand fromMap(Map<String, Object> m) {
        // Handle MongoJSScript objects for map, reduce, finalize before super.fromMap
        // which uses reflection and expects String types
        if (m.containsKey("map")) {
            Object mapVal = m.get("map");
            if (mapVal instanceof MongoJSScript) {
                m.put("map", ((MongoJSScript) mapVal).getJs());
            }
        }
        if (m.containsKey("reduce")) {
            Object reduceVal = m.get("reduce");
            if (reduceVal instanceof MongoJSScript) {
                m.put("reduce", ((MongoJSScript) reduceVal).getJs());
            }
        }
        if (m.containsKey("finalize")) {
            Object finalizeVal = m.get("finalize");
            if (finalizeVal instanceof MongoJSScript) {
                m.put("finalize", ((MongoJSScript) finalizeVal).getJs());
            }
        }

        super.fromMap(m);
        if (m.containsKey("out") && (m.get("out") instanceof String)){
            outColl= (String) m.get("out");
        }
        if (m.containsKey("out")&&(m.get("out") instanceof Map)){
            outConfig= (Doc) m.get("out");
        }
        return this;
    }
}
