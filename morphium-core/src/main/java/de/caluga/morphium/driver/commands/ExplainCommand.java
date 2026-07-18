package de.caluga.morphium.driver.commands;

import java.util.List;
import java.util.Map;

import de.caluga.morphium.driver.wire.MongoConnection;

public class ExplainCommand extends MongoCommand<ExplainCommand> {
    // db.runCommand(
    // {
    // explain: { count: "products", query: { quantity: { $gt: 50 } } },
    // verbosity: "queryPlanner"
    // }
    // )

    private Map<String,Object> command;
    private ExplainVerbosity verbosity;

    public ExplainCommand(MongoConnection d) {
        super(d);
    }

    @Override
    public String getCommandName() {
        return "explain";
    }

    @Override
    public Map<String, Object> asMap() {
        var m= super.asMap();
        m.put(getCommandName(),m.get("command"));
        m.remove("command");
        return m;
    }


    @SuppressWarnings("unchecked")
    @Override
    public ExplainCommand fromMap(Map<String, Object> m) {
        super.fromMap(m);
        Map<String, Object> inner = (Map<String, Object>) m.get(getCommandName());
        setCommand(inner);

        // MongoCommand.fromMap()'s generic first line (setColl("" + m.get(getCommandName())))
        // stamps `coll` with the *wrapped* command map's toString() - this command's own map key
        // ("explain") holds the wrapped command, not a plain collection name, unlike every other
        // MongoCommand. Recover the real target collection from the wrapped command's own key
        // instead. Only relevant when this command was rebuilt from a raw wire map (e.g. PoppyDB's
        // GenericCommand dispatch) - the Java-side FindCommand#explain/CountMongoCommand#explain
        // helpers set `coll` explicitly via setColl() and never call fromMap() at all.
        if (inner != null) {
            for (String wrappedCommandKey : List.of("find", "count", "aggregate")) {
                Object collValue = inner.get(wrappedCommandKey);
                if (collValue != null) {
                    setColl(String.valueOf(collValue));
                    break;
                }
            }
        }

        return this;
    }

    public Map<String, Object> getCommand() {
        return command;
    }

    public void setCommand(Map<String, Object> command) {
        this.command = command;
    }

    public ExplainVerbosity getVerbosity() {
        return verbosity;
    }

    public void setVerbosity(ExplainVerbosity verbosity) {
        this.verbosity = verbosity;
    }

    public enum ExplainVerbosity {
        allPlansExecution,executionStats,queryPlanner
    }

}
