package de.caluga.morphium.driver.commands;

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
