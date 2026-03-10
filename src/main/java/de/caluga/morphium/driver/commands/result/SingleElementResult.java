package de.caluga.morphium.driver.commands.result;

import java.util.Map;

public class SingleElementResult extends RunCommandResult<SingleElementResult> {
    private Map<String, Object> result;

    public Map<String, Object> getResult() {
        return result;
    }

    public SingleElementResult setResult(Map<String, Object> result) {
        this.result = result;
        return this;
    }

}
