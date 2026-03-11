package de.caluga.morphium.driver.commands.result;

import java.util.List;
import java.util.Map;

public class ListResult extends RunCommandResult<ListResult> {
    private List<Map<String, Object>> result;

    public List<Map<String, Object>> getResult() {
        return result;
    }

    public ListResult setResult(List<Map<String, Object>> result) {
        this.result = result;
        return this;
    }
}
