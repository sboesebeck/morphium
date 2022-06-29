package de.caluga.morphium.driver.commands;

import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ListIndexesCommand extends MongoCommand<ListIndexesCommand> {

    public ListIndexesCommand(MorphiumDriver d) {
        super(d);
    }

    @Override
    public String getCommandName() {
        return "listIndexes";
    }

    public List<IndexDescription> execute() throws MorphiumDriverException {
        var crs = getDriver().runCommand(getDb(), asMap());

        List<IndexDescription> lst = new ArrayList<>();
        while (crs.hasNext()) {
            Map<String, Object> next = crs.next();
            if (next.get("ok") != null && next.get("ok").equals(0.0)) continue;
            lst.add(IndexDescription.fromMap(next));
        }
        return lst;
    }
}
