package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClearCollectionSettings extends WriteMongoCommand<ClearCollectionSettings> {

    public ClearCollectionSettings(MorphiumDriver d) {
        super(d);
    }

    @Override
    public String getCommandName() {
        return null;
    }


    private DeleteMongoCommand getDelCmd() throws MorphiumDriverException {
        DeleteMongoCommand del = new DeleteMongoCommand(getDriver());
        del.addDelete(Doc.of("q", new HashMap<>(), "limit", 0));
        del.setDb(getDb());
        del.setColl(getColl());
        del.setOrdered(false);
        del.setComment(getComment());
        return del;
    }

    @Override
    public Map<String, Object> execute() throws MorphiumDriverException {
        return getDelCmd().execute();
    }

    public int doClear() throws MorphiumDriverException {
        var ret = getDelCmd().execute();
        return (Integer) ret.get("n");
    }

    @Override
    public int executeAsync() throws MorphiumDriverException {
        return getDelCmd().executeAsync();
    }
}
