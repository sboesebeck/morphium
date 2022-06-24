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

    @Override
    public List<Map<String, Object>> executeGetResult() throws MorphiumDriverException {
        return getDelCmd().executeGetResult();

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
    public MorphiumCursor execute() throws MorphiumDriverException {

        return getDelCmd().execute();
    }

    @Override
    public int executeGetMsgID() throws MorphiumDriverException {
        return getDelCmd().executeGetMsgID();
    }
}
