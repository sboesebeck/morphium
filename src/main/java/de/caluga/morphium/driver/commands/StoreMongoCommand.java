package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.MongoConnection;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class StoreMongoCommand extends WriteMongoCommand<StoreMongoCommand> {
    private List<Map<String, Object>> docs;

    public StoreMongoCommand(MongoConnection d) {
        super(d);
    }

    public List<Map<String, Object>> getDocs() {
        return docs;
    }

    public StoreMongoCommand setDocuments(List<Map<String, Object>> docs) {
        this.docs = docs;
        return this;
    }

    @Override
    public Map<String, Object> execute() throws MorphiumDriverException {
        UpdateMongoCommand updateSettings = getUpdateMongoCommand();

        Map<String, Object> result = updateSettings.execute();
        return result;
    }

    @Override
    public int executeAsync() throws MorphiumDriverException {
        UpdateMongoCommand updateSettings = getUpdateMongoCommand();
        return updateSettings.executeAsync();
    }


    private UpdateMongoCommand getUpdateMongoCommand() {
        List<Map<String, Object>> opsLst = new ArrayList<>();
        for (Map<String, Object> o : getDocs()) {
            o.putIfAbsent("_id", new ObjectId());
            Map<String, Object> up = new LinkedHashMap<>();
            up.put("q", Doc.of("_id", o.get("_id")));
            up.put("u", Doc.of("$set", o));
            up.put("upsert", true);
            up.put("multi", false);
            up.put("collation", null);
            //up.put("arrayFilters",list of arrayfilters)
            //up.put("hint",indexInfo);
            //up.put("c",variablesDocument);
            opsLst.add(up);
        }
        UpdateMongoCommand updateSettings = new UpdateMongoCommand(getConnection()).setDb(getDb()).setColl(getColl())
                .setUpdates(opsLst).setWriteConcern(getWriteConcern());
        setMetaData(updateSettings.getMetaData());
        return updateSettings;
    }

    @Override
    public String getCommandName() {
        return null;
    }
}
