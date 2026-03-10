package de.caluga.morphium.driver.commands;

import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CreateIndexesCommand extends WriteMongoCommand<CreateIndexesCommand> {
    private List<Map<String, Object>> indexes;
    private String commitQuorum;

    public CreateIndexesCommand(MongoConnection d) {
        super(d);
    }

    @Override
    public String getCommandName() {
        return "createIndexes";
    }

    public List<Map<String, Object>> getIndexes() {
        if (indexes == null) {
            indexes = new ArrayList<>();
        }
        return indexes;
    }

    public CreateIndexesCommand setIndexes(List<Map<String, Object>> indexes) {
        this.indexes = indexes;
        return this;
    }

    public CreateIndexesCommand addIndex(Map<String, Object> index, Map<String, Object> options) {
        Doc idx = Doc.of("key", index);
        if (options != null)
            idx.putAll(options);
        if (!idx.containsKey("name")) {
            StringBuilder b = new StringBuilder();
            for (var s : index.entrySet()) {
                b.append(s.getKey());
                b.append("_" + s.getValue());
            }
            idx.put("name", b.toString());
        }
        getIndexes().add(idx);

        return this;
    }


    public String getCommitQuorum() {
        return commitQuorum;
    }

    public CreateIndexesCommand setCommitQuorum(String commitQuorum) {
        this.commitQuorum = commitQuorum;
        return this;
    }


    public CreateIndexesCommand addIndex(IndexDescription idx) {
        if (indexes == null) indexes = new ArrayList<>();
        indexes.add(idx.asMap());
        return this;
    }

}
