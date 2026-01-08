package de.caluga.morphium.driver.commands;

import java.util.HashMap;
import java.util.Map;

import de.caluga.morphium.driver.wire.MongoConnection;

public class ListCollectionsCommand extends ReadMongoCommand<ListCollectionsCommand> {
    private Map<String, Object> filter;
    private Boolean nameOnly = false;
    private Boolean authorizedCollections = false;

    public ListCollectionsCommand(MongoConnection d) {
        super(d);
        setColl("1");
    }

    public Map<String, Object> getFilter() {
        return filter;
    }

    public ListCollectionsCommand setFilter(Map<String, Object> filter) {
        this.filter = filter;
        return this;
    }

    public Boolean getNameOnly() {
        return nameOnly;
    }

    public ListCollectionsCommand setNameOnly(Boolean nameOnly) {
        this.nameOnly = nameOnly;
        return this;
    }

    public Boolean getAuthorizedCollections() {
        return authorizedCollections;
    }

    public ListCollectionsCommand setAuthorizedCollections(Boolean authorizedCollections) {
        this.authorizedCollections = authorizedCollections;
        return this;
    }

    public ListCollectionsCommand addFilter(String k, Object v) {
        if (filter == null) {
            filter = new HashMap<>();
        }

        filter.put(k, v);
        return this;
    }

    @Override
    public ListCollectionsCommand setColl(String coll) {
        //LoggerFactory.getLogger(ListCollectionsCommand.class).warn("Cannot set collection on listCollectionCommand!");
        return this;
    }

    @Override
    public ListCollectionsCommand fromMap(Map<String, Object> m) {
        super.fromMap(m);
        Object v = m.get(getCommandName());

        if (v != null) {
            super.setColl(v.toString());
        }

        return this;
    }

    @Override
    public String getCommandName() {
        return "listCollections";
    }


}
