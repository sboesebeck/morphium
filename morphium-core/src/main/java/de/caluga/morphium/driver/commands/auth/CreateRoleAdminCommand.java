package de.caluga.morphium.driver.commands.auth;

import de.caluga.morphium.driver.commands.AdminMongoCommand;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.List;

public class CreateRoleAdminCommand extends AdminMongoCommand<CreateRoleAdminCommand> {
    private String roleName;

    public CreateRoleAdminCommand(MongoConnection d) {
        super(d);
    }

    @Override
    public String getCommandName() {
        return "createRole";
    }

    public static class Privilege {
        private List<String> actions;
        private Resource resource;

        public List<String> getActions() {
            return actions;
        }

        public Privilege setActions(List<String> actions) {
            this.actions = actions;
            return this;
        }
    }

    public static class Resource {
        private String db;
        private String collection;
        private Boolean cluster;

        public String getDb() {
            return db;
        }

        public Resource setDb(String db) {
            this.db = db;
            return this;
        }

        public String getCollection() {
            return collection;
        }

        public Resource setCollection(String collection) {
            this.collection = collection;
            return this;
        }

        public Boolean getCluster() {
            return cluster;
        }

        public Resource setCluster(Boolean cluster) {
            this.cluster = cluster;
            return this;
        }
    }
}
