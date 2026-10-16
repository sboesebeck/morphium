package de.caluga.test.mongo.suite.contract;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.query.Query;

import java.util.List;
import java.util.Map;

/** reads the raw documents of a query without the projection to known fields, so that stray fields show up */
final class ModifyFixturesAccess {
    private ModifyFixturesAccess() {
    }

    static List<Map<String, Object>> rawDocs(Query<?> q) throws Exception {
        FindCommand cmd = q.getFindCmd();

        try {
            cmd.setProjection(new Doc());
            return cmd.execute();
        } finally {
            cmd.releaseConnection();
        }
    }
}
