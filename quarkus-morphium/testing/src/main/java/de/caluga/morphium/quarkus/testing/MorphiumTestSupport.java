/*
 * Copyright 2025 The Quarkiverse Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.caluga.morphium.quarkus.testing;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.DeleteMongoCommand;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.ArrayList;
import java.util.List;

/**
 * Helpers for tests of an application that uses Morphium: empty the database between tests and load
 * entities as fixtures. Works with every driver, the in-memory one and a real MongoDB.
 */
public final class MorphiumTestSupport {

    private MorphiumTestSupport() {
    }

    /**
     * Removes all documents of all collections of the database of {@code morphium} and clears its cache.
     * The collections stay, so their indexes (unique indexes, TTL, capped settings) and the validation rules survive.
     * Collections of the server itself ({@code system.*}) are left alone.
     *
     * @return the number of collections that were emptied
     */
    public static int reset(Morphium morphium) {
        String db = morphium.getConfig().connectionSettings().getDatabase();
        int emptied = 0;
        MongoConnection con = null;

        try {
            // not Morphium.listCollections(): it caches its answer
            List<String> collections = morphium.getDriver().listCollections(db, null);
            con = morphium.getDriver().getPrimaryConnection(null);

            for (String coll : collections) {
                if (coll.startsWith("system.")) {
                    continue;
                }

                DeleteMongoCommand delete = new DeleteMongoCommand(con).setDb(db).setColl(coll)
                        .setDeletes(new ArrayList<>(List.of(Doc.of("q", Doc.of(), "limit", 0))));
                delete.execute();
                emptied++;
            }
        } catch (MorphiumDriverException e) {
            throw new IllegalStateException("Could not reset the database " + db, e);
        } finally {
            if (con != null) {
                morphium.getDriver().releaseConnection(con);
            }
        }

        morphium.getCache().resetCache();
        return emptied;
    }

    /**
     * Stores the entities as they are, with {@code Morphium.store}: ids, versions and lifecycle callbacks behave as in the application.
     *
     * @return the stored entities, in the given order
     */
    @SafeVarargs
    public static <T> List<T> load(Morphium morphium, T... entities) {
        return load(morphium, List.of(entities));
    }

    /** Like {@link #load(Morphium, Object[])} for a list. */
    public static <T> List<T> load(Morphium morphium, List<T> entities) {
        List<T> stored = new ArrayList<>(entities);

        for (T entity : stored) {
            morphium.store(entity);
        }

        return stored;
    }
}
