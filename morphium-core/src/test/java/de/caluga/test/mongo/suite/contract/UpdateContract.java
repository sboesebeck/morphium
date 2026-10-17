package de.caluga.test.mongo.suite.contract;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.DropMongoCommand;
import de.caluga.morphium.driver.commands.FindAndModifyMongoCommand;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The update contract (MOR-028): a table of raw update statements with the result that a real MongoDB produces, run through the {@code update}
 * command on every driver. A case either expects the resulting documents or an error code. Where the InMemoryDriver is known to deviate, the case
 * carries the pinned InMemoryDriver behaviour together with the ledger task that removes the deviation ({@link Case#inMem}); when the task is done the
 * pin fails, and the case is simplified. {@code UpdateContractLedgerTest} checks that every referenced task exists and is still open.
 */
final class UpdateContract {
    private UpdateContract() {
    }

    /**
     * the expected result of a case: the documents of the collection afterwards (ordered by _id), or an error code. An error may name the documents that
     * the collection holds afterwards (a failing statement changes nothing, a failing multi update keeps the documents it changed before the error);
     * without them only the code is compared.
     */
    static final class Result {
        final List<Map<String, Object>> docs;
        final Integer errorCode;

        private Result(List<Map<String, Object>> docs, Integer errorCode) {
            this.docs = docs;
            this.errorCode = errorCode;
        }

        @SafeVarargs
        static Result docs(Map<String, Object>... docs) {
            return new Result(List.of(docs), null);
        }

        static Result error(int code) {
            return new Result(null, code);
        }

        /** the statement fails with the code and leaves the collection with these documents */
        @SafeVarargs
        static Result error(int code, Map<String, Object>... docsAfter) {
            return new Result(List.of(docsAfter), code);
        }

        /** the statement fails, but not with a database error: the driver throws an exception that is not a MorphiumDriverException (only for pinned InMemoryDriver deviations) */
        static Result notADatabaseError() {
            return new Result(null, -1);
        }

        /** the statement fails, with whatever code */
        static Result anyError() {
            return new Result(null, 0);
        }

        @Override
        public String toString() {
            return errorCode != null ? "error " + errorCode + (docs != null ? " with " + canon(docs) : "") : canon(docs);
        }
    }

    static final class Case {
        final String name;
        List<Map<String, Object>> seed = List.of();
        Map<String, Object> filter = Doc.of();
        Map<String, Object> update;
        boolean upsert;
        boolean multi;
        List<Map<String, Object>> arrayFilters;
        Result expected;
        String divergenceTask;
        Result inMemPinned;
        Path pinnedPath; // null: every path

        Case(String name) {
            this.name = name;
        }

        @SafeVarargs
        final Case seed(Map<String, Object>... docs) {
            this.seed = List.of(docs);
            return this;
        }

        Case filter(Map<String, Object> filter) {
            this.filter = filter;
            return this;
        }

        Case update(Map<String, Object> update) {
            this.update = update;
            return this;
        }

        Case upsert() {
            this.upsert = true;
            return this;
        }

        Case multi() {
            this.multi = true;
            return this;
        }

        @SafeVarargs
        final Case arrayFilters(Map<String, Object>... filters) {
            this.arrayFilters = List.of(filters);
            return this;
        }

        Case expect(Result r) {
            this.expected = r;
            return this;
        }

        /** the statement fails and leaves the seeded documents as they are; call after {@link #seed} and {@link #expect} */
        @SuppressWarnings("unchecked")
        Case unchanged() {
            this.expected = Result.error(expected.errorCode, seed.toArray(new Map[0]));
            return this;
        }

        /** the InMemoryDriver deviates from {@link #expect(Result)} until {@code task} is done; this is what it does today */
        Case inMem(String task, Result pinned) {
            this.divergenceTask = task;
            this.inMemPinned = pinned;
            return this;
        }

        /** as {@link #inMem(String, Result)}, but only for the findAndModify path; the update command already behaves like a real MongoDB */
        Case inMemFindAndModify(String task, Result pinned) {
            inMem(task, pinned);
            this.pinnedPath = Path.FIND_AND_MODIFY;
            return this;
        }
    }

    static Case c(String name) {
        return new Case(name);
    }

    /** an ordered document from alternating keys and values */
    static Map<String, Object> d(Object... kv) {
        Doc doc = new Doc();

        for (int i = 0; i < kv.length; i += 2) {
            doc.put((String) kv[i], kv[i + 1]);
        }

        return doc;
    }

    static List<Object> l(Object... values) {
        return new ArrayList<>(java.util.Arrays.asList(values));
    }

    static boolean isInMem(Morphium m) {
        return m.getDriver() instanceof InMemoryDriver;
    }

    private static final String COLL = "contract_docs";

    /** a deep copy of documents and lists: every run gets its own, so that a driver that keeps references into the statement cannot influence the next run */
    @SuppressWarnings("unchecked")
    static <T> T copy(T value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> ret = new Doc();
            map.forEach((k, v) -> ret.put((String) k, copy(v)));
            return (T) ret;
        }

        if (value instanceof List<?> list) {
            List<Object> ret = new ArrayList<>();
            list.forEach(v -> ret.add(copy(v)));
            return (T) ret;
        }

        return value;
    }

    /** how a case is sent: as an {@code update} command, or (single document cases only) as {@code findAndModify} */
    enum Path {
        UPDATE, FIND_AND_MODIFY
    }

    /** runs the case as an update command on the driver of {@code m}; returns null if it behaves as the contract says, else a description of the difference */
    static String run(Morphium m, Case c) throws Exception {
        return run(m, c, Path.UPDATE);
    }

    /** runs the case on the driver of {@code m} the given way; returns null if it behaves as the contract says, else a description of the difference */
    static String run(Morphium m, Case c, Path path) throws Exception {
        String db = m.getConfig().connectionSettings().getDatabase();
        MongoConnection con = m.getDriver().getPrimaryConnection(null);

        try {
            try {
                new DropMongoCommand(con).setDb(db).setColl(COLL).execute();
            } catch (MorphiumDriverException e) {
                // ns not found: nothing to drop
            }

            if (!c.seed.isEmpty()) {
                new InsertMongoCommand(con).setDb(db).setColl(COLL).setDocuments(copy(new ArrayList<>(c.seed))).execute();
            }

            Result actual;

            try {
                if (path == Path.FIND_AND_MODIFY) {
                    FindAndModifyMongoCommand cmd = new FindAndModifyMongoCommand(con).setDb(db).setColl(COLL).setQuery(copy(c.filter)).setUpdate(copy(c.update)).setUpsert(c.upsert).setNewFlag(true);
                    cmd.setArrayFilters(copy(c.arrayFilters));
                    cmd.executeReply();
                    actual = null;
                } else {
                    UpdateMongoCommand cmd = new UpdateMongoCommand(con).setDb(db).setColl(COLL);
                    cmd.addUpdate(copy(c.filter), copy(c.update), null, c.upsert, c.multi, null, copy(c.arrayFilters), null);
                    Map<String, Object> reply = cmd.execute();
                    Integer code = errorCode(reply);
                    actual = code != null ? Result.error(code) : null;
                }
            } catch (MorphiumDriverException e) {
                actual = Result.error(errorCode(e));
            } catch (RuntimeException e) {
                actual = Result.error(-1); // not a database error: an exception of the driver itself
            }

            FindCommand find = new FindCommand(con).setDb(db).setColl(COLL).setFilter(Doc.of()).setSort(Doc.of("_id", 1)).setProjection(new Doc());
            @SuppressWarnings("unchecked")
            Map<String, Object>[] after = find.execute().toArray(new Map[0]);
            actual = actual == null ? Result.docs(after) : Result.error(actual.errorCode, after);

            return compare(m, c, actual, path);
        } finally {
            m.getDriver().releaseConnection(con);
        }
    }

    private static String compare(Morphium m, Case c, Result actual, Path path) {
        boolean pinned = c.inMemPinned != null && isInMem(m) && (c.pinnedPath == null || c.pinnedPath == path);
        Result want = pinned ? c.inMemPinned : c.expected;

        if (matches(want, actual)) {
            return null;
        }

        return c.name + " via " + path + " on " + m.getDriver().getName() + (pinned ? " (pinned deviation " + c.divergenceTask + ")" : "") + ": expected " + want + " but was " + actual;
    }

    private static boolean matches(Result want, Result actual) {
        if (want.errorCode != null || actual.errorCode != null) {
            boolean sameError = want.errorCode != null && actual.errorCode != null && (want.errorCode.equals(actual.errorCode) || want.errorCode == 0);
            return sameError && (want.docs == null || canon(want.docs).equals(canon(actual.docs)));
        }

        return canon(want.docs).equals(canon(actual.docs));
    }

    @SuppressWarnings("unchecked")
    private static Integer errorCode(Map<String, Object> reply) {
        if (reply.get("writeErrors") instanceof List<?> errors && !errors.isEmpty()) {
            Object code = ((Map<String, Object>) errors.get(0)).get("code");
            return code instanceof Number n ? n.intValue() : -1;
        }

        if (reply.get("ok") instanceof Number ok && ok.doubleValue() != 1.0) {
            return reply.get("code") instanceof Number n ? n.intValue() : -1;
        }

        return null;
    }

    private static Integer errorCode(MorphiumDriverException e) {
        if (e.getMongoCode() instanceof Number n) {
            return n.intValue();
        }

        if (e.getWriteErrors() != null && !e.getWriteErrors().isEmpty() && e.getWriteErrors().get(0).get("code") instanceof Number n) {
            return n.intValue();
        }

        return -1;
    }

    /** a canonical text of a value that keeps the number type (1, 1L and 1.0 differ), ignores the key order of documents and shows lists in order */
    static String canon(Object o) {
        if (o == null) {
            return "null";
        }

        if (o instanceof Map<?, ?> map) {
            StringBuilder b = new StringBuilder("{");
            new TreeMap<String, Object>(toStringKeys(map)).forEach((k, v) -> b.append(k).append(':').append(canon(v)).append(','));
            return b.append('}').toString();
        }

        if (o instanceof List<?> list) {
            StringBuilder b = new StringBuilder("[");
            list.forEach(v -> b.append(canon(v)).append(','));
            return b.append(']').toString();
        }

        if (o instanceof Object[] arr) {
            return canon(List.of(arr));
        }

        if (o instanceof Integer) {
            return o + "i";
        }

        if (o instanceof Long) {
            return o + "L";
        }

        if (o instanceof Double) {
            return o + "d";
        }

        if (o instanceof Date date) {
            return "date(" + date.getTime() + ")";
        }

        if (o instanceof String) {
            return "'" + o + "'";
        }

        return o.getClass().getSimpleName() + "(" + o + ")";
    }

    private static Map<String, Object> toStringKeys(Map<?, ?> map) {
        Map<String, Object> ret = new TreeMap<>();
        map.forEach((k, v) -> ret.put(String.valueOf(k), v));
        return ret;
    }
}
