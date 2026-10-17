package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.WriteConcernNotSatisfiedException;

import java.util.List;
import java.util.Map;

/**
 * Turns the reply of a write command ({@code update}, {@code delete}) into the exception of a rejected write. The server reports a statement that it rejects (duplicate key,
 * a path below a scalar, {@code $pull} on a non-array, ...) inside the reply ({@code writeErrors}) and does not fail the command; a write that is applied but
 * does not satisfy the write concern is reported as {@code writeConcernError}.
 * <ul>
 * <li>{@code writeErrors}: a {@link MorphiumDriverException} whose {@code mongoCode} is the code of the first error ({@code isDuplicateKey()} works), with all write
 * errors, the database and the collection. The message names the counts of the reply: the documents updated before the error stay updated (an ordered
 * multi update stops at the first error and is not rolled back);</li>
 * <li>{@code writeConcernError}: a {@link WriteConcernNotSatisfiedException}; the message says that the write was applied;</li>
 * <li>both: the rejection wins.</li>
 * </ul>
 * The exception is returned, not thrown, so that the caller can clear caches and fire events for what was applied before it throws.
 */
public final class WriteReplyErrors {
    private WriteReplyErrors() {
    }

    /** @return the exception for the reply, or null if the reply reports no error (also for no reply at all, as with {@code w:0}) */
    @SuppressWarnings("unchecked")
    public static MorphiumDriverException rejectionOf(String db, String collection, Map<String, Object> reply) {
        if (reply == null) {
            return null;
        }

        long n = reply.get("n") instanceof Number num ? num.longValue() : 0;
        long modified = reply.get("nModified") instanceof Number num ? num.longValue() : 0;

        if (reply.get("writeErrors") instanceof List<?> list && !list.isEmpty()) {
            List<Map<String, Object>> errors = (List<Map<String, Object>>) list;
            Object code = errors.get(0).get("code");
            Object errmsg = errors.get(0).get("errmsg");
            MorphiumDriverException e = new MorphiumDriverException("Write to " + collection + " failed: " + errmsg + " (code " + code + ")"
                    + (errors.size() > 1 ? " (+" + (errors.size() - 1) + " more, " + errors.size() + " errors)" : "") + "; the reply reported matched=" + n + ", modified=" + modified
                    + " before the error, documents updated before the error are not rolled back");
            e.setMongoCode(code);
            e.setWriteErrors(errors);
            e.setCollection(collection);
            e.setDb(db);
            return e;
        }

        if (reply.get("writeConcernError") instanceof Map<?, ?> wce) {
            WriteConcernNotSatisfiedException e = new WriteConcernNotSatisfiedException("The write to " + collection + " WAS APPLIED (matched=" + n + ", modified=" + modified
                    + ") but the write concern was not satisfied: " + wce.get("errmsg") + " (code " + wce.get("code") + ")");
            e.setMongoCode(wce.get("code"));
            e.setCollection(collection);
            e.setDb(db);
            return e;
        }

        return null;
    }
}
