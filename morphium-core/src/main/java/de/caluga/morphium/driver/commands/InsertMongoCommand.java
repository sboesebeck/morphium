package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.LoggerFactory;

public class InsertMongoCommand extends WriteMongoCommand<InsertMongoCommand> {
    private List<Map<String, Object>> documents;
    private Boolean ordered;
    private Boolean bypassDocumentValidation;
    private String comment;

    public InsertMongoCommand(MongoConnection d) {
        super(d);
    }

    public List<Map<String, Object>> getDocuments() {
        return documents;
    }

    public InsertMongoCommand setDocuments(List<Map<String, Object>> documents) {
        this.documents = documents;
        return this;
    }

    public Boolean getOrdered() {
        return ordered;
    }

    public InsertMongoCommand setOrdered(Boolean ordered) {
        this.ordered = ordered;
        return this;
    }

    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    public InsertMongoCommand setBypassDocumentValidation(Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    @Override
    public String getComment() {
        return comment;
    }

    @Override
    public InsertMongoCommand setComment(String comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public String getCommandName() {
        return "insert";
    }

    @Override
    protected List<Map<String, Object>> getPayloadStatements() {
        return documents;
    }

    @Override
    protected void setPayloadStatements(List<Map<String, Object>> statements) {
        this.documents = statements;
    }

    @Override
    protected boolean isOrderedWrite() {
        return ordered == null || ordered;
    }

    /**
     * Issue #359: this insert was re-sent because the reply to the first attempt was lost. If
     * that attempt had in fact committed, the re-sent documents now collide with themselves and
     * mongod answers E11000 on the {@code _id} index - a successful write reported as a failure.
     * Morphium assigns {@code _id} on the client, so such an error can be attributed: the
     * failing document's own id is the one the server reports as duplicate. Count it as written.
     *
     * <p>Only the {@code _id} index qualifies. A violation of any other unique index is a
     * genuine conflict with someone else's data and stays an error, as does a collision on a
     * first attempt (there is no earlier attempt of ours that could have caused it).
     *
     * <p>The one case this cannot separate: a document carrying an application-assigned
     * {@code _id} that already existed before, whose first attempt lost its reply. That write
     * really did fail and is now reported as successful. It needs both a pre-existing id and a
     * lost reply, and disappears entirely once #293 brings real {@code (lsid, txnNumber)}
     * deduplication.
     */
    @Override
    @SuppressWarnings("unchecked")
    protected Map<String, Object> reconcileWriteErrorsAfterNetworkRetry(Map<String, Object> result) {
        if (result == null || !(result.get("writeErrors") instanceof List<?> errors) || errors.isEmpty()) {
            return result;
        }

        List<Map<String, Object>> remaining = new ArrayList<>();
        List<Object> reconciledIds = new ArrayList<>();

        for (Object e : errors) {
            Object ownId = e instanceof Map ? ownDuplicateId((Map<String, Object>) e) : null;

            if (ownId != null) {
                reconciledIds.add(ownId);
            } else {
                remaining.add((Map<String, Object>) e);
            }
        }

        if (reconciledIds.isEmpty()) {
            return result;
        }

        LoggerFactory.getLogger(InsertMongoCommand.class).warn(
            "Insert was re-sent after a lost reply and collided with its own _id {} on {}.{} - the "
            + "first attempt had committed, counting {} document(s) as written",
            reconciledIds, getDb(), getColl(), reconciledIds.size());
        Map<String, Object> reconciled = new LinkedHashMap<>(result);
        int n = result.get("n") instanceof Number num ? num.intValue() : 0;
        reconciled.put("n", n + reconciledIds.size());

        if (remaining.isEmpty()) {
            reconciled.remove("writeErrors");
        } else {
            reconciled.put("writeErrors", remaining);
        }

        return reconciled;
    }

    /**
     * The {@code _id} of the document this write error belongs to, if the error is a duplicate
     * key on the {@code _id} index of a document of this very batch - null otherwise.
     */
    private Object ownDuplicateId(Map<String, Object> writeError) {
        if (!(writeError.get("code") instanceof Number code) || code.intValue() != 11000) {
            return null;
        }

        if (documents == null || !(writeError.get("index") instanceof Number idx)) {
            return null;
        }

        int i = idx.intValue();

        if (i < 0 || i >= documents.size()) {
            return null;
        }

        // No client-side id means the server generated it - it cannot collide with our own retry
        Object ourId = documents.get(i).get("_id");

        if (ourId == null || !isIdIndexViolation(writeError)) {
            return null;
        }

        // keyValue is the authoritative statement of what collided (mongod >= 4.4); older
        // servers only send errmsg, and there the index position identifies the document
        Object reported = writeError.get("keyValue") instanceof Map<?, ?> kv ? kv.get("_id") : null;
        return reported == null || ourId.equals(reported) ? ourId : null;
    }

    private boolean isIdIndexViolation(Map<String, Object> writeError) {
        if (writeError.get("keyPattern") instanceof Map<?, ?> keyPattern) {
            return keyPattern.size() == 1 && keyPattern.containsKey("_id");
        }

        return writeError.get("errmsg") instanceof String errmsg && errmsg.contains("index: _id_");
    }


    @Override
    public Map<String, Object> execute() throws MorphiumDriverException {
        if (!getConnection().isConnected()) throw new RuntimeException("Not connected");
        Map<String, Object> writeResult = super.execute();
        if (writeResult == null) {
            throw new MorphiumDriverException("Write failed: no result returned by driver");
        }
        if (writeResult.containsKey("writeErrors")) {
            int failedWrites = ((List<?>) writeResult.get("writeErrors")).size();
            int success = (int) writeResult.get("n");
            StringBuilder msg = new StringBuilder();
            msg.append("Failed to write: " + failedWrites + " - succeeded: " + success);
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> writeErrors = (List<Map<String, Object>>) writeResult.get("writeErrors");
            for (Map<String, Object> err : writeErrors) {
                msg.append("\n----> ");
                msg.append(err.get("code"));
                msg.append(":");
                msg.append(err.get("errmsg"));
            }
            var ex = new MorphiumDriverException(msg.toString());
            ex.setWriteErrors(writeErrors);
            throw ex;
        }
        return writeResult;
    }
}
