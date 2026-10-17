package de.caluga.morphium.driver;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One or more requests of a bulk write were rejected by the server (MOR-059). The requests that were executed before (ordered) or besides (unordered) the
 * rejected ones ARE applied and not rolled back: {@link #getPartialResult()} carries the counts ({@code num_matched}, {@code num_modified}, {@code num_inserted},
 * {@code num_deleted}, {@code num_upserts}) of what was applied. {@link #getMongoCode()} and {@link #getWriteErrors()} are those of the first rejection (all write errors are
 * collected), {@link #getFailures()} has one exception per rejected request.
 */
public class BulkWriteException extends MorphiumDriverException {
    private final transient Map<String, Object> partialResult;
    private final transient List<MorphiumDriverException> failures;

    public BulkWriteException(String message, Map<String, Object> partialResult, List<MorphiumDriverException> failures) {
        super(message, failures.isEmpty() ? null : failures.get(0));
        this.partialResult = partialResult == null ? Map.of() : new LinkedHashMap<>(partialResult);
        this.failures = List.copyOf(failures);

        if (!failures.isEmpty()) {
            setMongoCode(failures.get(0).getMongoCode());
            setCollection(failures.get(0).getCollection());
            setDb(failures.get(0).getDb());
            List<Map<String, Object>> all = new ArrayList<>();

            for (MorphiumDriverException f : failures) {
                if (f.getWriteErrors() != null) {
                    all.addAll(f.getWriteErrors());
                }
            }

            if (!all.isEmpty()) {
                setWriteErrors(all);
            }
        }
    }

    /** the counts of what was applied */
    public Map<String, Object> getPartialResult() {
        return partialResult;
    }

    /** one exception per rejected request, in the order of the requests */
    public List<MorphiumDriverException> getFailures() {
        return failures;
    }
}
