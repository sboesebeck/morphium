package de.caluga.morphium;

import java.util.Map;
import java.util.function.Predicate;

public class ThrowOnError {

    private static final String MODIFIED = "nModified";
    public static final Predicate<Map> EXPECTATION_AT_LEAST_ONE_ENTITY_MODIFIED = (map) -> 1 <= ((Integer) map.get(MODIFIED)).intValue();
    private static final Predicate<Map> EXPECTATION_EXACTLY_ONE_ENTITY_MODIFIED = (map) -> 1 == (((Integer) map.get(MODIFIED)).intValue());

    public static Map throwOnWriteError(Map mongoResponse) {
        if (mongoResponse.containsKey("writeErrors")) {
            throw new IllegalStateException("Mongo write error: " + mongoResponse.get("writeErrors"));
        }
        return mongoResponse;
    }

    public static Map throwOnErrorOrExpectationMismatch(Map mongoResponse, Predicate<Map> expectation) {
        if (mongoResponse.containsKey("writeErrors")) {
            throw new IllegalStateException("Mongo write error: " + mongoResponse.get("writeErrors"));
        }
        if (!expectation.test(mongoResponse)) {
            throw new IllegalStateException("Mongo write error: " + mongoResponse.get(MODIFIED));
        }
        return mongoResponse;
    }

    public static Map throwOnErrorOrNotExactlyOneEntityModified(Map mongoResponse) {

        return throwOnErrorOrExpectationMismatch(mongoResponse, EXPECTATION_EXACTLY_ONE_ENTITY_MODIFIED);
    }

}
