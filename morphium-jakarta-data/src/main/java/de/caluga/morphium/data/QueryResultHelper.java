package de.caluga.morphium.data;

import de.caluga.morphium.query.Query;
import jakarta.data.exceptions.EmptyResultException;
import jakarta.data.exceptions.NonUniqueResultException;

import java.util.List;
import java.util.Optional;

/**
 * Shared helper for enforcing Jakarta Data single-result semantics.
 * <p>
 * When a repository method declares a single-entity return type ({@code T}, not
 * {@code List<T>} or {@code Stream<T>}), the spec requires:
 * <ul>
 *   <li>{@link EmptyResultException} if the query returns no results</li>
 *   <li>{@link NonUniqueResultException} if the query returns more than one result</li>
 * </ul>
 * For {@code Optional<T>} return types, no result returns {@code Optional.empty()}
 * but multiple results still throw {@code NonUniqueResultException}.
 * <p>
 * Used by {@link QueryExecutor}, {@link FindMethodBridge}, and {@link JdqlMethodBridge} as the
 * final step before returning a single-entity or {@code Optional} result to the caller — the
 * query itself (conditions, sorting, etc.) has already been fully built by that point.
 */
final class QueryResultHelper {

    private QueryResultHelper() {}

    /**
     * Executes the query expecting exactly one result.
     *
     * @param query the Morphium query to execute
     * @return the single result entity (never null)
     * @throws EmptyResultException     if no result is found
     * @throws NonUniqueResultException if more than one result is found
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static Object requireSingle(Query query) {
        List results = query.limit(2).asList();
        if (results.isEmpty()) {
            throw new EmptyResultException("Query returned no result");
        }
        if (results.size() > 1) {
            throw new NonUniqueResultException("Query returned more than one result");
        }
        return results.get(0);
    }

    /**
     * Executes the query expecting zero or one result, returning an Optional.
     *
     * @param query the Morphium query to execute
     * @return {@code Optional.of(entity)} or {@code Optional.empty()}
     * @throws NonUniqueResultException if more than one result is found
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static Optional optionalSingle(Query query) {
        List results = query.limit(2).asList();
        if (results.isEmpty()) {
            return Optional.empty();
        }
        if (results.size() > 1) {
            throw new NonUniqueResultException("Query returned more than one result");
        }
        return Optional.of(results.get(0));
    }
}
