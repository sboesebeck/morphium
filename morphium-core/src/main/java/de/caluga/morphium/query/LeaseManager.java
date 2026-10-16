package de.caluga.morphium.query;

import de.caluga.morphium.Morphium;

import java.lang.reflect.Field;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

/**
 * A lease (a time limited, exclusive claim on a name such as a job, a relay or a command) on a collection, built on the update builder: one atomic conditional
 * upsert on the lease document, with the filter {@code _id = leaseId AND (expiresAt < now OR owner = me)}. A free or expired lease is taken, a lease that the
 * caller holds is renewed, a lease held by somebody else is reported as {@link LeaseResult.Status#HELD} - the lost insert of the upsert is a duplicate key on the
 * {@code _id}, which {@link UpdateBuilder#tryFindOneAndUpdate(ReturnDocument)} turns into a value. No code needs to be inspected.
 * <pre>{@code
 * LeaseManager<Lease> leases = LeaseManager.of(morphium, Lease.class, "ownerName", "expiresAt");
 * if (leases.tryAcquire("outbox-relay", nodeId, Duration.ofSeconds(30)).isAcquired()) {
 *     ... work; call tryAcquire again before the deadline to renew
 * }
 * leases.release("outbox-relay", nodeId);
 * }</pre>
 * Notes:
 * <ul>
 * <li>The lease id is the {@code @Id} of the entity, so the uniqueness is the one of {@code _id} (no unique index needed). A lease keyed by another field is
 * not supported.</li>
 * <li>The deadline is compared with the {@code now} of the caller, not with the clock of the server: leases between hosts are only as exact as their clocks.
 * Use a ttl that is large compared to the skew.</li>
 * <li>For an entity with an {@code @Version} field the version grows with every grant and renewal; it is a natural fencing token for the work done under the lease.</li>
 * <li>There is no heartbeat: renewing is calling {@code tryAcquire} again. A lease that is not renewed expires.</li>
 * <li>A duplicate key inside a real MongoDB transaction aborts the transaction on the server; do not call {@code tryAcquire} inside {@code @MorphiumTransactional}.</li>
 * </ul>
 *
 * @param <T> the lease entity
 * @apiNote incubating until 6.3
 */
public final class LeaseManager<T> {
    private static final Instant EXPIRED = Instant.EPOCH;

    private final Morphium morphium;
    private final Class<T> type;
    private final String idField;
    private final String ownerField;
    private final String expiresAtField;
    private final boolean dateExpiry;

    private LeaseManager(Morphium morphium, Class<T> type, String idField, String ownerField, String expiresAtField, boolean dateExpiry) {
        this.morphium = morphium;
        this.type = type;
        this.idField = idField;
        this.ownerField = ownerField;
        this.expiresAtField = expiresAtField;
        this.dateExpiry = dateExpiry;
    }

    /**
     * @param ownerField     Java name of the {@code String} field that holds the owner
     * @param expiresAtField Java name of the {@code Instant} or {@code java.util.Date} field that holds the deadline
     * @throws IllegalArgumentException if a field does not exist or has the wrong type
     */
    public static <T> LeaseManager<T> of(Morphium morphium, Class<T> type, String ownerField, String expiresAtField) {
        if (morphium == null || type == null) {
            throw new IllegalArgumentException("morphium and type are required");
        }

        if (ownerField == null || expiresAtField == null || ownerField.equals(expiresAtField)) {
            throw new IllegalArgumentException("the owner field and the expiry field are required and must differ");
        }

        Field owner = field(morphium, type, ownerField);
        Field expires = field(morphium, type, expiresAtField);

        if (!owner.getType().equals(String.class)) {
            throw new IllegalArgumentException(type.getSimpleName() + "." + ownerField + " must be a String, not " + owner.getType().getSimpleName());
        }

        boolean date = expires.getType().equals(Date.class);

        if (!date && !expires.getType().equals(Instant.class)) {
            throw new IllegalArgumentException(type.getSimpleName() + "." + expiresAtField + " must be an Instant or a java.util.Date, not " + expires.getType().getSimpleName());
        }

        return new LeaseManager<>(morphium, type, morphium.getARHelper().getIdFieldName(type), ownerField, expiresAtField, date);
    }

    private static Field field(Morphium morphium, Class<?> type, String name) {
        Field f = null;

        try {
            f = morphium.getARHelper().getField(type, name);
        } catch (RuntimeException e) {
            // reported below
        }

        if (f == null) {
            throw new IllegalArgumentException(type.getSimpleName() + " has no field " + name);
        }

        return f;
    }

    /** {@link #tryAcquire(Object, String, Duration, Instant)} with the system clock */
    public LeaseResult<T> tryAcquire(Object leaseId, String owner, Duration ttl) {
        return tryAcquire(leaseId, owner, ttl, Instant.now());
    }

    /**
     * Acquires the lease if it is free or expired at {@code now}, or renews it if {@code owner} holds it: the deadline becomes {@code now + ttl}. One atomic write.
     *
     * @return {@code ACQUIRED} with the lease document, or {@code HELD} if somebody else holds an unexpired lease
     * @throws IllegalArgumentException for a missing id, owner or clock or a ttl that is not positive
     */
    public LeaseResult<T> tryAcquire(Object leaseId, String owner, Duration ttl, Instant now) {
        if (leaseId == null || owner == null || now == null) {
            throw new IllegalArgumentException("lease id, owner and now are required");
        }

        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("ttl must be positive: " + ttl);
        }

        Query<T> q = morphium.createQueryFor(type).f(idField).eq(leaseId);
        q.or(morphium.createQueryFor(type).f(expiresAtField).lt(value(now)), morphium.createQueryFor(type).f(ownerField).eq(owner));
        UpdateOutcome<T> outcome = q.modify().set(ownerField, owner).set(expiresAtField, value(now.plus(ttl))).upsert().tryFindOneAndUpdate(ReturnDocument.AFTER);
        return outcome.isDuplicateKey() ? LeaseResult.held() : LeaseResult.acquired(outcome.getDocument());
    }

    /**
     * Gives the lease up: expires the lease of {@code owner} at once (the document stays). A lease of somebody else, an expired one of another owner and an
     * unknown id are not touched.
     *
     * @return true if {@code owner} held the lease
     */
    public boolean release(Object leaseId, String owner) {
        if (leaseId == null || owner == null) {
            throw new IllegalArgumentException("lease id and owner are required");
        }

        UpdateResult r = morphium.createQueryFor(type).f(idField).eq(leaseId).f(ownerField).eq(owner).modify().set(expiresAtField, value(EXPIRED)).updateOne();
        return r.getMatchedCount() > 0;
    }

    private Object value(Instant instant) {
        return dateExpiry ? Date.from(instant) : instant;
    }
}
