package de.caluga.morphium.query;

import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.Morphium;

import java.lang.reflect.Field;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.function.Supplier;

/**
 * A lease (a time limited, exclusive claim on a name such as a job, a relay or a command) on a collection, built on the update builder: one atomic conditional
 * upsert on the lease document, with the filter {@code key = leaseKey AND (expiresAt <= now OR owner = me)}. A free or expired lease is taken, a lease that the
 * caller holds is granted again, a lease held by somebody else is reported as {@link LeaseResult.Status#HELD} - the lost insert of the upsert is a duplicate key on
 * the key, which {@link UpdateBuilder#tryFindOneAndUpdate(ReturnDocument)} turns into a value. No code needs to be inspected.
 * <pre>{@code
 * LeaseManager<Lease> leases = LeaseManager.of(morphium, Lease.class, "ownerName", "expiresAt");
 * if (leases.tryAcquire("outbox-relay", nodeId, Duration.ofSeconds(30)).isAcquired()) {
 *     ... work; call tryAcquire again before the deadline to renew
 * }
 * leases.release("outbox-relay", nodeId);
 * }</pre>
 * The simple form keys the lease by the {@code @Id}. A lease with a natural key, a generated {@code _id}, an acquisition time and a fencing token needs the
 * {@linkplain #builder builder}:
 * <pre>{@code
 * LeaseManager<JobLease> leases = LeaseManager.builder(morphium, JobLease.class)
 *         .keyField("leaseName").ownerField("holder").expiresAtField("expiresAt")
 *         .acquiredAtField("acquiredAt").fencingField("fencingToken")
 *         .idGenerator(() -> UUID.randomUUID().toString())
 *         .build();
 * LeaseResult<JobLease> r = leases.tryAcquire("outbox-relay", nodeId, ttl);   // every grant counts the token up, also the one to the holder
 * long token = r.getFencingToken();
 * leases.renew("outbox-relay", nodeId, token, ttl);                            // keeps the token; LOST if somebody took the lease over
 * }</pre>
 * Notes:
 * <ul>
 * <li>Without {@code keyField} the lease key is the {@code @Id} of the entity, so the uniqueness is the one of {@code _id}. With another key field the
 * uniqueness is the one of a unique index on that field, and it is what makes the second contender lose: {@code build()} refuses a key field without a declared
 * unique index, not a partial one ({@code @Index(options = "unique:true")}) unless you say {@link Builder#assumeUniqueKey()} (the index is created elsewhere, for example by a
 * migration). Indexes of an entity are created by {@code Morphium.ensureIndicesFor}, an upsert does not create them.</li>
 * <li>The deadline is compared with the {@code now} of the caller, not with the clock of the server: leases between hosts are only as exact as their clocks.
 * Use a ttl that is large compared to the skew. A lease is expired at its deadline ({@code expiresAt <= now}).</li>
 * <li>Fencing: with a {@code fencingField} every grant ({@link #tryAcquire}: a new lease, a takeover, and the holder acquiring again) adds one to the token, a
 * {@link #renew renewal} keeps it. The token only grows, even through {@link #release}, so the work done under a lease can be refused by a token that is older
 * than the one it has seen. For an entity with an {@code @Version} field the version grows with every grant and renewal and is a fencing token of its own.</li>
 * <li>{@link #release(Object, String, long)} gives up one grant only (the one with that token).</li>
 * <li>There is no heartbeat: renewing is calling {@link #renew} (or {@link #tryAcquire} again, which counts as a grant). A lease that is not renewed expires.</li>
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
    private final String keyField;
    private final String ownerField;
    private final String expiresAtField;
    private final String acquiredAtField;
    private final String fencingField;
    private final Supplier<?> idGenerator;
    private final boolean dateExpiry;
    private final boolean dateAcquired;

    private LeaseManager(Builder<T> b, String idField, boolean dateExpiry, boolean dateAcquired) {
        this.morphium = b.morphium;
        this.type = b.type;
        this.idField = idField;
        this.keyField = b.keyField == null ? idField : b.keyField;
        this.ownerField = b.ownerField;
        this.expiresAtField = b.expiresAtField;
        this.acquiredAtField = b.acquiredAtField;
        this.fencingField = b.fencingField;
        this.idGenerator = b.idGenerator;
        this.dateExpiry = dateExpiry;
        this.dateAcquired = dateAcquired;
    }

    /**
     * A lease keyed by the {@code @Id}, without acquisition time and fencing token.
     *
     * @param ownerField     Java name of the {@code String} field that holds the owner
     * @param expiresAtField Java name of the {@code Instant} or {@code java.util.Date} field that holds the deadline
     * @throws IllegalArgumentException if a field does not exist or has the wrong type
     */
    public static <T> LeaseManager<T> of(Morphium morphium, Class<T> type, String ownerField, String expiresAtField) {
        return builder(morphium, type).ownerField(ownerField).expiresAtField(expiresAtField).build();
    }

    /** the builder for a lease with a key other than the {@code @Id}, an acquisition time, a fencing token or a generated {@code _id} */
    public static <T> Builder<T> builder(Morphium morphium, Class<T> type) {
        if (morphium == null || type == null) {
            throw new IllegalArgumentException("morphium and type are required");
        }

        return new Builder<>(morphium, type);
    }

    public static final class Builder<T> {
        private final Morphium morphium;
        private final Class<T> type;
        private String keyField;
        private String ownerField;
        private String expiresAtField;
        private String acquiredAtField;
        private String fencingField;
        private Supplier<?> idGenerator;
        private boolean assumeUniqueKey;

        private Builder(Morphium morphium, Class<T> type) {
            this.morphium = morphium;
            this.type = type;
        }

        /** Java name of the field that names the lease; default: the {@code @Id}. Needs a unique index, see {@link #assumeUniqueKey()}. */
        public Builder<T> keyField(String keyField) {
            this.keyField = keyField;
            return this;
        }

        /** Java name of the {@code String} field that holds the owner (required) */
        public Builder<T> ownerField(String ownerField) {
            this.ownerField = ownerField;
            return this;
        }

        /** Java name of the {@code Instant} or {@code java.util.Date} field that holds the deadline (required) */
        public Builder<T> expiresAtField(String expiresAtField) {
            this.expiresAtField = expiresAtField;
            return this;
        }

        /** Java name of the {@code Instant} or {@code java.util.Date} field that gets the time of each grant (not of a renewal) */
        public Builder<T> acquiredAtField(String acquiredAtField) {
            this.acquiredAtField = acquiredAtField;
            return this;
        }

        /** Java name of the {@code long}, {@code Long}, {@code int} or {@code Integer} field that counts the grants, the fencing token */
        public Builder<T> fencingField(String fencingField) {
            this.fencingField = fencingField;
            return this;
        }

        /** the {@code _id} of a lease that is created; only with a {@link #keyField} other than the {@code @Id} (the id is the key otherwise) */
        public Builder<T> idGenerator(Supplier<?> idGenerator) {
            this.idGenerator = idGenerator;
            return this;
        }

        /** do not check that the key field has a declared unique index: it is created elsewhere (a migration, the database administration) */
        public Builder<T> assumeUniqueKey() {
            this.assumeUniqueKey = true;
            return this;
        }

        /** @throws IllegalArgumentException if a field does not exist or has the wrong type, or the key field has no declared unique index */
        public LeaseManager<T> build() {
            if (ownerField == null || expiresAtField == null) {
                throw new IllegalArgumentException("the owner field and the expiry field are required");
            }

            String idField = morphium.getARHelper().getIdFieldName(type);
            String key = keyField == null ? idField : keyField;
            java.util.Set<String> names = new java.util.HashSet<>();

            for (String name : new String[] {key, ownerField, expiresAtField, acquiredAtField, fencingField}) {
                if (name != null && !names.add(name)) {
                    throw new IllegalArgumentException("a field is used for two purposes: " + name);
                }
            }

            Field owner = field(morphium, type, ownerField);
            Field expires = field(morphium, type, expiresAtField);

            if (!owner.getType().equals(String.class)) {
                throw new IllegalArgumentException(type.getSimpleName() + "." + ownerField + " must be a String, not " + owner.getType().getSimpleName());
            }

            boolean dateExpiry = isDate(expires, expiresAtField);
            boolean dateAcquired = false;

            if (acquiredAtField != null) {
                dateAcquired = isDate(field(morphium, type, acquiredAtField), acquiredAtField);
            }

            if (fencingField != null) {
                Class<?> t = field(morphium, type, fencingField).getType();

                if (!(t.equals(long.class) || t.equals(Long.class) || t.equals(int.class) || t.equals(Integer.class))) {
                    throw new IllegalArgumentException(type.getSimpleName() + "." + fencingField + " must be a long or an int, not " + t.getSimpleName());
                }
            }

            if (idGenerator != null && key.equals(idField)) {
                throw new IllegalArgumentException("an id generator needs a key field other than the id: the lease key is the id");
            }

            if (!key.equals(idField)) {
                field(morphium, type, key);

                if (!assumeUniqueKey && !hasUniqueIndexOn(key)) {
                    throw new IllegalArgumentException(type.getSimpleName() + "." + key + " is the key of the lease and needs a unique index (@Index(options = \"unique:true\")),"
                            + " otherwise two contenders can both create the lease; use assumeUniqueKey() if the index is created elsewhere");
                }
            }

            return new LeaseManager<>(this, idField, dateExpiry, dateAcquired);
        }

        private boolean isDate(Field f, String name) {
            if (f.getType().equals(Date.class)) {
                return true;
            }

            if (!f.getType().equals(Instant.class)) {
                throw new IllegalArgumentException(type.getSimpleName() + "." + name + " must be an Instant or a java.util.Date, not " + f.getType().getSimpleName());
            }

            return false;
        }

        private boolean hasUniqueIndexOn(String javaName) {
            String stored = morphium.getARHelper().getMongoFieldName(type, javaName);

            for (IndexDescription d : morphium.getIndexesFromEntity(type)) {
                if (Boolean.TRUE.equals(d.getUnique()) && d.getPartialFilterExpression() == null && d.getKey() != null && d.getKey().size() == 1 && d.getKey().containsKey(stored)) {
                    return true;
                }
            }

            return false;
        }
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
    public LeaseResult<T> tryAcquire(Object leaseKey, String owner, Duration ttl) {
        return tryAcquire(leaseKey, owner, ttl, Instant.now());
    }

    /**
     * Acquires the lease if it is free or expired at {@code now}, or acquires it again if {@code owner} holds it: the deadline becomes {@code now + ttl}, the
     * fencing token (if there is a fencing field) grows by one, the acquisition time (if there is a field) is {@code now}. One atomic write.
     *
     * @param leaseKey the value of the key field, the {@code @Id} without a key field
     * @return {@code ACQUIRED} with the lease document, or {@code HELD} if somebody else holds an unexpired lease
     * @throws IllegalArgumentException for a missing key, owner or clock or a ttl that is not positive
     */
    public LeaseResult<T> tryAcquire(Object leaseKey, String owner, Duration ttl, Instant now) {
        if (leaseKey == null || owner == null || now == null) {
            throw new IllegalArgumentException("lease key, owner and now are required");
        }

        requirePositive(ttl);
        Query<T> q = morphium.createQueryFor(type).f(keyField).eq(leaseKey);
        q.or(morphium.createQueryFor(type).f(expiresAtField).lte(value(now)), morphium.createQueryFor(type).f(ownerField).eq(owner));
        UpdateBuilder<T> update = q.modify().set(ownerField, owner).set(expiresAtField, value(now.plus(ttl)));

        if (acquiredAtField != null) {
            update.set(acquiredAtField, dateAcquired ? Date.from(now) : now);
        }

        if (fencingField != null) {
            update.inc(fencingField, 1);
        }

        if (!keyField.equals(idField)) {
            // the key is also seeded from the filter; this is what the document is created with, whichever way the driver treats the $or
            update.setOnInsert(keyField, leaseKey);

            if (idGenerator != null) {
                update.setOnInsert(idField, idGenerator.get());
            }
        }

        UpdateOutcome<T> outcome = update.upsert().tryFindOneAndUpdate(ReturnDocument.AFTER);
        return outcome.isDuplicateKey() ? LeaseResult.held() : LeaseResult.acquired(outcome.getDocument(), tokenOf(outcome.getDocument()));
    }

    /** {@link #renew(Object, String, Duration, Instant)} with the system clock */
    public LeaseResult<T> renew(Object leaseKey, String owner, Duration ttl) {
        return renew(leaseKey, owner, ttl, Instant.now());
    }

    /**
     * Renews a lease of a manager without a fencing field: the deadline becomes {@code now + ttl} if {@code owner} is the owner of the lease, whether it has
     * expired or not (as long as nobody else took it over).
     *
     * @return {@code ACQUIRED} with the lease document, or {@code LOST} if the lease does not exist or has another owner
     * @throws IllegalArgumentException for a missing key, owner or clock, a ttl that is not positive, or a manager with a fencing field (the token is required)
     */
    public LeaseResult<T> renew(Object leaseKey, String owner, Duration ttl, Instant now) {
        if (fencingField != null) {
            throw new IllegalArgumentException("this manager has a fencing field: renew with the token");
        }

        return renewInternal(leaseKey, owner, null, ttl, now);
    }

    /** {@link #renew(Object, String, long, Duration, Instant)} with the system clock */
    public LeaseResult<T> renew(Object leaseKey, String owner, long fencingToken, Duration ttl) {
        return renew(leaseKey, owner, fencingToken, ttl, Instant.now());
    }

    /**
     * Renews the lease of {@code owner} that carries {@code fencingToken}: only the deadline becomes {@code now + ttl}, the token and the acquisition time stay.
     * The token is the guard: every takeover and every new grant changes it, so a renewal with an older token cannot revive a lease that was lost, also not
     * after the deadline (an expired lease that nobody took over can still be renewed).
     *
     * @return {@code ACQUIRED} with the lease document and its (unchanged) token, or {@code LOST} if no lease of this owner and token exists
     * @throws IllegalArgumentException for a missing key, owner or clock, a ttl that is not positive, or a manager without a fencing field
     */
    public LeaseResult<T> renew(Object leaseKey, String owner, long fencingToken, Duration ttl, Instant now) {
        if (fencingField == null) {
            throw new IllegalArgumentException("this manager has no fencing field: renew without a token");
        }

        return renewInternal(leaseKey, owner, Long.valueOf(fencingToken), ttl, now);
    }

    private LeaseResult<T> renewInternal(Object leaseKey, String owner, Long fencingToken, Duration ttl, Instant now) {
        if (leaseKey == null || owner == null || now == null) {
            throw new IllegalArgumentException("lease key, owner and now are required");
        }

        requirePositive(ttl);
        Query<T> q = morphium.createQueryFor(type).f(keyField).eq(leaseKey).f(ownerField).eq(owner);

        if (fencingToken != null) {
            q = q.f(fencingField).eq(fencingToken);
        }

        UpdateOutcome<T> outcome = q.modify().set(expiresAtField, value(now.plus(ttl))).tryFindOneAndUpdate(ReturnDocument.AFTER);
        return outcome.getKind() == UpdateOutcome.Kind.UPDATED ? LeaseResult.acquired(outcome.getDocument(), tokenOf(outcome.getDocument())) : LeaseResult.lost();
    }

    /**
     * Gives the lease up: expires the lease of {@code owner} at once (the document stays, and with it the fencing token). A lease of somebody else, an expired
     * one of another owner and an unknown key are not touched.
     *
     * @return true if {@code owner} held the lease
     */
    public boolean release(Object leaseKey, String owner) {
        if (leaseKey == null || owner == null) {
            throw new IllegalArgumentException("lease key and owner are required");
        }

        UpdateResult r = morphium.createQueryFor(type).f(keyField).eq(leaseKey).f(ownerField).eq(owner).modify().set(expiresAtField, value(EXPIRED)).updateOne();
        return r.getMatchedCount() > 0;
    }

    /**
     * {@link #release(Object, String)} for the grant that carries {@code fencingToken}: a holder that acquired the lease again does not give up the newer grant
     * by releasing with the token of the older one.
     *
     * @return true if {@code owner} held the lease with this token
     * @throws IllegalArgumentException for a manager without a fencing field
     */
    public boolean release(Object leaseKey, String owner, long fencingToken) {
        if (fencingField == null) {
            throw new IllegalArgumentException("this manager has no fencing field: release without a token");
        }

        if (leaseKey == null || owner == null) {
            throw new IllegalArgumentException("lease key and owner are required");
        }

        UpdateResult r = morphium.createQueryFor(type).f(keyField).eq(leaseKey).f(ownerField).eq(owner).f(fencingField).eq(fencingToken).modify()
                .set(expiresAtField, value(EXPIRED)).updateOne();
        return r.getMatchedCount() > 0;
    }

    private static void requirePositive(Duration ttl) {
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("ttl must be positive: " + ttl);
        }
    }

    private Long tokenOf(T lease) {
        if (fencingField == null || lease == null) {
            return null;
        }

        Object v = morphium.getARHelper().getValue(lease, fencingField);
        return v instanceof Number n ? n.longValue() : null;
    }

    private Object value(Instant instant) {
        return dateExpiry ? Date.from(instant) : instant;
    }
}
