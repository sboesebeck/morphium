package de.caluga.morphium.query;

import java.util.Objects;

/**
 * Result of {@link LeaseManager#tryAcquire} and {@link LeaseManager#renew}: either the lease is {@code ACQUIRED} (the caller holds it now and gets the lease
 * document as written; a renewal that worked is reported the same way), or it is {@code HELD} by somebody else (acquire), or a renewal is {@code LOST}: the
 * lease does not exist any more for this owner and token (somebody took it over, or it never existed).
 *
 * @param <T> the lease entity
 * @apiNote incubating in 7.0, may change in a later 7.x release
 */
public final class LeaseResult<T> {

    public enum Status {
        ACQUIRED, HELD, LOST
    }

    private final Status status;
    private final T lease;
    private final Long fencingToken;

    private LeaseResult(Status status, T lease, Long fencingToken) {
        this.status = status;
        this.lease = lease;
        this.fencingToken = fencingToken;
    }

    static <T> LeaseResult<T> acquired(T lease) {
        return acquired(lease, null);
    }

    static <T> LeaseResult<T> acquired(T lease, Long fencingToken) {
        if (lease == null) {
            throw new IllegalArgumentException("an acquired lease has a document");
        }

        return new LeaseResult<>(Status.ACQUIRED, lease, fencingToken);
    }

    static <T> LeaseResult<T> held() {
        return new LeaseResult<>(Status.HELD, null, null);
    }

    static <T> LeaseResult<T> lost() {
        return new LeaseResult<>(Status.LOST, null, null);
    }

    public Status getStatus() {
        return status;
    }

    public boolean isAcquired() {
        return status == Status.ACQUIRED;
    }

    /**
     * the fencing token of the lease as written: it grows with every grant and stays the same for a renewal; {@code null} if the lease is not acquired or the
     * manager has no fencing field
     */
    public Long getFencingToken() {
        return fencingToken;
    }

    /** the lease document after the write (with the new deadline and, for an {@code @Version} entity, the new version - a fencing token), {@code null} if the lease is held */
    public T getLease() {
        return lease;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof LeaseResult<?> that && status == that.status && Objects.equals(lease, that.lease) && Objects.equals(fencingToken, that.fencingToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, lease, fencingToken);
    }

    @Override
    public String toString() {
        return "LeaseResult{" + status + (lease != null ? ", lease=" + lease : "") + (fencingToken != null ? ", token=" + fencingToken : "") + "}";
    }
}
