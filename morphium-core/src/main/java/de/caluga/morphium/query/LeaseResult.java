package de.caluga.morphium.query;

import java.util.Objects;

/**
 * Result of {@link LeaseManager#tryAcquire}: either the lease is {@code ACQUIRED} (the caller holds it now and gets the lease document as written), or it is
 * {@code HELD} by somebody else.
 *
 * @param <T> the lease entity
 * @apiNote incubating until 6.3
 */
public final class LeaseResult<T> {

    public enum Status {
        ACQUIRED, HELD
    }

    private final Status status;
    private final T lease;

    private LeaseResult(Status status, T lease) {
        this.status = status;
        this.lease = lease;
    }

    static <T> LeaseResult<T> acquired(T lease) {
        if (lease == null) {
            throw new IllegalArgumentException("an acquired lease has a document");
        }

        return new LeaseResult<>(Status.ACQUIRED, lease);
    }

    static <T> LeaseResult<T> held() {
        return new LeaseResult<>(Status.HELD, null);
    }

    public Status getStatus() {
        return status;
    }

    public boolean isAcquired() {
        return status == Status.ACQUIRED;
    }

    /** the lease document after the write (with the new deadline and, for an {@code @Version} entity, the new version - a fencing token), {@code null} if the lease is held */
    public T getLease() {
        return lease;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof LeaseResult<?> that && status == that.status && Objects.equals(lease, that.lease);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, lease);
    }

    @Override
    public String toString() {
        return "LeaseResult{" + status + (lease != null ? ", lease=" + lease : "") + "}";
    }
}
