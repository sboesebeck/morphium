/*
 * Copyright 2025 The Quarkiverse Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.caluga.morphium.showcase.atomic;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.query.LeaseManager;
import de.caluga.morphium.query.LeaseResult;
import de.caluga.morphium.query.ReturnDocument;
import de.caluga.morphium.query.UpdateOutcome;
import de.caluga.morphium.showcase.atomic.entity.JobState;
import de.caluga.morphium.showcase.atomic.entity.JobTicket;
import de.caluga.morphium.showcase.atomic.entity.WorkLease;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * The operations of the "Atomic Updates" page. Each method returns the sentence the page shows, because the interesting part of an atomic operation is the outcome:
 * who won, what was refused, which error came back.
 */
@ApplicationScoped
public class AtomicService {

    /** the key of the lease that the page competes for */
    public static final String LEASE_KEY = "nightly-import";

    private static final Duration LEASE_TTL = Duration.ofSeconds(60);

    @Inject
    Morphium morphium;

    @Inject
    JobTicketRepository tickets;

    /** the three sample tickets: two new ones and one that is done; existing tickets stay as they are */
    public void seed() {
        if (morphium.createQueryFor(JobTicket.class).countAll() > 0) {
            return;
        }

        morphium.store(JobTicket.builder().id("T-1").code("A-100").state(JobState.NEW).build());
        morphium.store(JobTicket.builder().id("T-2").code("A-200").state(JobState.NEW).build());
        morphium.store(JobTicket.builder().id("T-3").code("A-300").state(JobState.DONE).owner("carol").attempts(1).build());
    }

    public void reset() {
        // delete, do not drop: dropping the collection would drop its unique index, too
        morphium.createQueryFor(JobTicket.class).delete();
        morphium.createQueryFor(WorkLease.class).delete();
        seed();
    }

    public List<JobTicket> findAll() {
        return morphium.createQueryFor(JobTicket.class).sort(JobTicket.Fields.id).asList();
    }

    public WorkLease currentLease() {
        return morphium.createQueryFor(WorkLease.class).f(WorkLease.Fields.id).eq(LEASE_KEY).get();
    }

    /**
     * A worker claims a ticket: ONE atomic update that only matches while the ticket is {@code NEW}. The builder translates the names, maps the enum and the {@link Instant}, counts the
     * attempt and sets {@code changedAt}; the typed outcome says whether this worker won.
     */
    public String claim(String ticketId, String worker) {
        UpdateOutcome<JobTicket> outcome = morphium.createQueryFor(JobTicket.class)
                .f(JobTicket.Fields.id).eq(ticketId)
                .f(JobTicket.Fields.state).eq(JobState.NEW)
                .modify()
                .set(JobTicket.Fields.state, JobState.RUNNING)
                .set(JobTicket.Fields.owner, worker)
                .set(JobTicket.Fields.startedAt, Instant.now())
                .inc(JobTicket.Fields.attempts, 1)
                .tryFindOneAndUpdate(ReturnDocument.AFTER);

        return switch (outcome.getKind()) {
            case UPDATED -> worker + " claimed " + ticketId + " (attempt " + outcome.getDocument().getAttempts() + ").";
            case NOT_MATCHED -> worker + " lost: " + ticketId + " is not NEW (taken by somebody else, or unknown).";
            case INSERTED, DUPLICATE_KEY -> "Unexpected outcome " + outcome.getKind();
        };
    }

    /** The count form of a derived update method: every ticket in the state gets the owner. */
    public long assignAll(JobState state, String owner) {
        return tickets.updateByState(state, owner);
    }

    /** The find-and-modify form of a derived update method: one ticket, as it is after the update. */
    public String assignOne(String ticketId, JobState state, String owner) {
        return tickets.updateByIdAndState(ticketId, state, owner)
                .map(t -> ticketId + " belongs to " + t.getOwner() + " now.")
                .orElse(ticketId + " is not in state " + state + ": nothing was changed.");
    }

    /**
     * Changes the unique {@code code} of a ticket with {@code Query.set}. If another ticket has that code, the server rejects the statement; since 7.0 that raises a
     * {@link MorphiumDriverException} (6.x ignored the reply and returned as if the write had happened).
     */
    public String changeCode(String ticketId, String newCode) {
        try {
            morphium.createQueryFor(JobTicket.class).f(JobTicket.Fields.id).eq(ticketId).set(JobTicket.Fields.code, newCode);
            return "The code of " + ticketId + " is " + newCode + " now.";
        } catch (MorphiumDriverException e) {
            return "Rejected: " + (e.isDuplicateKey() ? "duplicate key" : "write error") + " (code " + e.getMongoCode() + "). The ticket was not changed.";
        }
    }

    /** A worker asks for the lease: one conditional upsert; the one that holds it keeps it, the others are told who does. */
    public String acquireLease(String worker) {
        LeaseResult<WorkLease> result = leases().tryAcquire(LEASE_KEY, worker, LEASE_TTL);

        if (result.isAcquired()) {
            return worker + " holds the lease for " + LEASE_TTL.toSeconds() + " s.";
        }

        // a lost race carries no document: the holder is read
        WorkLease holder = currentLease();
        return worker + " did not get it: " + (holder != null ? holder.getOwnerName() : "somebody else") + " holds it.";
    }

    public String releaseLease(String worker) {
        return leases().release(LEASE_KEY, worker) ? worker + " released the lease." : worker + " does not hold the lease.";
    }

    private LeaseManager<WorkLease> leases() {
        return LeaseManager.of(morphium, WorkLease.class, WorkLease.Fields.ownerName, WorkLease.Fields.expiresAt);
    }
}
