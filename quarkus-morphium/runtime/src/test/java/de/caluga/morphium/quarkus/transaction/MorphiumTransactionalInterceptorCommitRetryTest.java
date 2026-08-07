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
package de.caluga.morphium.quarkus.transaction;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumTransactionContext;
import jakarta.enterprise.event.Event;
import jakarta.interceptor.InvocationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression test for the commit-retry data-loss bug found by Stephan Boesebeck's re-review
 * (2026-08-06, item A) on top of the blocker #5 fix.
 *
 * <p>{@code PooledDriver.commitTransaction()} clears the transaction context in a {@code finally}
 * block unconditionally -- even when the commit command itself failed. Without
 * {@code safeCommitWithRetry()} re-installing the saved context before each retry attempt,
 * {@code safeCommit()}'s own {@code morphium.getTransaction() == null} check (meant to tolerate
 * "no DB operations occurred at all") misinterprets "the driver already cleared the context
 * after a FAILED commit attempt" as exactly that, and returns normally -- silently converting a
 * real commit failure (nothing persisted) into a reported success.
 *
 * <p>This test builds a fake {@link MorphiumDriver} whose {@code commitTransaction()} mimics
 * {@code PooledDriver}'s exact behavior: it always clears the transaction context, even when it
 * throws. The first call throws a transient (code 112) {@link MorphiumDriverException}; the
 * second call (the retry) succeeds. If {@code safeCommitWithRetry()} does not re-install the
 * saved context before the retry, {@code safeCommit()} would see a null context on the second
 * attempt and short-circuit as "success" WITHOUT actually calling {@code commitTransaction()}
 * again -- which this test also verifies against directly (via the invocation count on the fake
 * driver), not just the interceptor's return value.
 */
@DisplayName("MorphiumTransactionalInterceptor — commit-retry context preservation")
class MorphiumTransactionalInterceptorCommitRetryTest {

    private Morphium morphium;
    private MorphiumDriver driver;
    private MorphiumTransactionalInterceptor interceptor;

    /** Mimics PooledDriver's real (buggy-if-not-handled) behavior: the transaction context
     *  ThreadLocal is cleared unconditionally by commitTransaction(), success or failure. */
    private MorphiumTransactionContext transactionContext;

    @BeforeEach
    void setUp() {
        morphium = mock(Morphium.class);
        driver = mock(MorphiumDriver.class);
        // Must start out null: aroundInvoke() checks morphium.getTransaction() != null very
        // early for REQUIRED-propagation ("join an already active transaction"). If this were
        // pre-seeded with a mock here, aroundInvoke() would take that join-existing-transaction
        // branch and return ctx.proceed() directly, calling neither startTransaction() nor
        // commitTransaction() at all -- the doAnswer stub for morphium.startTransaction() below
        // is what assigns a fresh mock to this field once aroundInvoke() actually starts its own
        // transaction.
        transactionContext = null;

        when(morphium.getDriver()).thenReturn(driver);
        try {
            when(driver.isCosmosDB()).thenReturn(false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // getTransaction()/setTransaction() delegate to a single mutable field, exactly like
        // the real Morphium.getTransaction()/setTransaction() delegate to the driver's
        // transaction-context ThreadLocal.
        when(morphium.getTransaction()).thenAnswer(inv -> transactionContext);
        doAnswer(inv -> {
            transactionContext = inv.getArgument(0);
            return null;
        }).when(morphium).setTransaction(any());

        doAnswer(inv -> {
            transactionContext = mock(MorphiumTransactionContext.class);
            return null;
        }).when(morphium).startTransaction();

        interceptor = new MorphiumTransactionalInterceptor();
        interceptor.morphium = morphium;
        interceptor.beforeCommit = noopEvent();
        interceptor.afterCommit = noopEvent();
        interceptor.afterRollback = noopEvent();
    }

    @SuppressWarnings("unchecked")
    private static Event<MorphiumTransactionEvent> noopEvent() {
        return mock(Event.class);
    }

    private InvocationContext fakeInvocationContext(Object returnValue) throws Exception {
        InvocationContext ctx = mock(InvocationContext.class);
        Method dummyMethod = String.class.getMethod("trim");
        when(ctx.getMethod()).thenReturn(dummyMethod);
        when(ctx.proceed()).thenReturn(returnValue);
        return ctx;
    }

    @Test
    @DisplayName("code 112 (WriteConflict) at commit time is retried by re-installing the saved context, not silently treated as success")
    void transientCommitFailure_retriesWithRestoredContext_notSilentSuccess() throws Throwable {
        AtomicInteger commitCallCount = new AtomicInteger(0);

        // Mimic PooledDriver.commitTransaction(): finally { clearTransactionContext(); } runs
        // unconditionally, even when the commit command itself failed.
        doAnswer(inv -> {
            int call = commitCallCount.incrementAndGet();
            try {
                if (call == 1) {
                    MorphiumDriverException e = new MorphiumDriverException("WriteConflict at commit");
                    e.setMongoCode(112);
                    throw e;
                }
                // call == 2 (the retry): succeeds
                return null;
            } finally {
                transactionContext = null; // PooledDriver's unconditional finally-block clear
            }
        }).when(morphium).commitTransaction();

        InvocationContext ctx = fakeInvocationContext("business-result");
        // Deliberately NOT calling morphium.startTransaction() here first: aroundInvoke() itself
        // checks morphium.getTransaction() != null for REQUIRED-propagation (join an already
        // active transaction) before doing anything else. Pre-seeding a context would make it
        // take that join-existing-transaction branch and return ctx.proceed() directly, calling
        // neither startTransaction() nor commitTransaction() at all -- exactly the failure mode
        // that produced a false "success" here on the first attempt at writing this test
        // (commitCallCount stayed at 0, not 2, because aroundInvoke() never got past the
        // REQUIRED-propagation check to its own transaction-start/commit logic).
        Object result = invokeAroundInvoke(ctx);

        assertThat(result).as("business method result must be returned on eventual success")
                .isEqualTo("business-result");
        assertThat(commitCallCount.get())
                .as("commitTransaction() must actually be called twice: once (fails), once more (the retry) -- "
                        + "if safeCommit() short-circuited on the second attempt seeing a null context, "
                        + "this would be 1, not 2")
                .isEqualTo(2);
    }

    @Test
    @DisplayName("code 11000 (DuplicateKey) at commit time is NOT transient -- must not be retried and must propagate")
    void nonTransientCommitFailure_isNotRetried_andPropagates() throws Exception {
        AtomicInteger commitCallCount = new AtomicInteger(0);

        // Same fake-driver pattern as the transient case above, but the commit failure is a
        // non-transient MongoDB error (11000 / DuplicateKey is not in isTransientTransactionError()'s
        // allow-list of 112/251). safeCommitWithRetry() must therefore rethrow immediately after
        // the FIRST attempt instead of retrying, and aroundInvoke() must propagate that exception
        // out to the caller (after firing AFTER_ROLLBACK, not AFTER_COMMIT).
        doAnswer(inv -> {
            commitCallCount.incrementAndGet();
            try {
                MorphiumDriverException e = new MorphiumDriverException("E11000 duplicate key error");
                e.setMongoCode(11000);
                throw e;
            } finally {
                transactionContext = null; // PooledDriver's unconditional finally-block clear
            }
        }).when(morphium).commitTransaction();

        InvocationContext ctx = fakeInvocationContext("business-result");

        assertThatThrownBy(() -> invokeAroundInvoke(ctx))
                .as("a non-transient commit error must propagate out of aroundInvoke(), not be swallowed")
                .isInstanceOf(MorphiumDriverException.class)
                .satisfies(t -> assertThat(((MorphiumDriverException) t).getMongoCode()).isEqualTo(11000));

        assertThat(commitCallCount.get())
                .as("commitTransaction() must be called exactly once: a non-transient error must not be retried")
                .isEqualTo(1);
    }

    /** Invokes the package-private aroundInvoke() via reflection (it's not public API). */
    private Object invokeAroundInvoke(InvocationContext ctx) throws Throwable {
        try {
            Method m = MorphiumTransactionalInterceptor.class.getDeclaredMethod("aroundInvoke", InvocationContext.class);
            m.setAccessible(true);
            return m.invoke(interceptor, ctx);
        } catch (java.lang.reflect.InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
