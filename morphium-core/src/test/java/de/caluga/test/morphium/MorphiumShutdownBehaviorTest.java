package de.caluga.test.morphium;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.cache.MorphiumCacheImpl;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.Test;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MorphiumShutdownBehaviorTest {

    private Morphium newMorphium() {
        MorphiumConfig config = new MorphiumConfig("shutdown_behavior_test_db", 10, 10_000, 10_000);
        config.driverSettings().setDriverName(InMemoryDriver.driverName);
        config.writerSettings().setWriteBufferTimeGranularity(300_000);
        return new Morphium(config);
    }

    @Test
    void queueTaskRejectsSubmissionAfterClose() {
        Morphium morphium = newMorphium();
        morphium.close();

        assertThatThrownBy(() -> morphium.queueTask(() -> { }))
                .isInstanceOf(RejectedExecutionException.class);
    }

    @Test
    void throwingShutdownListenerDoesNotAbortCleanup() {
        Morphium morphium = newMorphium();
        AtomicBoolean laterListenerRan = new AtomicBoolean();
        morphium.addShutdownListener(m -> {
            throw new IllegalStateException("listener failure");
        });
        morphium.addShutdownListener(m -> laterListenerRan.set(true));

        assertThatThrownBy(morphium::close)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("1 failure");

        assertThat(laterListenerRan).isTrue();
        assertThat(morphium.getConfig()).isNull();
        assertThatThrownBy(() -> morphium.queueTask(() -> { }))
                .isInstanceOf(RejectedExecutionException.class);
    }

    @Test
    void failingCacheResetDoesNotSkipCacheCloseOrFinalCleanup() {
        AtomicBoolean cacheCloseRan = new AtomicBoolean();
        MorphiumCacheImpl cache = new MorphiumCacheImpl() {
            @Override
            public void resetCache() {
                throw new IllegalStateException("cache reset failed");
            }

            @Override
            public void close() {
                cacheCloseRan.set(true);
            }
        };
        MorphiumConfig config = new MorphiumConfig("shutdown_behavior_test_db", 10, 10_000, 10_000);
        config.driverSettings().setDriverName(InMemoryDriver.driverName);
        config.writerSettings().setWriteBufferTimeGranularity(300_000);
        config.cacheSettings().setCache(cache);
        Morphium morphium = new Morphium(config);

        assertThatThrownBy(morphium::close)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("1 failure")
                .satisfies(error -> assertThat(error.getSuppressed()[0].getCause())
                        .hasMessage("cache reset failed"));

        assertThat(cacheCloseRan).isTrue();
        assertThat(morphium.getConfig()).isNull();
        // The close gate stays one-shot even when a cleanup step reported failure.
        morphium.close();
    }

    @Test
    void failingDriverCloseIsReportedAndDoesNotSkipLaterCleanup() {
        AtomicBoolean cacheCloseRan = new AtomicBoolean();
        MorphiumCacheImpl cache = new MorphiumCacheImpl() {
            @Override
            public void close() {
                cacheCloseRan.set(true);
            }
        };
        MorphiumConfig config = new MorphiumConfig("shutdown_behavior_test_db", 10, 10_000, 10_000);
        config.driverSettings().setDriverName(InMemoryDriver.driverName);
        config.writerSettings().setWriteBufferTimeGranularity(300_000);
        config.cacheSettings().setCache(cache);
        Morphium morphium = new Morphium(config);
        morphium.setDriver(new InMemoryDriver() {
            @Override
            public void close() {
                throw new IllegalStateException("driver close failed");
            }
        });

        assertThatThrownBy(morphium::close)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("1 failure")
                .satisfies(error -> assertThat(error.getSuppressed()[0].getCause().getCause())
                        .hasMessage("driver close failed"));

        assertThat(cacheCloseRan).isTrue();
        assertThat(morphium.getConfig()).isNull();
    }

    @Test
    void concurrentCloseRunsCleanupOnce() throws Exception {
        Morphium morphium = newMorphium();
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch listenerEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        morphium.addShutdownListener(m -> {
            calls.incrementAndGet();
            listenerEntered.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("timed out waiting for close callers");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        });

        Thread first = new Thread(morphium::close);
        first.start();
        try {
            assertThat(listenerEntered.await(5, TimeUnit.SECONDS)).isTrue();
            Thread second = new Thread(morphium::close);
            second.start();
            second.join(5000);
            assertThat(second.isAlive()).isFalse();
        } finally {
            release.countDown();
            first.join(5000);
        }
        assertThat(first.isAlive()).isFalse();
        assertThat(calls).hasValue(1);
        assertThat(morphium.getConfig()).isNull();
    }
}
