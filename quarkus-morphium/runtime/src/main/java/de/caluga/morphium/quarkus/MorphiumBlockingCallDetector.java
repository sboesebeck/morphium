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
package de.caluga.morphium.quarkus;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumAccessVetoException;
import de.caluga.morphium.MorphiumStorageListener;
import de.caluga.morphium.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Detects Morphium write operations that are called from the Vert.x I/O event-loop thread.
 *
 * <p>Morphium write operations are blocking (they communicate synchronously with MongoDB).
 * Calling them directly from a Vert.x event-loop thread will stall the event loop, which
 * causes health-check timeouts and general request degradation.
 *
 * <p>{@link #registerOn(Morphium)} attaches a {@link MorphiumStorageListener} that logs a
 * clear {@code WARN} with fix instructions whenever a write is attempted on an event-loop
 * thread. No Vert.x API dependency is required — detection is based solely on the well-known
 * thread-name prefix {@code "vert.x-eventloop-thread"}.
 *
 * <p><b>Fix:</b> annotate the offending JAX-RS method with
 * {@code @io.smallrye.common.annotation.RunOnVirtualThread} (preferred) or
 * {@code @io.smallrye.common.annotation.Blocking}.
 *
 * <p><b>Why this is not a CDI bean anymore:</b> this used to be an {@code @ApplicationScoped}
 * bean that injected {@code Instance<Morphium>} and dereferenced it (via {@code .get()}) from
 * a {@code StartupEvent} observer to register the listener. That dereference is what actually
 * triggers {@code MorphiumProducer.buildMorphium()} — a blocking connect with a full retry
 * ladder — because {@code Morphium} is a normal-scoped CDI bean whose proxy connects lazily on
 * first real use. Running that on a background thread (the previous workaround) only kept the
 * Quarkus boot thread free; it did not stop the eager connect attempt itself, so without a
 * reachable MongoDB the extension would still burn through the full retry ladder in the
 * background — exactly the failure this class must never cause, since it is pure debug
 * diagnostics. Registering the listener here, called directly from
 * {@link MorphiumProducer#buildMorphium()} right after the real connect has already succeeded,
 * makes it structurally impossible for this class to be the cause of a connect: by the time
 * {@link #registerOn(Morphium)} runs, the {@code Morphium} instance already exists.
 */
public final class MorphiumBlockingCallDetector {

    private static final Logger log = LoggerFactory.getLogger(MorphiumBlockingCallDetector.class);
    private static final String EVENTLOOP_THREAD_PREFIX = "vert.x-eventloop-thread";
    private static final long WARN_INTERVAL_NANOS = Duration.ofSeconds(30).toNanos();
    private static final AtomicLong lastWarnNanos = new AtomicLong(0);

    private MorphiumBlockingCallDetector() {}

    /**
     * Registers the storage listener on the given, already-connected {@link Morphium} instance.
     * Called from {@link MorphiumProducer#buildMorphium()} once the connection has been
     * established — never triggers a connect itself.
     */
    public static void registerOn(Morphium morphium) {
        morphium.addListener(new MorphiumStorageListener<Object>() {
            @Override
            public void preStore(Morphium m, Object r, boolean isNew) throws MorphiumAccessVetoException {
                warnIfOnEventLoop();
            }

            @Override
            public void preStore(Morphium m, Map<Object, Boolean> isNew) throws MorphiumAccessVetoException {
                warnIfOnEventLoop();
            }

            @Override
            public void postStore(Morphium m, Object r, boolean isNew) {}

            @Override
            public void postStore(Morphium m, Map<Object, Boolean> isNew) {}

            @Override
            public void preRemove(Morphium m, Query<Object> q) throws MorphiumAccessVetoException {
                warnIfOnEventLoop();
            }

            @Override
            public void preRemove(Morphium m, Object r) throws MorphiumAccessVetoException {
                warnIfOnEventLoop();
            }

            @Override
            public void postRemove(Morphium m, Object r) {}

            @Override
            public void postRemove(Morphium m, List<Object> lst) {}

            @Override
            public void postRemove(Morphium m, Query<Object> q) {}

            @Override
            public void postLoad(Morphium m, Object o) {}

            @Override
            public void postLoad(Morphium m, List<Object> o) {}

            @Override
            public void preDrop(Morphium m, Class<? extends Object> cls) throws MorphiumAccessVetoException {}

            @Override
            public void postDrop(Morphium m, Class<? extends Object> cls) {}

            @Override
            public void preUpdate(Morphium m, Class<? extends Object> cls, Enum updateType) throws MorphiumAccessVetoException {
                warnIfOnEventLoop();
            }

            @Override
            public void postUpdate(Morphium m, Class<? extends Object> cls, Enum updateType) {}
        });
    }

    private static void warnIfOnEventLoop() {
        String threadName = Thread.currentThread().getName();
        if (threadName.startsWith(EVENTLOOP_THREAD_PREFIX) && shouldWarnNow()) {
            log.warn("""
                    [Morphium] Blocking write operation called from Vert.x I/O thread '{}'.
                    This blocks the event loop and can cause request timeouts and health-check failures.
                    Fix: Add @RunOnVirtualThread (recommended) or @Blocking to your JAX-RS method.
                    See: https://quarkus.io/guides/rest#blocking-non-blocking""",
                    threadName);
        }
    }

    private static boolean shouldWarnNow() {
        long now = System.nanoTime();
        long last = lastWarnNanos.get();
        return now - last >= WARN_INTERVAL_NANOS && lastWarnNanos.compareAndSet(last, now);
    }
}
