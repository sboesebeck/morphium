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
package de.caluga.morphium.quarkus.observability;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumStorageAdapter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Times the update operations of the writer path ({@code Query.set}, {@code inc}, {@code push},
 * {@code pull}, ..., the bulk context) as the Micrometer timer {@code morphium.operations.duration}
 * with the tags {@code database}, {@code operation=update}, {@code update_type} and {@code entity}.
 *
 * <p>The source is the pair {@code preUpdate}/{@code postUpdate} of {@link MorphiumStorageAdapter}; the
 * writer fires both in the same task, so the start time is kept per thread. A write that fails never
 * reaches {@code postUpdate}: its start time stays on the thread's stack, below the start of the next
 * successful write of the same kind, which is popped first, so a failure does not distort a later
 * duration (the stack of a kind is capped, the oldest entry is dropped). A {@code postUpdate} without a
 * start on its thread (a buffered writer that flushes on another thread) is not timed.
 *
 * <p>Not covered: the update builder ({@code Query.modify()}), {@code findOneAndUpdate(Map)} and the
 * counting {@code updateOne}/{@code updateMany} fire no storage-listener events in morphium-core
 * (MOR-072), so they do not appear here.
 */
public class MorphiumOperationMetrics extends MorphiumStorageAdapter<Object> {

    static final String TIMER_NAME = "morphium.operations.duration";
    static final String OPERATION_TAG = "operation";
    static final String UPDATE_TYPE_TAG = "update_type";
    static final String ENTITY_TAG = "entity";
    private static final int MAX_PENDING_PER_KIND = 8;

    private final MeterRegistry registry;
    private final Tags baseTags;
    private final Map<String, Timer> timers = new ConcurrentHashMap<>();
    private final ThreadLocal<Map<String, ArrayDeque<Long>>> pending = ThreadLocal.withInitial(HashMap::new);

    MorphiumOperationMetrics(MeterRegistry registry, Tags baseTags) {
        this.registry = registry;
        this.baseTags = baseTags;
    }

    @Override
    public void preUpdate(Morphium m, Class<?> cls, Enum updateType) {
        ArrayDeque<Long> starts = pending.get().computeIfAbsent(key(cls, updateType), k -> new ArrayDeque<>());
        starts.push(System.nanoTime());
        if (starts.size() > MAX_PENDING_PER_KIND) {
            starts.removeLast();
        }
    }

    @Override
    public void postUpdate(Morphium m, Class<?> cls, Enum updateType) {
        ArrayDeque<Long> starts = pending.get().get(key(cls, updateType));
        if (starts == null || starts.isEmpty()) {
            return;
        }
        long elapsed = System.nanoTime() - starts.pop();
        timers.computeIfAbsent(key(cls, updateType), k -> Timer.builder(TIMER_NAME)
                        .description("Duration of Morphium write operations")
                        .tags(baseTags)
                        .tag(OPERATION_TAG, "update")
                        .tag(UPDATE_TYPE_TAG, updateType.name().toLowerCase(Locale.ROOT))
                        .tag(ENTITY_TAG, cls.getSimpleName())
                        .register(registry))
                .record(elapsed, TimeUnit.NANOSECONDS);
    }

    private static String key(Class<?> cls, Enum<?> updateType) {
        return cls.getName() + '/' + updateType.name();
    }

    /** Removes the timers that this listener registered. */
    void close() {
        for (Timer timer : timers.values()) {
            registry.remove((Meter) timer);
        }
        timers.clear();
    }
}
