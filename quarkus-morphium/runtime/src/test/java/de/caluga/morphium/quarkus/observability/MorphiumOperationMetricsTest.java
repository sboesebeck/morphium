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
import de.caluga.morphium.MorphiumStorageListener;
import de.caluga.morphium.MorphiumStorageListener.UpdateTypes;
import de.caluga.morphium.driver.MorphiumDriver;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DisplayName("MorphiumOperationMetrics: timer of the update operations")
class MorphiumOperationMetricsTest {

    static class Order {
    }

    private SimpleMeterRegistry registry;
    private MorphiumOperationMetrics metrics;
    private Morphium morphium;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new MorphiumOperationMetrics(registry, Tags.of("database", "db1"));
        morphium = mock(Morphium.class);
    }

    private Timer timer(UpdateTypes type) {
        return registry.find("morphium.operations.duration").tag("update_type", type.name().toLowerCase()).timer();
    }

    @Test
    @DisplayName("a pre/post pair records one timing with the tags of the operation")
    void aPairIsRecorded() {
        metrics.preUpdate(morphium, Order.class, UpdateTypes.INC);
        metrics.postUpdate(morphium, Order.class, UpdateTypes.INC);

        Timer t = timer(UpdateTypes.INC);
        assertThat(t).isNotNull();
        assertThat(t.count()).isEqualTo(1);
        assertThat(t.getId().getTag("database")).isEqualTo("db1");
        assertThat(t.getId().getTag("operation")).isEqualTo("update");
        assertThat(t.getId().getTag("entity")).isEqualTo("Order");
    }

    @Test
    @DisplayName("each update type has its own timer")
    void typesAreSeparate() {
        for (UpdateTypes type : new UpdateTypes[] {UpdateTypes.SET, UpdateTypes.PUSH}) {
            metrics.preUpdate(morphium, Order.class, type);
            metrics.postUpdate(morphium, Order.class, type);
        }
        assertThat(timer(UpdateTypes.SET).count()).isEqualTo(1);
        assertThat(timer(UpdateTypes.PUSH).count()).isEqualTo(1);
    }

    @Test
    @DisplayName("a write that failed (no post) does not distort the next successful one")
    void aFailedWriteIsNotPaired() throws Exception {
        metrics.preUpdate(morphium, Order.class, UpdateTypes.SET);          // fails, its post never comes
        Thread.sleep(300);
        metrics.preUpdate(morphium, Order.class, UpdateTypes.SET);
        metrics.postUpdate(morphium, Order.class, UpdateTypes.SET);

        assertThat(timer(UpdateTypes.SET).count()).isEqualTo(1);
        assertThat(timer(UpdateTypes.SET).totalTime(TimeUnit.MILLISECONDS)).isLessThan(250);
    }

    @Test
    @DisplayName("a post without a start on its thread is not timed")
    void anUnpairedPostIsIgnored() throws Exception {
        Thread t = new Thread(() -> metrics.preUpdate(morphium, Order.class, UpdateTypes.SET));
        t.start();
        t.join();
        metrics.postUpdate(morphium, Order.class, UpdateTypes.SET);

        assertThat(timer(UpdateTypes.SET)).isNull();
    }

    @Test
    @DisplayName("starts that pile up on a thread (failed writes) are bounded")
    void pendingStartsAreBounded() {
        for (int i = 0; i < 1000; i++) {
            metrics.preUpdate(morphium, Order.class, UpdateTypes.SET);
        }
        for (int i = 0; i < 1000; i++) {
            metrics.postUpdate(morphium, Order.class, UpdateTypes.SET);
        }
        assertThat(timer(UpdateTypes.SET).count()).isPositive().isLessThanOrEqualTo(8);
    }

    @Test
    @DisplayName("close removes the timers")
    void closeRemovesTheTimers() {
        metrics.preUpdate(morphium, Order.class, UpdateTypes.SET);
        metrics.postUpdate(morphium, Order.class, UpdateTypes.SET);
        assertThat(timer(UpdateTypes.SET)).isNotNull();

        metrics.close();

        assertThat(timer(UpdateTypes.SET)).isNull();
    }

    @Test
    @DisplayName("the binder adds the listener and removes it on close; the switch keeps it away")
    void theBinderManagesTheListener() {
        MorphiumDriver driver = mock(MorphiumDriver.class);
        when(driver.getDriverStats()).thenReturn(new HashMap<>());
        when(morphium.getDriver()).thenReturn(driver);
        when(morphium.getStatistics()).thenReturn(new HashMap<>());

        MorphiumMetricsBinder binder = new MorphiumMetricsBinder();
        binder.registry = registry;

        binder.bindTo(morphium, "db1", false);
        verify(morphium, never()).addListener(any());
        binder.close();

        binder.bindTo(morphium, "db1", true);
        ArgumentCaptor<MorphiumStorageListener<?>> added = ArgumentCaptor.forClass(MorphiumStorageListener.class);
        verify(morphium).addListener(added.capture());
        assertThat(added.getValue()).isInstanceOf(MorphiumOperationMetrics.class);

        binder.close();
        verify(morphium).removeListener(added.getValue());
    }
}
