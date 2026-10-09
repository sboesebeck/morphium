package de.caluga.poppydb.netty;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/** The settings record rejects what the gate could not run with, at construction, naming the rule. */
public class ReplicationFlowControlSettingsTest {

    @Test
    void defaultsAreOnFiftyTwentyFiveTenSeconds() {
        ReplicationFlowControlSettings d = ReplicationFlowControlSettings.defaults();
        assertThat(d.enabled()).isTrue();
        assertThat(d.highWaterPercent()).isEqualTo(50);
        assertThat(d.lowWaterPercent()).isEqualTo(25);
        assertThat(d.maxWaitMs()).isEqualTo(10_000L);
    }

    @Test
    void waterMarksMustBeOrderedPercentages() {
        assertThatThrownBy(() -> new ReplicationFlowControlSettings(true, 101, 25, 1000))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("high-water");
        assertThatThrownBy(() -> new ReplicationFlowControlSettings(true, 50, 0, 1000))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("low-water");
        assertThatThrownBy(() -> new ReplicationFlowControlSettings(true, 30, 30, 1000))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("below high-water");
        assertThatThrownBy(() -> new ReplicationFlowControlSettings(true, 20, 40, 1000))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("below high-water");
    }

    @Test
    void maxWaitMustBePositive() {
        assertThatThrownBy(() -> new ReplicationFlowControlSettings(true, 50, 25, 0))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("max-wait");
        assertThatThrownBy(() -> new ReplicationFlowControlSettings(true, 50, 25, -1))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("max-wait");
    }

    @Test
    void aDisabledGateStillValidatesItsNumbers() {
        assertThatThrownBy(() -> new ReplicationFlowControlSettings(false, 10, 20, 1000))
            .isInstanceOf(IllegalArgumentException.class);
        assertThat(new ReplicationFlowControlSettings(false, 60, 30, 1).enabled()).isFalse();
    }
}
