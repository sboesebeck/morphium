package de.caluga.morphium.driver;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code ok} field of a command reply is not a typed contract: MongoDB writes it as a double
 * on most paths, as int32 on others, and the server-compatible backends Morphium talks to do the
 * same. Comparing it literally therefore read an integer {@code 0} as success (a swallowed command
 * error, e.g. in {@code MorphiumWriterImpl}) and an integer {@code 1} as failure (a bogus
 * "Error: null - null" on every read, from {@code SingleMongoConnection.checkForError}).
 * {@link CommandStatus} is the single place that normalises this - these tests pin down which
 * encodings count as what, so a future "simplification" back to {@code equals(1.0)} fails here.
 */
@Tag("core")
public class CommandStatusTest {

    private static Map<String, Object> reply(Object ok) {
        Map<String, Object> m = new HashMap<>();
        m.put("ok", ok);
        return m;
    }

    @Test
    public void doubleOneIsOkAndDoubleZeroIsFailure() {
        assertThat(CommandStatus.of(reply(1.0))).isEqualTo(CommandStatus.OK);
        assertThat(CommandStatus.of(reply(0.0))).isEqualTo(CommandStatus.FAILURE);
    }

    @Test
    public void integerOneIsOkAndIntegerZeroIsFailure() {
        assertThat(CommandStatus.of(reply(1))).isEqualTo(CommandStatus.OK);
        assertThat(CommandStatus.of(reply(0))).isEqualTo(CommandStatus.FAILURE);
    }

    @Test
    public void anyNumericRepresentationOfOneAndZeroCounts() {
        for (Object ok : new Object[] {1L, 0L, (short) 1, (short) 0, (byte) 1, (byte) 0, 1.0f, 0.0f,
                                       BigDecimal.ONE, BigDecimal.ZERO}) {
            CommandStatus expected = ((Number) ok).doubleValue() == 1.0 ? CommandStatus.OK : CommandStatus.FAILURE;
            assertThat(CommandStatus.of(reply(ok))).as("ok=%s (%s)", ok, ok.getClass().getSimpleName())
                    .isEqualTo(expected);
        }
    }

    @Test
    public void wrongTypesAreUnknownNotSilentSuccessOrFailure() {
        assertThat(CommandStatus.of(reply("1"))).isEqualTo(CommandStatus.UNKNOWN);
        assertThat(CommandStatus.of(reply("0"))).isEqualTo(CommandStatus.UNKNOWN);
        assertThat(CommandStatus.of(reply(true))).isEqualTo(CommandStatus.UNKNOWN);
        assertThat(CommandStatus.of(reply(Boolean.FALSE))).isEqualTo(CommandStatus.UNKNOWN);
    }

    @Test
    public void numbersThatAreNeitherOneNorZeroAreUnknown() {
        // ok is documented as 0 or 1 - anything else is a reply whose status we cannot judge
        assertThat(CommandStatus.of(reply(2))).isEqualTo(CommandStatus.UNKNOWN);
        assertThat(CommandStatus.of(reply(-1))).isEqualTo(CommandStatus.UNKNOWN);
        assertThat(CommandStatus.of(reply(Double.NaN))).isEqualTo(CommandStatus.UNKNOWN);
        assertThat(CommandStatus.of(reply(0.5))).isEqualTo(CommandStatus.UNKNOWN);
    }

    @Test
    public void missingOkAndMissingReplyAreUnknown() {
        assertThat(CommandStatus.of(new HashMap<String, Object>())).isEqualTo(CommandStatus.UNKNOWN);
        assertThat(CommandStatus.of((Map<String, Object>) null)).isEqualTo(CommandStatus.UNKNOWN);
        assertThat(CommandStatus.of((Object) null)).isEqualTo(CommandStatus.UNKNOWN);
    }

    @Test
    public void conveniencePredicatesMatchTheVerdict() {
        assertThat(CommandStatus.OK.isOk()).isTrue();
        assertThat(CommandStatus.OK.isFailure()).isFalse();
        assertThat(CommandStatus.FAILURE.isFailure()).isTrue();
        assertThat(CommandStatus.FAILURE.isOk()).isFalse();
        // neither predicate claims an inconclusive reply - the call site has to decide
        assertThat(CommandStatus.UNKNOWN.isOk()).isFalse();
        assertThat(CommandStatus.UNKNOWN.isFailure()).isFalse();
    }
}
