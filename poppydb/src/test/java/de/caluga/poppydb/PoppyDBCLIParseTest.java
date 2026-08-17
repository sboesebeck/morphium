package de.caluga.poppydb;

import de.caluga.poppydb.config.ConfigException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for the pure argument-parsing seam {@link PoppyDBCLI#parse(String[], int)} and the
 * {@link ServerOptions} helpers. No server is started here - parse() has no side effects.
 */
public class PoppyDBCLIParseTest {

    @Test
    void defaultsWhenNoArgs() {
        ServerOptions opts = PoppyDBCLI.parse(new String[0], 0);
        assertThat(opts.port).isEqualTo(17017);
        assertThat(opts.bind).isEqualTo("localhost");
        assertThat(opts.logLevel).isEqualTo("INFO");
        assertThat(opts.memoryWarnPct).isEqualTo(75);
        assertThat(opts.memoryRejectPct).isEqualTo(90);
        assertThat(opts.maxBsonSizeBytes).isEqualTo(16 * 1024 * 1024);
        assertThat(opts.compressor).isEqualTo("none");
        assertThat(opts.rsName).isEmpty();
        assertThat(opts.rsSeed).isEmpty();
        assertThat(opts.rsPriorities).isEmpty();
        assertThat(opts.ssl).isFalse();
        assertThat(opts.auth).isFalse();
        assertThat(opts.rootUser).isNull();
        assertThat(opts.rootPassword).isNull();
        assertThat(opts.sslKeystore).isNull();
        assertThat(opts.sslKeystorePassword).isNull();
        assertThat(opts.dumpDir).isNull();
        assertThat(opts.dumpIntervalSec).isZero();
        assertThat(opts.maxConnections).isEqualTo(500);
        assertThat(opts.socketTimeoutSec).isEqualTo(300);
        assertThat(opts.usersFile).isNull();
        assertThat(opts.sourceOf("port")).isEqualTo(ServerOptions.Source.DEFAULT);
        assertThat(opts.sourceOf("bind")).isEqualTo(ServerOptions.Source.DEFAULT);
        assertThat(opts.sourceOf("users-file")).isEqualTo(ServerOptions.Source.DEFAULT);
    }

    @Test
    void usersFileCliFlagIsParsedAndTaggedAsCli() {
        ServerOptions opts = PoppyDBCLI.parse(new String[] {"--users-file", "/etc/poppydb/users.json"}, 0);
        assertThat(opts.usersFile).isEqualTo("/etc/poppydb/users.json");
        assertThat(opts.sourceOf("users-file")).isEqualTo(ServerOptions.Source.CLI);
    }

    @Test
    void usersFileCliValueOverridesConfigFileValue() {
        // First 2 tokens simulate config-file origin, the rest is the "real" CLI.
        ServerOptions fromConfig = PoppyDBCLI.parse(new String[] {"--users-file", "/etc/poppydb/users.json"}, 2);
        assertThat(fromConfig.usersFile).isEqualTo("/etc/poppydb/users.json");
        assertThat(fromConfig.sourceOf("users-file")).isEqualTo(ServerOptions.Source.CONFIG_FILE);

        ServerOptions overridden = PoppyDBCLI.parse(new String[] {
            "--users-file", "/etc/poppydb/users.json", "--users-file", "/opt/poppydb/users.json"
        }, 2);
        assertThat(overridden.usersFile).isEqualTo("/opt/poppydb/users.json");
        assertThat(overridden.sourceOf("users-file")).isEqualTo(ServerOptions.Source.CLI);
    }

    @Test
    void cliValuesAreParsedAndTaggedAsCli() {
        ServerOptions opts = PoppyDBCLI.parse(new String[] {
            "--port", "27018", "--bind", "0.0.0.0", "--max-connections", "99",
            "--compressor", "snappy", "--ssl", "--auth", "--dump-interval", "300"
        }, 0);
        assertThat(opts.port).isEqualTo(27018);
        assertThat(opts.bind).isEqualTo("0.0.0.0");
        assertThat(opts.maxConnections).isEqualTo(99);
        assertThat(opts.compressor).isEqualTo("snappy");
        assertThat(opts.ssl).isTrue();
        assertThat(opts.auth).isTrue();
        assertThat(opts.dumpIntervalSec).isEqualTo(300L);
        assertThat(opts.sourceOf("port")).isEqualTo(ServerOptions.Source.CLI);
        assertThat(opts.sourceOf("ssl")).isEqualTo(ServerOptions.Source.CLI);
        assertThat(opts.sourceOf("bind")).isEqualTo(ServerOptions.Source.CLI);
    }

    @Test
    void configTokenBoundarySeparatesConfigFileFromCliSources() {
        // First 2 tokens "come from the config file", the rest from the real command line.
        ServerOptions fromConfig = PoppyDBCLI.parse(new String[] {"--port", "1234"}, 2);
        assertThat(fromConfig.port).isEqualTo(1234);
        assertThat(fromConfig.sourceOf("port")).isEqualTo(ServerOptions.Source.CONFIG_FILE);

        ServerOptions overridden = PoppyDBCLI.parse(new String[] {"--port", "1234", "--port", "5678"}, 2);
        assertThat(overridden.port).isEqualTo(5678);
        assertThat(overridden.sourceOf("port")).isEqualTo(ServerOptions.Source.CLI);
    }

    @Test
    void noSslAfterConfigSslWinsAndIsTaggedCli() {
        ServerOptions opts = PoppyDBCLI.parse(new String[] {"--ssl", "--no-ssl"}, 1);
        assertThat(opts.ssl).isFalse();
        assertThat(opts.sourceOf("ssl")).isEqualTo(ServerOptions.Source.CLI);
    }

    @Test
    void seedHostsGetDefaultPortAndTrimming() {
        ServerOptions opts = PoppyDBCLI.parse(new String[] {"--rs-seed", "alpha, beta:27018"}, 0);
        assertThat(opts.seedHosts()).containsExactly("alpha:27017", "beta:27018");
    }

    @Test
    void seedPrioritiesDefaultTo50() {
        ServerOptions opts = PoppyDBCLI.parse(new String[] {"--rs-seed", "a:1,b:2"}, 0);
        assertThat(opts.seedPriorities()).containsEntry("a:1", 50).containsEntry("b:2", 50);
    }

    @Test
    void explicitSeedPrioritiesAreAssignedInOrder() {
        ServerOptions opts = PoppyDBCLI.parse(
            new String[] {"--rs-seed", "a:1,b:2", "--rs-priorities", "100,0"}, 0);
        assertThat(opts.seedPriorities()).containsEntry("a:1", 100).containsEntry("b:2", 0);
    }

    @Test
    void seedPriorityCountMismatchThrows() {
        ServerOptions opts = PoppyDBCLI.parse(
            new String[] {"--rs-seed", "a:1,b:2", "--rs-priorities", "100"}, 0);
        assertThatThrownBy(opts::seedPriorities)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("must match");
    }

    @Test
    void seedPriorityOutOfRangeThrows() {
        ServerOptions opts = PoppyDBCLI.parse(
            new String[] {"--rs-seed", "a:1", "--rs-priorities", "101"}, 0);
        assertThatThrownBy(opts::seedPriorities)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("between 0 and 100");
    }

    @Test
    void invalidSeedPortThrows() {
        ServerOptions opts = PoppyDBCLI.parse(new String[] {"--rs-seed", "a:xyz"}, 0);
        assertThatThrownBy(opts::seedHosts)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("rs-seed");
    }

    @Test
    void unknownParameterThrowsConfigException() {
        assertThatThrownBy(() -> PoppyDBCLI.parse(new String[] {"--bogus"}, 0))
            .isInstanceOf(ConfigException.class)
            .hasMessageContaining("--bogus");
    }

    @Test
    void missingValueThrowsConfigException() {
        assertThatThrownBy(() -> PoppyDBCLI.parse(new String[] {"--port"}, 0))
            .isInstanceOf(ConfigException.class)
            .hasMessageContaining("--port");
    }

    @Test
    void nonNumericValueThrowsConfigException() {
        assertThatThrownBy(() -> PoppyDBCLI.parse(new String[] {"--port", "abc"}, 0))
            .isInstanceOf(ConfigException.class)
            .hasMessageContaining("abc");
    }

    @Test
    void helpFlagIsToleratedWithoutSideEffects() {
        ServerOptions opts = PoppyDBCLI.parse(new String[] {"--help", "--port", "4711"}, 0);
        assertThat(opts.port).isEqualTo(4711);
    }

    // --- replay-buffer (spec 2026-08-14-replay-buffer-byte-budget.md) ---

    @Test
    void replayBufferDefaultsTo256mAndIsParsedFromCli() {
        ServerOptions defaults = PoppyDBCLI.parse(new String[0], 0);
        assertThat(defaults.replayBuffer).isEqualTo("256m");
        assertThat(defaults.sourceOf("replay-buffer")).isEqualTo(ServerOptions.Source.DEFAULT);

        ServerOptions opts = PoppyDBCLI.parse(new String[] {"--replay-buffer", "5%"}, 0);
        assertThat(opts.replayBuffer).isEqualTo("5%");
        assertThat(opts.sourceOf("replay-buffer")).isEqualTo(ServerOptions.Source.CLI);
    }

    @Test
    void replayBufferSizesResolveFixedAndPercent() {
        long heap = 1024L * 1024 * 1024; // pretend 1 GB max heap
        assertThat(ServerOptions.parseReplayBufferBytes("256m", heap)).isEqualTo(256L * 1024 * 1024);
        assertThat(ServerOptions.parseReplayBufferBytes("1g", heap)).isEqualTo(1024L * 1024 * 1024);
        assertThat(ServerOptions.parseReplayBufferBytes("64k", heap)).isEqualTo(64L * 1024);
        assertThat(ServerOptions.parseReplayBufferBytes("12345", heap)).isEqualTo(12345L);
        assertThat(ServerOptions.parseReplayBufferBytes("5%", heap)).isEqualTo(heap / 20);
        assertThat(ServerOptions.parseReplayBufferBytes("0", heap)).isZero();
        assertThat(ServerOptions.parseReplayBufferBytes(" 1G ", heap)).isEqualTo(1024L * 1024 * 1024);
    }

    @Test
    void replayBufferInvalidValuesAreRejected() {
        long heap = 1024L * 1024 * 1024;
        assertThatThrownBy(() -> ServerOptions.parseReplayBufferBytes("abc", heap))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("abc");
        assertThatThrownBy(() -> ServerOptions.parseReplayBufferBytes("150%", heap))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("150%");
        assertThatThrownBy(() -> ServerOptions.parseReplayBufferBytes("-5m", heap))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ServerOptions.parseReplayBufferBytes("", heap))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void replayBufferInvalidValueIsReportedByValidate() {
        ServerOptions opts = PoppyDBCLI.parse(new String[] {"--replay-buffer", "lots"}, 0);
        ConfigInspector.Result result = ConfigInspector.validate(opts);
        assertThat(result.errors()).anyMatch(e -> e.contains("lots"));
    }

    // --- event-queue-budget (byte backpressure for the secondary's replication event queue) ---

    @Test
    void eventQueueBudgetDefaultsTo256mAndIsParsedFromCli() {
        ServerOptions defaults = PoppyDBCLI.parse(new String[0], 0);
        assertThat(defaults.eventQueueBudget).isEqualTo("256m");
        assertThat(defaults.sourceOf("event-queue-budget")).isEqualTo(ServerOptions.Source.DEFAULT);

        ServerOptions opts = PoppyDBCLI.parse(new String[] {"--event-queue-budget", "64m"}, 0);
        assertThat(opts.eventQueueBudget).isEqualTo("64m");
        assertThat(opts.sourceOf("event-queue-budget")).isEqualTo(ServerOptions.Source.CLI);
    }

    @Test
    void eventQueueBudgetUsesTheSharedSizeParser() {
        long heap = 1024L * 1024 * 1024; // pretend 1 GB max heap
        assertThat(ServerOptions.parseByteSize("event-queue-budget", "64m", heap)).isEqualTo(64L * 1024 * 1024);
        assertThat(ServerOptions.parseByteSize("event-queue-budget", "5%", heap)).isEqualTo(heap / 20);
        assertThat(ServerOptions.parseByteSize("event-queue-budget", "0", heap)).isZero();
        assertThatThrownBy(() -> ServerOptions.parseByteSize("event-queue-budget", "lots", heap))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("event-queue-budget")
            .hasMessageContaining("lots");
    }

    @Test
    void eventQueueBudgetInvalidValueIsReportedByValidate() {
        ServerOptions opts = PoppyDBCLI.parse(new String[] {"--event-queue-budget", "plenty"}, 0);
        ConfigInspector.Result result = ConfigInspector.validate(opts);
        assertThat(result.errors()).anyMatch(e -> e.contains("plenty"));
    }
}
