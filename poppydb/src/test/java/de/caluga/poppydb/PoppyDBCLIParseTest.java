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
        assertThat(opts.sourceOf("port")).isEqualTo(ServerOptions.Source.DEFAULT);
        assertThat(opts.sourceOf("bind")).isEqualTo(ServerOptions.Source.DEFAULT);
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
}
