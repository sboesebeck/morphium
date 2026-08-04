package de.caluga.poppydb;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Semantic validation rules - every rule one test, errors are collected, not fail-fast. */
public class ConfigInspectorValidateTest {

    private static ServerOptions opts(String... args) {
        return PoppyDBCLI.parse(args, 0);
    }

    @Test
    void defaultsAreValidWithoutWarnings() {
        ConfigInspector.Result r = ConfigInspector.validate(opts());
        assertThat(r.errors()).isEmpty();
        assertThat(r.warnings()).isEmpty();
    }

    @Test
    void portOutOfRangeIsAnError() {
        assertThat(ConfigInspector.validate(opts("--port", "0")).errors())
            .anySatisfy(e -> assertThat(e).contains("port"));
        assertThat(ConfigInspector.validate(opts("--port", "65536")).errors())
            .anySatisfy(e -> assertThat(e).contains("port"));
    }

    @Test
    void memoryWatermarksMustBeOrderedPercentages() {
        assertThat(ConfigInspector.validate(opts("--memory-warn", "0")).errors())
            .anySatisfy(e -> assertThat(e).contains("memory-warn"));
        assertThat(ConfigInspector.validate(opts("--memory-reject", "101")).errors())
            .anySatisfy(e -> assertThat(e).contains("memory-reject"));
        assertThat(ConfigInspector.validate(opts("--memory-warn", "95", "--memory-reject", "80")).errors())
            .anySatisfy(e -> assertThat(e).contains("must not exceed"));
    }

    @Test
    void negativeSizesAndCountsAreErrors() {
        assertThat(ConfigInspector.validate(opts("--max-bson-size", "-1")).errors())
            .anySatisfy(e -> assertThat(e).contains("max-bson-size"));
        assertThat(ConfigInspector.validate(opts("--max-connections", "0")).errors())
            .anySatisfy(e -> assertThat(e).contains("max-connections"));
        assertThat(ConfigInspector.validate(opts("--socket-timeout", "-1")).errors())
            .anySatisfy(e -> assertThat(e).contains("socket-timeout"));
        assertThat(ConfigInspector.validate(opts("--dump-interval", "-5")).errors())
            .anySatisfy(e -> assertThat(e).contains("dump-interval"));
    }

    @Test
    void unknownLogLevelAndCompressorAreErrors() {
        assertThat(ConfigInspector.validate(opts("--log-level", "CHATTY")).errors())
            .anySatisfy(e -> assertThat(e).contains("CHATTY"));
        assertThat(ConfigInspector.validate(opts("--compressor", "gzip")).errors())
            .anySatisfy(e -> assertThat(e).contains("gzip"));
    }

    @Test
    void logLevelOffAndAllAreAcceptedForBackwardCompatibility() {
        // Undocumented (help/error text still only advertise ERROR/WARN/INFO/DEBUG/TRACE), but the
        // pre-refactor CLI accepted anything logback's Level.toLevel understood, so existing
        // --log-level OFF/ALL invocations must keep starting.
        assertThat(ConfigInspector.validate(opts("--log-level", "OFF")).errors()).isEmpty();
        assertThat(ConfigInspector.validate(opts("--log-level", "all")).errors()).isEmpty();
    }

    @Test
    void rootUserAndPasswordOnlyTogether() {
        assertThat(ConfigInspector.validate(opts("--rootUser", "admin")).errors())
            .anySatisfy(e -> assertThat(e).contains("together"));
        assertThat(ConfigInspector.validate(opts("--rootPassword", "s3cret")).errors())
            .anySatisfy(e -> assertThat(e).contains("together"));
        assertThat(ConfigInspector.validate(opts("--rootUser", "admin", "--rootPassword", "s3cret")).errors())
            .isEmpty();
    }

    @Test
    void rsPrioritiesProblemsAreCollectedNotThrown() {
        ConfigInspector.Result r = ConfigInspector.validate(
            opts("--rs-seed", "a:1,b:2", "--rs-priorities", "100"));
        assertThat(r.errors()).anySatisfy(e -> assertThat(e).contains("must match"));

        assertThat(ConfigInspector.validate(opts("--rs-priorities", "100")).errors())
            .anySatisfy(e -> assertThat(e).contains("rs-seed"));
    }

    @Test
    void multipleErrorsAreAllReported() {
        ConfigInspector.Result r = ConfigInspector.validate(
            opts("--port", "0", "--memory-warn", "0", "--rootUser", "admin"));
        assertThat(r.errors()).hasSizeGreaterThanOrEqualTo(3);
    }

    @Test
    void warningsForSuspiciousButLegalConfigs() {
        assertThat(ConfigInspector.validate(opts("--ssl")).warnings())
            .anySatisfy(w -> assertThat(w).contains("ssl-keystore"));
        assertThat(ConfigInspector.validate(opts("--auth")).warnings())
            .anySatisfy(w -> assertThat(w).contains("root-user"));
        assertThat(ConfigInspector.validate(opts("--dump-interval", "60")).warnings())
            .anySatisfy(w -> assertThat(w).contains("dump-dir"));
        assertThat(ConfigInspector.validate(opts("--sslKeystorePassword", "pw")).warnings())
            .anySatisfy(w -> assertThat(w).contains("no effect"));
    }
}
