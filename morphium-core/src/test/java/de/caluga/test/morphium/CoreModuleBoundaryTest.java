package de.caluga.test.morphium;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks selected framework imports and direct extension artifact names in core. This is not a
 * complete dependency graph or a check for fully qualified references and transitive dependencies.
 */
class CoreModuleBoundaryTest {
    private static final Pattern FRAMEWORK_IMPORT =
            Pattern.compile("(?m)^\\s*import\\s+(io\\.quarkus|org\\.springframework|jakarta\\.data)\\.");

    @Test
    void coreSourcesDoNotImportExtensionFrameworks() throws IOException {
        Path sources = Path.of("src/main/java");
        assertThat(sources).as("core module source root").exists();

        try (Stream<Path> files = Files.walk(sources)) {
            for (Path file : files.filter(path -> path.toString().endsWith(".java")).toList()) {
                assertThat(FRAMEWORK_IMPORT.matcher(Files.readString(file)).find())
                        .as("extension framework imports in %s", file)
                        .isFalse();
            }
        }
    }

    @Test
    void corePomDoesNotDependOnExtensionArtifacts() throws IOException {
        String pom = Files.readString(Path.of("pom.xml"));

        assertThat(pom)
                .doesNotContain("<artifactId>quarkus-morphium</artifactId>")
                .doesNotContain("<artifactId>morphium-jakarta-data</artifactId>")
                .doesNotContain("<artifactId>spring-boot-morphium</artifactId>");
    }
}
