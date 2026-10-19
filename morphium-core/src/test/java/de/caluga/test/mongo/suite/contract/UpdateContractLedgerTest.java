package de.caluga.test.mongo.suite.contract;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * MOR-028: every deviation that the update contract suite pins ({@code .inMem("MOR-nnn", ...)} or a comment {@code // pinned deviation MOR-nnn}) must name a task of the ledger that is still open. A task that
 * is done removes the deviation: the pin has to go (and the case then runs against the same expectation on every driver). This keeps deviations tracked,
 * not hidden.
 */
@Tag("core")
public class UpdateContractLedgerTest {
    private static final Pattern PIN = Pattern.compile("(?:\\.inMem\\w*\\(\"|//\\s*pinned deviation )(MOR-\\d+)");
    private static final Path LEDGER = Path.of("..", "docs", "tasks", "ledger.yaml");
    private static final Path SOURCES = Path.of("src", "test", "java", "de", "caluga", "test", "mongo", "suite", "contract");

    /** the ledger tasks named by the pins of a source text */
    static TreeSet<String> pinsIn(String source) {
        TreeSet<String> tasks = new TreeSet<>();
        Matcher m = PIN.matcher(source);

        while (m.find()) {
            tasks.add(m.group(1));
        }

        return tasks;
    }

    private static TreeSet<String> pinnedTasks() throws IOException {
        TreeSet<String> tasks = new TreeSet<>();

        try (Stream<Path> files = Files.list(SOURCES)) {
            for (Path f : files.filter(p -> p.getFileName().toString().endsWith("ContractTest.java")).toList()) {
                tasks.addAll(pinsIn(Files.readString(f)));
            }
        }

        return tasks;
    }

    /** the status of a task in the ledger, or null if there is no such task */
    static String status(String ledger, String id) {
        Matcher m = Pattern.compile("(?m)^- id: " + Pattern.quote(id) + "\\n(?:(?!- id: ).*\\n)*?  status: (\\w+)").matcher(ledger);
        return m.find() ? m.group(1) : null;
    }

    @Test
    public void everyPinnedDeviationNamesAnOpenLedgerTask() throws IOException {
        assumeTrue(Files.exists(LEDGER) && Files.isDirectory(SOURCES), "run from the morphium-core module of a full checkout");
        String ledger = Files.readString(LEDGER);
        TreeSet<String> tasks = pinnedTasks();
        // no pin at all is the goal: the InMemoryDriver answers every case like a real MongoDB; a new deviation is pinned again with its task
        List<String> problems = new ArrayList<>();

        for (String id : tasks) {
            String status = status(ledger, id);

            if (status == null) {
                problems.add(id + " is not in the ledger");
            } else if (status.equals("done")) {
                problems.add(id + " is done: remove its pinned deviations from the contract suite");
            }
        }

        assertTrue(problems.isEmpty(), String.join("\n", problems));
    }

    @Test
    public void theStatusLookupFindsTheStatusOfATask() {
        String ledger = "- id: MOR-001\n  title: a\n  status: done\n  priority: 1\n- id: MOR-002\n  title: b\n  status: open\n";
        assertEquals("done", status(ledger, "MOR-001"));
        assertEquals("open", status(ledger, "MOR-002"));
        assertEquals(null, status(ledger, "MOR-003"));
    }

    @Test
    public void thePinPatternFindsBothKindsOfPin() {
        assertEquals(new TreeSet<>(List.of("MOR-101", "MOR-102")), pinsIn("c(\"x\").expect(a)\n.inMem(\"MOR-101\", r),\n// pinned deviation MOR-102: text\n.inMemFindAndModify(\"MOR-101\", r)"));
        assertTrue(pinsIn("no pins here, MOR-103 only mentioned").isEmpty());
    }
}
