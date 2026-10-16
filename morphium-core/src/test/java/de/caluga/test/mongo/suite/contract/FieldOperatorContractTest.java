package de.caluga.test.mongo.suite.contract;

import de.caluga.morphium.Morphium;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import de.caluga.test.mongo.suite.contract.UpdateContract.Case;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;

import static de.caluga.test.mongo.suite.contract.UpdateContract.Result.docs;
import static de.caluga.test.mongo.suite.contract.UpdateContract.Result.error;
import static de.caluga.test.mongo.suite.contract.UpdateContract.Result.notADatabaseError;
import static de.caluga.test.mongo.suite.contract.UpdateContract.c;
import static de.caluga.test.mongo.suite.contract.UpdateContract.d;
import static de.caluga.test.mongo.suite.contract.UpdateContract.l;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** MOR-028: the field operators ($set, $unset, $inc, $mul, $min, $max, $rename) on every driver, with the results of a real MongoDB. */
@Tag("core")
public class FieldOperatorContractTest extends MultiDriverTestBase {

    static List<Case> setAndUnset() {
        Case[] cases = {
            c("set adds a field").seed(d("_id", 1, "a", 1)).filter(d("_id", 1)).update(d("$set", d("b", 2))).expect(docs(d("_id", 1, "a", 1, "b", 2))),
            c("set creates the parents of a dotted path").seed(d("_id", 1)).filter(d("_id", 1)).update(d("$set", d("a.b.c", 1))).expect(docs(d("_id", 1, "a", d("b", d("c", 1))))),
            c("set changes the type").seed(d("_id", 1, "a", 1)).filter(d("_id", 1)).update(d("$set", d("a", "x"))).expect(docs(d("_id", 1, "a", "x"))),
            c("set of a document replaces the old one").seed(d("_id", 1, "a", d("x", 1, "y", 2))).filter(d("_id", 1)).update(d("$set", d("a", d("z", 3)))).expect(docs(d("_id", 1, "a", d("z", 3)))),
            c("set of a dotted path keeps the siblings").seed(d("_id", 1, "a", d("x", 1, "y", 2))).filter(d("_id", 1)).update(d("$set", d("a.y", 5))).expect(docs(d("_id", 1, "a", d("x", 1, "y", 5)))),
            c("set null keeps the field").seed(d("_id", 1, "a", 1)).filter(d("_id", 1)).update(d("$set", d("a", null))).expect(docs(d("_id", 1, "a", null))),
            c("set of an array element").seed(d("_id", 1, "a", l(1, 2, 3))).filter(d("_id", 1)).update(d("$set", d("a.1", 9))).expect(docs(d("_id", 1, "a", l(1, 9, 3)))),
            c("set beyond the end of an array pads with null").seed(d("_id", 1, "a", l(1))).filter(d("_id", 1)).update(d("$set", d("a.3", 5))).expect(docs(d("_id", 1, "a", l(1, null, null, 5)))),
            c("set with a numeric segment on a missing path creates a document").seed(d("_id", 1)).filter(d("_id", 1)).update(d("$set", d("a.0", 1))).expect(docs(d("_id", 1, "a", d("0", 1)))),
            c("set of an array").seed(d("_id", 1)).filter(d("_id", 1)).update(d("$set", d("a", l(1, "x", d("k", 1))))).expect(docs(d("_id", 1, "a", l(1, "x", d("k", 1))))),
            c("set with the same value changes nothing").seed(d("_id", 1, "a", 1)).filter(d("_id", 1)).update(d("$set", d("a", 1))).expect(docs(d("_id", 1, "a", 1))),
            c("set of the unchanged _id is allowed").seed(d("_id", 1, "a", 1)).filter(d("_id", 1)).update(d("$set", d("_id", 1))).expect(docs(d("_id", 1, "a", 1))),
            c("set updates one document without multi").seed(d("_id", 1, "k", 1), d("_id", 2, "k", 1)).filter(d("k", 1)).update(d("$set", d("v", 1))).expect(docs(d("_id", 1, "k", 1, "v", 1), d("_id", 2, "k", 1))),
            c("set updates all documents with multi").seed(d("_id", 1, "k", 1), d("_id", 2, "k", 1), d("_id", 3, "k", 2)).filter(d("k", 1)).update(d("$set", d("v", 1))).multi()
            .expect(docs(d("_id", 1, "k", 1, "v", 1), d("_id", 2, "k", 1, "v", 1), d("_id", 3, "k", 2))),
            c("no match changes nothing").seed(d("_id", 1, "a", 1)).filter(d("_id", 2)).update(d("$set", d("a", 9))).expect(docs(d("_id", 1, "a", 1))),

            c("unset removes the field").seed(d("_id", 1, "a", 1, "b", 2)).filter(d("_id", 1)).update(d("$unset", d("a", ""))).expect(docs(d("_id", 1, "b", 2))),
            c("unset of a missing field is a no-op").seed(d("_id", 1, "a", 1)).filter(d("_id", 1)).update(d("$unset", d("zz", ""))).expect(docs(d("_id", 1, "a", 1))),
            c("unset of a nested field").seed(d("_id", 1, "a", d("x", 1, "y", 2))).filter(d("_id", 1)).update(d("$unset", d("a.x", ""))).expect(docs(d("_id", 1, "a", d("y", 2)))),
            c("unset of an array element sets null").seed(d("_id", 1, "a", l(1, 2, 3))).filter(d("_id", 1)).update(d("$unset", d("a.1", ""))).expect(docs(d("_id", 1, "a", l(1, null, 3)))),
            c("unset ignores the value").seed(d("_id", 1, "a", 1)).filter(d("_id", 1)).update(d("$unset", d("a", 1))).expect(docs(d("_id", 1))),
        };
        return new ArrayList<>(List.of(cases));
    }

    static List<Case> incAndMul() {
        Case[] cases = {
            c("inc of an int by an int stays an int").seed(d("_id", 1, "a", 1)).filter(d("_id", 1)).update(d("$inc", d("a", 2))).expect(docs(d("_id", 1, "a", 3))),
            c("inc creates a missing field").seed(d("_id", 1)).filter(d("_id", 1)).update(d("$inc", d("a", 5))).expect(docs(d("_id", 1, "a", 5))),
            c("inc by a negative number").seed(d("_id", 1, "a", 5)).filter(d("_id", 1)).update(d("$inc", d("a", -7))).expect(docs(d("_id", 1, "a", -2))),
            c("inc of an int by a double is a double").seed(d("_id", 1, "a", 1)).filter(d("_id", 1)).update(d("$inc", d("a", 0.5))).expect(docs(d("_id", 1, "a", 1.5))),
            c("inc of an int by a long is a long").seed(d("_id", 1, "a", 1)).filter(d("_id", 1)).update(d("$inc", d("a", 2L))).expect(docs(d("_id", 1, "a", 3L))),
            c("inc of a long by an int is a long").seed(d("_id", 1, "a", 1L)).filter(d("_id", 1)).update(d("$inc", d("a", 2))).expect(docs(d("_id", 1, "a", 3L))),
            c("inc of an int that overflows becomes a long").seed(d("_id", 1, "a", Integer.MAX_VALUE)).filter(d("_id", 1)).update(d("$inc", d("a", 1))).expect(docs(d("_id", 1, "a", 2147483648L)))
            .inMem("MOR-055", docs(d("_id", 1, "a", Integer.MIN_VALUE))),
            c("inc of a double by an int is a double").seed(d("_id", 1, "a", 1.5)).filter(d("_id", 1)).update(d("$inc", d("a", 1))).expect(docs(d("_id", 1, "a", 2.5))),
            c("inc of a missing nested path creates the parents").seed(d("_id", 1)).filter(d("_id", 1)).update(d("$inc", d("a.b", 1))).expect(docs(d("_id", 1, "a", d("b", 1)))),
            c("inc of a string fails").seed(d("_id", 1, "a", "x")).filter(d("_id", 1)).update(d("$inc", d("a", 1))).expect(error(14)).unchanged(),
            c("inc of null fails").seed(d("_id", 1, "a", null)).filter(d("_id", 1)).update(d("$inc", d("a", 1))).expect(error(14)).unchanged(),
            c("inc by a string fails").seed(d("_id", 1, "a", 1)).filter(d("_id", 1)).update(d("$inc", d("a", "x"))).expect(error(14)).unchanged(),
            c("inc by a string fails even if nothing matches").filter(d("_id", 1)).update(d("$inc", d("a", "x"))).expect(error(14)).unchanged(),
            c("mul of null fails").seed(d("_id", 1, "a", null)).filter(d("_id", 1)).update(d("$mul", d("a", 2))).expect(error(14)).unchanged(),
            c("mul by a string fails").seed(d("_id", 1, "a", 1)).filter(d("_id", 1)).update(d("$mul", d("a", "x"))).expect(error(14)).unchanged(),
            c("mul of an int by an int").seed(d("_id", 1, "a", 3)).filter(d("_id", 1)).update(d("$mul", d("a", 2))).expect(docs(d("_id", 1, "a", 6))),
            c("mul of a missing field creates zero of the type of the factor").seed(d("_id", 1)).filter(d("_id", 1)).update(d("$mul", d("a", 5))).expect(docs(d("_id", 1, "a", 0))),
            c("mul of an int by a double is a double").seed(d("_id", 1, "a", 2)).filter(d("_id", 1)).update(d("$mul", d("a", 1.5))).expect(docs(d("_id", 1, "a", 3.0)))
            .inMem("MOR-055", notADatabaseError()),
            c("mul of a string fails").seed(d("_id", 1, "a", "x")).filter(d("_id", 1)).update(d("$mul", d("a", 2))).expect(error(14)).unchanged(),
        };
        return new ArrayList<>(List.of(cases));
    }

    static List<Case> minMaxRename() {
        Case[] cases = {
            c("min takes the smaller value").seed(d("_id", 1, "a", 5)).filter(d("_id", 1)).update(d("$min", d("a", 3))).expect(docs(d("_id", 1, "a", 3))),
            c("min keeps the smaller existing value").seed(d("_id", 1, "a", 5)).filter(d("_id", 1)).update(d("$min", d("a", 7))).expect(docs(d("_id", 1, "a", 5))),
            c("min sets a missing field").seed(d("_id", 1)).filter(d("_id", 1)).update(d("$min", d("a", 7))).expect(docs(d("_id", 1, "a", 7))),
            c("max takes the larger value").seed(d("_id", 1, "a", 5)).filter(d("_id", 1)).update(d("$max", d("a", 9))).expect(docs(d("_id", 1, "a", 9))),
            c("max keeps the larger existing value").seed(d("_id", 1, "a", 5)).filter(d("_id", 1)).update(d("$max", d("a", 2))).expect(docs(d("_id", 1, "a", 5))),
            c("max sets a missing field").seed(d("_id", 1)).filter(d("_id", 1)).update(d("$max", d("a", 2))).expect(docs(d("_id", 1, "a", 2))),
            c("max compares across types in BSON order (string above number)").seed(d("_id", 1, "a", 5)).filter(d("_id", 1)).update(d("$max", d("a", "x"))).expect(docs(d("_id", 1, "a", "x")))
            .inMem("MOR-055", notADatabaseError()),
            c("min compares across types in BSON order (number below string)").seed(d("_id", 1, "a", 5)).filter(d("_id", 1)).update(d("$min", d("a", "x"))).expect(docs(d("_id", 1, "a", 5)))
            .inMem("MOR-055", notADatabaseError()),
            c("max of an equal value of another number type keeps the old value").seed(d("_id", 1, "a", 5)).filter(d("_id", 1)).update(d("$max", d("a", 5.0))).expect(docs(d("_id", 1, "a", 5)))
            .inMem("MOR-055", notADatabaseError()),
            c("rename moves a field").seed(d("_id", 1, "a", 1)).filter(d("_id", 1)).update(d("$rename", d("a", "b"))).expect(docs(d("_id", 1, "b", 1))),
            c("rename of a missing field is a no-op").seed(d("_id", 1, "a", 1)).filter(d("_id", 1)).update(d("$rename", d("zz", "b"))).expect(docs(d("_id", 1, "a", 1))),
            c("rename overwrites an existing target").seed(d("_id", 1, "a", 1, "b", 2)).filter(d("_id", 1)).update(d("$rename", d("a", "b"))).expect(docs(d("_id", 1, "b", 1))),
            c("rename into a nested path").seed(d("_id", 1, "a", 1)).filter(d("_id", 1)).update(d("$rename", d("a", "x.y"))).expect(docs(d("_id", 1, "x", d("y", 1)))),
            c("rename out of a nested path").seed(d("_id", 1, "x", d("y", 1))).filter(d("_id", 1)).update(d("$rename", d("x.y", "a"))).expect(docs(d("_id", 1, "x", d(), "a", 1))),
            c("rename to the same name fails").seed(d("_id", 1, "a", 1)).filter(d("_id", 1)).update(d("$rename", d("a", "a"))).expect(error(2)).unchanged(),
            c("rename with a non-string target fails").seed(d("_id", 1, "a", 1)).filter(d("_id", 1)).update(d("$rename", d("a", 5))).expect(error(2)).unchanged(),
        };
        return new ArrayList<>(List.of(cases));
    }

    private void check(Morphium m, List<Case> cases) throws Exception {
        List<String> differences = new ArrayList<>();

        for (Case c : cases) {
            for (UpdateContract.Path path : UpdateContract.Path.values()) {
                if (path == UpdateContract.Path.FIND_AND_MODIFY && c.multi) {
                    continue; // findAndModify updates one document
                }

                String diff = UpdateContract.run(m, c, path);

                if (diff != null) {
                    differences.add(diff);
                }
            }
        }

        assertTrue(differences.isEmpty(), "\n" + String.join("\n", differences));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void setAndUnset(Morphium m) throws Exception {
        try (m) {
            check(m, setAndUnset());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void incAndMul(Morphium m) throws Exception {
        try (m) {
            check(m, incAndMul());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void minMaxRename(Morphium m) throws Exception {
        try (m) {
            check(m, minMaxRename());
        }
    }
}
