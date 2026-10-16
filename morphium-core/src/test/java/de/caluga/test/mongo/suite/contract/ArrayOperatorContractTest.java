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

/** MOR-028: the array operators and the positional paths on every driver, with the results of a real MongoDB. */
@Tag("core")
public class ArrayOperatorContractTest extends MultiDriverTestBase {

    static List<Case> pushAndAddToSet() {
        Case[] cases = {
            c("push appends").seed(d("_id", 1, "a", l(1, 2))).filter(d("_id", 1)).update(d("$push", d("a", 3))).expect(docs(d("_id", 1, "a", l(1, 2, 3)))),
            c("push creates a missing array").seed(d("_id", 1)).filter(d("_id", 1)).update(d("$push", d("a", 1))).expect(docs(d("_id", 1, "a", l(1)))),
            c("push allows duplicates").seed(d("_id", 1, "a", l(1))).filter(d("_id", 1)).update(d("$push", d("a", 1))).expect(docs(d("_id", 1, "a", l(1, 1)))),
            c("push of a document").seed(d("_id", 1, "a", l())).filter(d("_id", 1)).update(d("$push", d("a", d("k", 1)))).expect(docs(d("_id", 1, "a", l(d("k", 1))))),
            c("push of an array adds it as one element").seed(d("_id", 1, "a", l(1))).filter(d("_id", 1)).update(d("$push", d("a", l(2, 3)))).expect(docs(d("_id", 1, "a", l(1, l(2, 3))))),
            c("push with each").seed(d("_id", 1, "a", l(1))).filter(d("_id", 1)).update(d("$push", d("a", d("$each", l(2, 3))))).expect(docs(d("_id", 1, "a", l(1, 2, 3)))),
            c("push with each and position").seed(d("_id", 1, "a", l(1, 2))).filter(d("_id", 1)).update(d("$push", d("a", d("$each", l(8, 9), "$position", 1)))).expect(docs(d("_id", 1, "a", l(1, 8, 9, 2)))),
            c("push with each and slice keeps the last elements").seed(d("_id", 1, "a", l(1, 2))).filter(d("_id", 1)).update(d("$push", d("a", d("$each", l(3, 4), "$slice", -3)))).expect(docs(d("_id", 1, "a", l(2, 3, 4)))),
            c("push with each and sort").seed(d("_id", 1, "a", l(3, 1))).filter(d("_id", 1)).update(d("$push", d("a", d("$each", l(2), "$sort", 1)))).expect(docs(d("_id", 1, "a", l(1, 2, 3)))),
            c("push to a nested array").seed(d("_id", 1, "x", d("a", l(1)))).filter(d("_id", 1)).update(d("$push", d("x.a", 2))).expect(docs(d("_id", 1, "x", d("a", l(1, 2))))),
            c("push to a field that is not an array fails").seed(d("_id", 1, "a", 1)).filter(d("_id", 1)).update(d("$push", d("a", 2))).expect(error(2)).unchanged(),
            c("addToSet adds a new value").seed(d("_id", 1, "a", l(1, 2))).filter(d("_id", 1)).update(d("$addToSet", d("a", 3))).expect(docs(d("_id", 1, "a", l(1, 2, 3)))),
            c("addToSet ignores an existing value").seed(d("_id", 1, "a", l(1, 2))).filter(d("_id", 1)).update(d("$addToSet", d("a", 2))).expect(docs(d("_id", 1, "a", l(1, 2)))),
            c("push to a field that is null fails").seed(d("_id", 1, "a", null)).filter(d("_id", 1)).update(d("$push", d("a", 2))).expect(error(2)).unchanged(),
            c("push with an each that is not an array fails").seed(d("_id", 1, "a", l())).filter(d("_id", 1)).update(d("$push", d("a", d("$each", 5)))).expect(error(2)).unchanged(),
            c("addToSet creates a missing array").seed(d("_id", 1)).filter(d("_id", 1)).update(d("$addToSet", d("a", 1))).expect(docs(d("_id", 1, "a", l(1)))),
            c("addToSet with each skips duplicates").seed(d("_id", 1, "a", l(1))).filter(d("_id", 1)).update(d("$addToSet", d("a", d("$each", l(1, 2, 2, 3))))).expect(docs(d("_id", 1, "a", l(1, 2, 3)))),
            c("addToSet treats 1 and 1.0 as equal").seed(d("_id", 1, "a", l(1))).filter(d("_id", 1)).update(d("$addToSet", d("a", 1.0))).expect(docs(d("_id", 1, "a", l(1))))
            .inMem("MOR-055", docs(d("_id", 1, "a", l(1, 1.0)))),
            c("addToSet compares documents with the field order").seed(d("_id", 1, "a", l(d("x", 1, "y", 2)))).filter(d("_id", 1)).update(d("$addToSet", d("a", d("y", 2, "x", 1)))).expect(docs(d("_id", 1, "a", l(d("x", 1, "y", 2), d("y", 2, "x", 1)))))
            .inMem("MOR-055", docs(d("_id", 1, "a", l(d("x", 1, "y", 2))))),
            c("addToSet to a field that is not an array fails").seed(d("_id", 1, "a", "s")).filter(d("_id", 1)).update(d("$addToSet", d("a", 1))).expect(error(2)).unchanged(),
        };
        return new ArrayList<>(List.of(cases));
    }

    static List<Case> pullAndPop() {
        Case[] cases = {
            c("pull removes all equal elements").seed(d("_id", 1, "a", l(1, 2, 1, 3))).filter(d("_id", 1)).update(d("$pull", d("a", 1))).expect(docs(d("_id", 1, "a", l(2, 3)))),
            c("pull with a condition").seed(d("_id", 1, "a", l(1, 2, 3, 4))).filter(d("_id", 1)).update(d("$pull", d("a", d("$gt", 2)))).expect(docs(d("_id", 1, "a", l(1, 2)))),
            c("pull of documents by a field condition").seed(d("_id", 1, "a", l(d("k", 1, "v", "x"), d("k", 2, "v", "y")))).filter(d("_id", 1)).update(d("$pull", d("a", d("k", 1))))
            .expect(docs(d("_id", 1, "a", l(d("k", 2, "v", "y"))))),
            c("pull of a missing field is a no-op").seed(d("_id", 1)).filter(d("_id", 1)).update(d("$pull", d("a", 1))).expect(docs(d("_id", 1))),
            c("pull from a field that is not an array fails").seed(d("_id", 1, "a", 1)).filter(d("_id", 1)).update(d("$pull", d("a", 1))).expect(error(2)).unchanged(),
            c("pull from a field that is null fails").seed(d("_id", 1, "a", null)).filter(d("_id", 1)).update(d("$pull", d("a", 1))).expect(error(2)).unchanged(),
            c("pullAll removes the listed values").seed(d("_id", 1, "a", l(1, 2, 3, 2))).filter(d("_id", 1)).update(d("$pullAll", d("a", l(2, 3)))).expect(docs(d("_id", 1, "a", l(1)))),
            c("pullAll of a missing field is a no-op").seed(d("_id", 1)).filter(d("_id", 1)).update(d("$pullAll", d("a", l(1)))).expect(docs(d("_id", 1))),

            c("pop 1 removes the last element").seed(d("_id", 1, "a", l(1, 2, 3))).filter(d("_id", 1)).update(d("$pop", d("a", 1))).expect(docs(d("_id", 1, "a", l(1, 2)))),
            c("pop -1 removes the first element").seed(d("_id", 1, "a", l(1, 2, 3))).filter(d("_id", 1)).update(d("$pop", d("a", -1))).expect(docs(d("_id", 1, "a", l(2, 3)))),
            c("pop of an empty array is a no-op").seed(d("_id", 1, "a", l())).filter(d("_id", 1)).update(d("$pop", d("a", 1))).expect(docs(d("_id", 1, "a", l()))),
            c("pop of a missing field is a no-op").seed(d("_id", 1)).filter(d("_id", 1)).update(d("$pop", d("a", 1))).expect(docs(d("_id", 1))),
            c("pop of a field that is null fails").seed(d("_id", 1, "a", null)).filter(d("_id", 1)).update(d("$pop", d("a", 1))).expect(error(14)).unchanged(),
            c("pop of a field that is not an array fails").seed(d("_id", 1, "a", 1)).filter(d("_id", 1)).update(d("$pop", d("a", 1))).expect(error(14)).unchanged(),
        };
        return new ArrayList<>(List.of(cases));
    }

    static List<Case> positional() {
        Case[] cases = {
            c("positional $ updates the first matched element").seed(d("_id", 1, "a", l(d("x", 1, "y", 0), d("x", 2, "y", 0), d("x", 2, "y", 0)))).filter(d("a.x", 2)).update(d("$set", d("a.$.y", 9)))
            .expect(docs(d("_id", 1, "a", l(d("x", 1, "y", 0), d("x", 2, "y", 9), d("x", 2, "y", 0))))),
            c("positional $ on a scalar array").seed(d("_id", 1, "a", l(5, 6, 7))).filter(d("a", 6)).update(d("$set", d("a.$", 60))).expect(docs(d("_id", 1, "a", l(5, 60, 7)))),
            c("all-positional $[] updates every element").seed(d("_id", 1, "a", l(1, 2, 3))).filter(d("_id", 1)).update(d("$inc", d("a.$[]", 10))).expect(docs(d("_id", 1, "a", l(11, 12, 13)))),
            c("all-positional $[] on documents").seed(d("_id", 1, "a", l(d("n", 1), d("n", 2)))).filter(d("_id", 1)).update(d("$set", d("a.$[].done", true))).expect(docs(d("_id", 1, "a", l(d("n", 1, "done", true), d("n", 2, "done", true))))),
            c("filtered positional $[id] with arrayFilters").seed(d("_id", 1, "a", l(d("n", 1), d("n", 2), d("n", 3)))).filter(d("_id", 1)).update(d("$set", d("a.$[e].hit", true))).arrayFilters(d("e.n", d("$gte", 2)))
            .expect(docs(d("_id", 1, "a", l(d("n", 1), d("n", 2, "hit", true), d("n", 3, "hit", true))))),
            c("filtered positional on scalars").seed(d("_id", 1, "a", l(1, 5, 9))).filter(d("_id", 1)).update(d("$set", d("a.$[big]", 0))).arrayFilters(d("big", d("$gt", 4))).expect(docs(d("_id", 1, "a", l(1, 0, 0)))),
            c("filtered positional matching nothing changes nothing").seed(d("_id", 1, "a", l(1, 2))).filter(d("_id", 1)).update(d("$set", d("a.$[e]", 0))).arrayFilters(d("e", d("$gt", 5))).expect(docs(d("_id", 1, "a", l(1, 2)))),
            c("nested filtered positional").seed(d("_id", 1, "a", l(d("k", 1, "b", l(1, 2)), d("k", 2, "b", l(1, 2))))).filter(d("_id", 1)).update(d("$inc", d("a.$[o].b.$[i]", 100)))
            .arrayFilters(d("o.k", 2), d("i", d("$gte", 2))).expect(docs(d("_id", 1, "a", l(d("k", 1, "b", l(1, 2)), d("k", 2, "b", l(1, 102)))))),
            c("an arrayFilters identifier that is not used fails").seed(d("_id", 1, "a", l(1))).filter(d("_id", 1)).update(d("$set", d("a.0", 5))).arrayFilters(d("e", d("$gt", 0))).expect(error(9)).unchanged(),
            c("push into a filtered element").seed(d("_id", 1, "a", l(d("k", 1, "t", l()), d("k", 2, "t", l())))).filter(d("_id", 1)).update(d("$push", d("a.$[e].t", "x"))).arrayFilters(d("e.k", 2))
            .expect(docs(d("_id", 1, "a", l(d("k", 1, "t", l()), d("k", 2, "t", l("x")))))),
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
    public void pushAndAddToSet(Morphium m) throws Exception {
        try (m) {
            check(m, pushAndAddToSet());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pullAndPop(Morphium m) throws Exception {
        try (m) {
            check(m, pullAndPop());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void positional(Morphium m) throws Exception {
        try (m) {
            check(m, positional());
        }
    }
}
