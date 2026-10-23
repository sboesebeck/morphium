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

/** MOR-028: upserts ($setOnInsert, seeding from the filter) and the statements that a real MongoDB rejects, on every driver. */
@Tag("core")
public class UpsertAndErrorContractTest extends MultiDriverTestBase {

    static List<Case> upserts() {
        Case[] cases = {
            c("upsert inserts the filter equalities and the update").filter(d("_id", 7, "k", "x")).update(d("$set", d("v", 1))).upsert().expect(docs(d("_id", 7, "k", "x", "v", 1))),
            c("upsert seeds a dotted equality as a nested document").filter(d("_id", 1, "a.b", 1)).update(d("$set", d("v", 1))).upsert().expect(docs(d("_id", 1, "a", d("b", 1), "v", 1))),
            c("upsert seeds the equalities of an $and").filter(d("$and", l(d("_id", 1), d("k", 2)))).update(d("$set", d("v", 1))).upsert().expect(docs(d("_id", 1, "k", 2, "v", 1))),
            c("upsert does not seed a range condition").filter(d("_id", 1, "n", d("$gt", 5))).update(d("$set", d("v", 1))).upsert().expect(docs(d("_id", 1, "v", 1))),
            c("upsert seeds an explicit $eq").filter(d("_id", 1, "k", d("$eq", 3))).update(d("$set", d("v", 1))).upsert().expect(docs(d("_id", 1, "k", 3, "v", 1))),
            c("upsert seeds a single-branch $or").filter(d("_id", 1, "$or", l(d("k", 1)))).update(d("$set", d("v", 1))).upsert().expect(docs(d("_id", 1, "k", 1, "v", 1)))
            .inMem("MOR-056", docs(d("_id", 1, "v", 1))),
            c("upsert does not seed a multi-branch $or").filter(d("_id", 1, "$or", l(d("k", 1), d("k", 2)))).update(d("$set", d("v", 1))).upsert().expect(docs(d("_id", 1, "v", 1))),
            c("upsert does not seed an $in").filter(d("_id", 1, "k", d("$in", l(1, 2)))).update(d("$set", d("v", 1))).upsert().expect(docs(d("_id", 1, "v", 1))),
            c("upsert applies setOnInsert on insert").filter(d("_id", 1)).update(d("$set", d("v", 1), "$setOnInsert", d("created", "now"))).upsert().expect(docs(d("_id", 1, "v", 1, "created", "now"))),
            c("upsert ignores setOnInsert on an existing document").seed(d("_id", 1, "v", 0)).filter(d("_id", 1)).update(d("$set", d("v", 1), "$setOnInsert", d("created", "now"))).upsert()
            .expect(docs(d("_id", 1, "v", 1))),
            c("upsert with inc creates the counter").filter(d("_id", 1)).update(d("$inc", d("n", 1))).upsert().expect(docs(d("_id", 1, "n", 1))),
            c("upsert with only setOnInsert inserts").filter(d("_id", 1)).update(d("$setOnInsert", d("a", 1))).upsert().expect(docs(d("_id", 1, "a", 1))),
            c("upsert with a matching document updates and inserts nothing").seed(d("_id", 1, "k", "x")).filter(d("k", "x")).update(d("$set", d("v", 1))).upsert().expect(docs(d("_id", 1, "k", "x", "v", 1))),
            c("upsert with multi updates every match and inserts nothing").seed(d("_id", 1, "k", 1), d("_id", 2, "k", 1)).filter(d("k", 1)).update(d("$set", d("v", 1))).upsert().multi()
            .expect(docs(d("_id", 1, "k", 1, "v", 1), d("_id", 2, "k", 1, "v", 1))),
            c("upsert pinning _id in setOnInsert").filter(d("k", "x")).update(d("$setOnInsert", d("_id", 9))).upsert().expect(docs(d("_id", 9, "k", "x"))),
            c("upsert of an _id that exists with a filter that does not match is a duplicate key").seed(d("_id", 1, "owner", "A")).filter(d("_id", 1, "owner", "B")).update(d("$set", d("v", 1))).upsert()
            .expect(error(11000)),
            c("upsert applies inc on top of a seeded equality").filter(d("_id", 1, "k", 1)).update(d("$inc", d("k", 1))).upsert().expect(docs(d("_id", 1, "k", 2))),
            c("an update without operators replaces the document").seed(d("_id", 1, "a", 1)).filter(d("_id", 1)).update(d("b", 2)).expect(docs(d("_id", 1, "b", 2))),
            c("an empty replacement keeps only the _id").seed(d("_id", 1, "a", 1)).filter(d("_id", 1)).update(d()).expect(docs(d("_id", 1))),
            c("an upsert with a replacement inserts it with the filter id").filter(d("_id", 5)).update(d("b", 2)).upsert().expect(docs(d("_id", 5, "b", 2))),
        };
        return new ArrayList<>(List.of(cases));
    }

    static List<Case> rejected() {
        Case[] cases = {
            c("set of a path and its child conflicts").seed(d("_id", 1, "a", d("b", 1))).filter(d("_id", 1)).update(d("$set", d("a", d("x", 1), "a.b", 2))).expect(error(40))
            .inMem("MOR-051", docs(d("_id", 1, "a", d("b", 2, "x", 1)))),
            c("set and inc of one path conflict").seed(d("_id", 1, "a", 1)).filter(d("_id", 1)).update(d("$set", d("a", 5), "$inc", d("a", 1))).expect(error(40))
            .inMem("MOR-051", docs(d("_id", 1, "a", 6))),
            c("set and unset of one path conflict").seed(d("_id", 1, "a", 1)).filter(d("_id", 1)).update(d("$set", d("a", 5), "$unset", d("a", ""))).expect(error(40))
            .inMem("MOR-051", docs(d("_id", 1))),
            c("set of a parent and unset of the child conflict").seed(d("_id", 1, "a", d("b", 1))).filter(d("_id", 1)).update(d("$set", d("a", d()), "$unset", d("a.b", ""))).expect(error(40))
            .inMem("MOR-051", docs(d("_id", 1, "a", d()))),
            c("push and pull of one path conflict").seed(d("_id", 1, "a", l(1))).filter(d("_id", 1)).update(d("$push", d("a", 2), "$pull", d("a", 1))).expect(error(40))
            .inMem("MOR-051", docs(d("_id", 1, "a", l(2)))),
            c("changing the _id fails").seed(d("_id", 1)).filter(d("_id", 1)).update(d("$set", d("_id", 2))).expect(error(66))
            .inMem("MOR-054", docs(d("_id", 2))),
            c("a field name that starts with a dollar sign fails").seed(d("_id", 1)).filter(d("_id", 1)).update(d("$set", d("$bad", 1))).expect(error(52))
            .inMemFindAndModify("MOR-058", docs(d("_id", 1))),
            c("an unknown operator fails").seed(d("_id", 1)).filter(d("_id", 1)).update(d("$foo", d("a", 1))).expect(error(9))
            .inMem("MOR-054", notADatabaseError()),
            c("operators mixed with fields fail").seed(d("_id", 1)).filter(d("_id", 1)).update(d("$set", d("a", 1), "b", 2)).expect(error(9))
            .inMem("MOR-054", notADatabaseError()),
            c("an operator with an empty field name fails").seed(d("_id", 1)).filter(d("_id", 1)).update(d("$set", d("", 1))).expect(error(56))
            .inMem("MOR-054", docs(d("", 1, "_id", 1))),
            c("setting a field below a scalar fails").seed(d("_id", 1, "a", 1)).filter(d("_id", 1)).update(d("$set", d("a.b", 1))).expect(error(28))
            .inMemFindAndModify("MOR-058", docs(d("_id", 1, "a", 1))),
            c("setting a field below an array with a non-numeric segment fails").seed(d("_id", 1, "a", l(1))).filter(d("_id", 1)).update(d("$set", d("a.b", 1))).expect(error(28))
            .inMemFindAndModify("MOR-058", docs(d("_id", 1, "a", l(1)))),
            c("a failing statement changes nothing").seed(d("_id", 1, "a", 1, "s", "x")).filter(d("_id", 1)).update(d("$set", d("a", 2), "$inc", d("s", 1))).expect(error(14))
            .inMem("MOR-053", docs(d("_id", 1, "a", 2, "s", "x"))),
            c("a failing multi update stops at the first error").seed(d("_id", 1, "v", 1), d("_id", 2, "v", "x"), d("_id", 3, "v", 1)).filter(d()).update(d("$inc", d("v", 1))).multi().expect(error(14))
            .inMem("MOR-053", docs(d("_id", 1, "v", 2), d("_id", 2, "v", "x"), d("_id", 3, "v", 2))),
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
    public void upserts(Morphium m) throws Exception {
        try (m) {
            check(m, upserts());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void rejected(Morphium m) throws Exception {
        try (m) {
            check(m, rejected());
        }
    }
}
