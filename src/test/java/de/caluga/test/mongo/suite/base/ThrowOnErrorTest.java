package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.test.mongo.suite.data.AdditionalDataEntity;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

import static de.caluga.morphium.ThrowOnError.*;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ThrowOnErrorTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void throwErrorBecauseOfTypeMismatch(Morphium morphium) {

        AdditionalDataEntity entity = new AdditionalDataEntity();
        morphium.store(entity);

        morphium.createQueryFor(AdditionalDataEntity.class)
                .f("_id").eq(entity.getMorphiumId())
                .set("someStringField", "noStringsAttached");

        // without the usage of throwOnWriteError the morphium instances differ strongly
        // 1) the in-memory driver throws an exception
        // 2) the other drivers don't throw an exception,
        //    they just return a Map with a 'writeErrors' key
        //
        // Thanks to the usage throwOnWriteError() they only differ in the Exception thrown
        // in most cases.

        assertThatThrownBy(() -> {
            throwOnWriteError(morphium.createQueryFor(AdditionalDataEntity.class)
                    .f("_id").eq(entity.getMorphiumId())
                    .set("someStringField.thisCantWork", "pleaseThrowError"));
        }).isInstanceOf(Exception.class);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void throwErrorBecauseOfBadQuery(Morphium morphium) {

        if (morphium.getDriver().getName().contains("InMem")) {
            // it's difficult to check all query expressions for validity
            // in the in-memory case
            // This is a future TODO
        } else {
            assertThatThrownBy(() -> {
                // it's important to know if a query is badly written!
                throwOnWriteError(morphium.createQueryFor(AdditionalDataEntity.class)
                        .f("$big$").eq("$nonsense$")
                        .set("otherwise", "looking good"));
            }).isInstanceOf(Exception.class);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void throwNoErrorWithExpectedNumberOfEntitiesModified(Morphium morphium) {

        AdditionalDataEntity entity = new AdditionalDataEntity();
        morphium.store(entity);

        throwOnErrorOrNotExactlyOneEntityModified(
                morphium.createQueryFor(AdditionalDataEntity.class)
                        .f("_id").eq(entity.getMorphiumId())
                        .set("name", "Spongebob"));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void throwErrorBecauseOfUnexpectedNumberOfEntitiesModified(Morphium morphium) {

        assertThatThrownBy(() -> {
            throwOnErrorOrNotExactlyOneEntityModified(
                    morphium.createQueryFor(AdditionalDataEntity.class)
                            .f("_id").eq("thisIdDoesntExist")
                            .set("name", "Spongebob"));
        }).isInstanceOf(Exception.class);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void atLeastOneEntityModifiedCheckWithDifferentMongoOperations(Morphium morphium) {

        AdditionalDataEntity entity1 = new AdditionalDataEntity();
        AdditionalDataEntity entity2 = new AdditionalDataEntity();
        morphium.store(List.of(entity1, entity2));

        // using set
        throwOnErrorOrExpectationMismatch(
                morphium.createQueryFor(AdditionalDataEntity.class)
                        .f("_id").in(List.of(entity1.getMorphiumId(), entity2.getMorphiumId()))
                        .set("name", "Spongebob"),
                EXPECTATION_AT_LEAST_ONE_ENTITY_MODIFIED);

        // using unset
        throwOnErrorOrExpectationMismatch(
                morphium.createQueryFor(AdditionalDataEntity.class)
                        .f("_id").in(List.of(entity1.getMorphiumId(), entity2.getMorphiumId()))
                        .unset("name", "Spongebob"),
                EXPECTATION_AT_LEAST_ONE_ENTITY_MODIFIED);

        // using push
        throwOnErrorOrExpectationMismatch(
                morphium.createQueryFor(AdditionalDataEntity.class)
                        .f("_id").in(List.of(entity1.getMorphiumId(), entity2.getMorphiumId()))
                        .push("myFriendsList", "Thaddäus"),
                EXPECTATION_AT_LEAST_ONE_ENTITY_MODIFIED);

        // using pull
        throwOnErrorOrExpectationMismatch(
                morphium.createQueryFor(AdditionalDataEntity.class)
                        .f("_id").in(List.of(entity1.getMorphiumId(), entity2.getMorphiumId()))
                        .pull("myFriendsList", "Thaddäus"),
                EXPECTATION_AT_LEAST_ONE_ENTITY_MODIFIED);

        // using addToSet
        throwOnErrorOrExpectationMismatch(
                morphium.addToSet(morphium.createQueryFor(AdditionalDataEntity.class)
                                .f("_id").in(List.of(entity1.getMorphiumId(), entity2.getMorphiumId())),
                        "myFriendsList", "Thaddäus"),
                EXPECTATION_AT_LEAST_ONE_ENTITY_MODIFIED);

    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void throwErrorBecauseOfNotAtLeastOneEntityModified(Morphium morphium) {

        assertThatThrownBy(() -> {
            throwOnErrorOrNotExactlyOneEntityModified(
                    morphium.createQueryFor(AdditionalDataEntity.class)
                            .f("_id").eq("thisIdDoesntExist")
                            .set("name", "Spongebob"));
        }).isInstanceOf(Exception.class);
    }

}