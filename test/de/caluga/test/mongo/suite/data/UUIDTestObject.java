package de.caluga.test.mongo.suite.data;

import java.util.UUID;

import de.caluga.morphium.annotations.DefaultReadPreference;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.annotations.SafetyLevel;
import de.caluga.morphium.annotations.WriteSafety;

@Entity(typeId = "uuidtest", nameProvider = TestEntityNameProvider.class)
@WriteSafety(timeout = -1, level = SafetyLevel.WAIT_FOR_ALL_SLAVES)
@DefaultReadPreference(ReadPreferenceLevel.SECONDARY_PREFERRED)
public class UUIDTestObject {

    @Id
    public UUID id;

    public UUID uuidValue;

    public enum Fields {uuidValue, id}
}
