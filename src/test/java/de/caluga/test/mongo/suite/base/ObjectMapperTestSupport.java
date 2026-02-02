package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.test.mongo.suite.data.EmbeddedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;

import java.math.BigInteger;
import java.util.*;

/**
 * Support classes extracted from the former {@code ObjectMapperTest} to be shared across split test classes.
 *
 * Package-private on purpose (same package as tests, no new packages).
 */
@Embedded
class MyClass {
    // does not need to be entity
    String theValue;
}

enum TestEnum {
    v1, v2, v3, v4,
}

@Entity
class ListOfEmbedded {
    @Id
    public MorphiumId id;
    public List<EmbeddedObject> list;
}

@Entity
class ListOfListOfListOfString {
    @Id
    public String id;

    public List<List<List<String>>> list;
}

@Entity
class ListOfListOfListOfUncached {
    @Id
    public String id;

    public List<List<List<UncachedObject>>> list;
}

@Entity
class ListOfMapOfListOfString {
    @Id
    public MorphiumId id;

    public List<Map<String, List<String>>> list;
}

@Entity
class MappedObject {
    @Id
    public String id;
    public UncachedObject uc;
    public Map<String, String> aMap;
}

@Entity
class SetObject {
    @Id
    public MorphiumId id;
    public Set<String> setOfStrings;
    public Set<UncachedObject> setOfUC;
    public List<Set<String>> listOfSetOfStrings;
    public Map<String, Set<String>> mapOfSetOfStrings;
}

@Entity
class ObjectMapperEnumTestContainer {
    @Id
    public String id;
    public TestEnum anEnum;
    public Map<String, TestEnum> aMap;
    public List<TestEnum> lst;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ObjectMapperEnumTestContainer)) return false;
        ObjectMapperEnumTestContainer enumTest = (ObjectMapperEnumTestContainer) o;
        return Objects.equals(id, enumTest.id) &&
               anEnum == enumTest.anEnum &&
               Objects.equals(aMap, enumTest.aMap) &&
               Objects.equals(lst, enumTest.lst);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, anEnum, aMap, lst);
    }
}

@Entity
class BIObject {
    @Id
    public MorphiumId id;
    public String value;
    public BigInteger biValue;
}

@Entity
class Complex {
    @Id
    public MorphiumId id;
    public List<Map<String, Object>> structureK;
}

class Simple {
    public String test = "test_" + System.currentTimeMillis();
    public int value = (int) (System.currentTimeMillis() % 42);
}

class NoDefaultConstructorUncachedObject extends UncachedObject {
    public NoDefaultConstructorUncachedObject(String v, int c) {
        setCounter(c);
        setStrValue(v);
    }
}
