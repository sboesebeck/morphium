package de.caluga.morphium.driver.bson;

public enum UUIDRepresentation {

    UNSPECIFIED(-1), STANDARD(4), C_SHARP_LEGACY(3), JAVA_LEGACY(3), PYTHON_LEGACY(3);

    int subtype;

    UUIDRepresentation(int s) {
        subtype = s;
    }
}
