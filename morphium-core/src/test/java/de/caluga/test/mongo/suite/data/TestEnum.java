package de.caluga.test.mongo.suite.data;

/**
 * User: Stephan Bösebeck
 * Date: 04.05.12
 * Time: 12:45
 * <p>
 */
public enum TestEnum {
    TEST1("test"), TEST2("test2"), VALUE("value"), NOCH_EIN_TEST("noch ein test");

    String content;

    TestEnum(String n) {
        content = n;
    }
}
