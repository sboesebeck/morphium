package de.caluga.test.mongo.suite.data;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumObjectMapper;
import de.caluga.morphium.NameProvider;

import java.util.concurrent.atomic.AtomicInteger;

public class TestEntityNameProvider implements NameProvider {
    public static AtomicInteger number = new AtomicInteger();

    @Override
    public String getCollectionName(Class<?> type, MorphiumObjectMapper om, boolean translateCamelCase, boolean useFQN, String specifiedName, Morphium morphium) {
        if (specifiedName != null) return specifiedName;
        String n = type.getSimpleName();
        if (translateCamelCase) {
            if (morphium == null) {
                n = new AnnotationAndReflectionHelper(translateCamelCase).convertCamelCase(n);
            } else {
                n = morphium.getARHelper().convertCamelCase(n);
            }
        }
        return n + "_" + number.get();
    }
}
