package de.caluga.morphium.data;

import java.util.List;

/**
 * The parameter styles of Jakarta Data 1.1 that {@code @Find} and {@code @Delete} methods of this module do not implement (MOR-019): a parameter with
 * {@code @Is}, a parameter of a {@code jakarta.data.constraint} type or a {@code Restriction}, and a method with {@code @First}. Every other parameter of those
 * methods is bound as an equality condition, so an unsupported one would run as something else than it says: {@code @Is(AtLeast.class) int min} as
 * {@code price = min}. The check works on class NAMES, so it needs neither the 1.1 API on the classpath nor reflection: the Quarkus build asks it with the names
 * from Jandex, the Spring proxy with the names from reflection.
 */
public final class UnsupportedParameterStyles {

    private static final String IS = "jakarta.data.repository.Is";
    private static final String FIRST = "jakarta.data.repository.First";
    private static final String CONSTRAINT_PACKAGE = "jakarta.data.constraint.";
    private static final String RESTRICT_PACKAGE = "jakarta.data.restrict.";

    /** a parameter: the name of its type and the names of its annotations */
    public record Param(String typeName, List<String> annotationNames) {
    }

    private UnsupportedParameterStyles() {
    }

    public static boolean isUnsupportedParameterAnnotation(String annotationName) {
        return IS.equals(annotationName);
    }

    public static boolean isUnsupportedParameterType(String typeName) {
        return typeName != null && (typeName.startsWith(CONSTRAINT_PACKAGE) || typeName.startsWith(RESTRICT_PACKAGE));
    }

    public static boolean isUnsupportedMethodAnnotation(String annotationName) {
        return FIRST.equals(annotationName);
    }

    /**
     * Checks a method.
     *
     * @param method            what the message names, for example {@code Shop.findCheap}
     * @param methodAnnotations the names of the annotations of the method
     * @param params            the parameters of the method
     * @return null if the method is fine, otherwise a message that names the method, the first unsupported thing and the way out
     */
    public static String check(String method, List<String> methodAnnotations, List<Param> params) {
        for (String annotation : methodAnnotations) {
            if (isUnsupportedMethodAnnotation(annotation)) {
                return message(method, "the annotation @First");
            }
        }

        for (int i = 0; i < params.size(); i++) {
            Param p = params.get(i);

            for (String annotation : p.annotationNames()) {
                if (isUnsupportedParameterAnnotation(annotation)) {
                    return message(method, "parameter " + (i + 1) + " with @Is");
                }
            }

            if (isUnsupportedParameterType(p.typeName())) {
                return message(method, "parameter " + (i + 1) + " of the type " + p.typeName());
            }
        }

        return null;
    }

    private static String message(String method, String what) {
        return "Unsupported Jakarta Data 1.1 feature on " + method + ": " + what + ". The parameters of a @Find or @Delete method of morphium-jakarta-data are bound as equality "
            + "conditions, so this would silently run as something else. Use a derived query (findByPriceGreaterThan) or @Query instead (MOR-020 tracks the support).";
    }
}
