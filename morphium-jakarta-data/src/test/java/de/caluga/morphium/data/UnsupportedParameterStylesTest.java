package de.caluga.morphium.data;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** MOR-019: the Jakarta Data 1.1 parameter styles that {@code @Find} and {@code @Delete} would run as an equality are recognised by their names. */
class UnsupportedParameterStylesTest {

    @Test
    @DisplayName("@Is, the constraint types and Restriction are recognised, plain types and annotations are not")
    void recognised() {
        assertThat(UnsupportedParameterStyles.isUnsupportedParameterAnnotation("jakarta.data.repository.Is")).isTrue();
        assertThat(UnsupportedParameterStyles.isUnsupportedParameterAnnotation("jakarta.data.repository.By")).isFalse();
        assertThat(UnsupportedParameterStyles.isUnsupportedParameterAnnotation("jakarta.data.repository.Param")).isFalse();

        assertThat(UnsupportedParameterStyles.isUnsupportedParameterType("jakarta.data.constraint.AtLeast")).isTrue();
        assertThat(UnsupportedParameterStyles.isUnsupportedParameterType("jakarta.data.constraint.Constraint")).isTrue();
        assertThat(UnsupportedParameterStyles.isUnsupportedParameterType("jakarta.data.restrict.Restriction")).isTrue();
        assertThat(UnsupportedParameterStyles.isUnsupportedParameterType("jakarta.data.restrict.Restrict")).isTrue();
        assertThat(UnsupportedParameterStyles.isUnsupportedParameterType("java.lang.String")).isFalse();
        assertThat(UnsupportedParameterStyles.isUnsupportedParameterType("jakarta.data.Limit")).isFalse();
        assertThat(UnsupportedParameterStyles.isUnsupportedParameterType("jakarta.data.page.PageRequest")).isFalse();

        assertThat(UnsupportedParameterStyles.isUnsupportedMethodAnnotation("jakarta.data.repository.First")).isTrue();
        assertThat(UnsupportedParameterStyles.isUnsupportedMethodAnnotation("jakarta.data.repository.OrderBy")).isFalse();
    }

    @Test
    @DisplayName("the first unsupported thing is reported with the method and the way out; a supported method is null")
    void messages() {
        assertThat(UnsupportedParameterStyles.check("Shop.cheap", List.of("jakarta.data.repository.Find"), List.of())).isNull();

        String atLeast = UnsupportedParameterStyles.check("Shop.cheap", List.of("jakarta.data.repository.Find"),
                List.of(new UnsupportedParameterStyles.Param("int", List.of("jakarta.data.repository.By", "jakarta.data.repository.Is"))));
        assertThat(atLeast).contains("Shop.cheap").contains("@Is").contains("parameter 1").contains("findBy").contains("equality");

        String restriction = UnsupportedParameterStyles.check("Shop.cheap", List.of(), List.of(new UnsupportedParameterStyles.Param("java.lang.String", List.of()),
                new UnsupportedParameterStyles.Param("jakarta.data.restrict.Restriction", List.of())));
        assertThat(restriction).contains("parameter 2").contains("Restriction");

        String first = UnsupportedParameterStyles.check("Shop.cheap", List.of("jakarta.data.repository.First"), List.of());
        assertThat(first).contains("@First");
    }
}
