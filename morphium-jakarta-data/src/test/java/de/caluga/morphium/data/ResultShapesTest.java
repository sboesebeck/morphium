package de.caluga.morphium.data;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** MOR-080: a repository method may return an array of the entity. */
class ResultShapesTest {

    @Test
    @DisplayName("a List, a Stream and a single value become an array of the declared type")
    void toArray() {
        Object fromList = ResultShapes.toArray(List.of("a", "b"), String.class);
        assertThat(fromList).isInstanceOf(String[].class);
        assertThat((String[]) fromList).containsExactly("a", "b");

        assertThat((String[]) ResultShapes.toArray(Stream.of("x", "y", "z"), String.class)).containsExactly("x", "y", "z");
        assertThat((String[]) ResultShapes.toArray("only", String.class)).containsExactly("only");
        assertThat((String[]) ResultShapes.toArray(List.of(), String.class)).isEmpty();
    }

    @Test
    @DisplayName("an array stays, null stays null, a primitive component works")
    void edges() {
        String[] already = {"a"};
        assertThat(ResultShapes.toArray(already, String.class)).isSameAs(already);
        assertThat(ResultShapes.toArray(null, String.class)).isNull();
        assertThat((int[]) ResultShapes.toArray(List.of(1, 2, 3), int.class)).containsExactly(1, 2, 3);
    }
}
