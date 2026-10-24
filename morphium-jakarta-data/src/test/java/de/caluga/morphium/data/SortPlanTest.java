package de.caluga.morphium.data;

import jakarta.data.Order;
import jakarta.data.Sort;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** MOR-081: the static order comes first, the dynamic sorts follow in the order of the parameters, a property counts once. */
class SortPlanTest {

    @Test
    @DisplayName("a Sort, several Sorts, a varargs array and an Order are collected in the order of the parameters")
    void collected() {
        Object[] args = {"ignored", 5L, Sort.asc("a"), new Sort<?>[] {Sort.desc("b"), Sort.asc("c")}, Order.by(Sort.desc("d"), Sort.asc("e")), Sort.desc("f"), null};

        assertThat(SortPlan.dynamic(args)).extracting(SortPlan.Entry::property).containsExactly("a", "b", "c", "d", "e", "f");
        assertThat(SortPlan.dynamic(args)).extracting(SortPlan.Entry::ascending).containsExactly(true, false, true, false, true, false);
        assertThat(SortPlan.dynamic(null)).isEmpty();
        assertThat(SortPlan.dynamic(new Object[0])).isEmpty();
    }

    @Test
    @DisplayName("static first, dynamic after, the first occurrence of a property wins")
    void merged() {
        List<SortPlan.Entry> merged = SortPlan.merge(List.of(new SortPlan.Entry("type", true)),
                List.of(new SortPlan.Entry("root", false), new SortPlan.Entry("type", false), new SortPlan.Entry("id", true)));

        assertThat(merged).extracting(SortPlan.Entry::property).containsExactly("type", "root", "id");
        assertThat(merged.get(0).ascending()).isTrue();
    }
}
