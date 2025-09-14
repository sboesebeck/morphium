package de.caluga.test.mongo.suite.aggregationStages;

import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Property;
import de.caluga.morphium.annotations.ReadOnly;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.Arrays;
import java.util.List;

@Tag("aggregation")
public class AddFieldAndSetTests extends MorphiumTestBase {

    @Test
    public void addFieldsTest() throws Exception {
        prepareData();

        Aggregator<Student, Student> agg = morphium.createAggregator(Student.class, Student.class);
        agg.addFields(UtilsMap.of("total_homework", (Object) UtilsMap.of("$sum", "$homework"), "total_quiz", UtilsMap.of("$sum", "$quiz"))
        );
        agg.addFields(UtilsMap.of("total_score", (Object) Expr.add(Expr.field("total_homework"),
                Expr.field("total_quiz"), Expr.field("extra_credit"))));

        List<Student> lst = agg.aggregate();
        for (Student s : lst) {
            log.info(s.toString());
            assert (s.totalHomework != 0);
            assert (s.totalQuiz != 0);
            assert (s.totalScore != 0);
        }

    }

    private void prepareData()  throws Exception {
        morphium.clearCollection(Student.class);
        Thread.sleep(100);
        Student s1 = new Student();
        s1.name = "Maya";
        s1.homework = new int[]{5, 10, 5};
        s1.quiz = new int[]{8, 10};
        s1.extraCredit = 0;
        morphium.store(s1);
        s1 = new Student();
        s1.name = "Ryan";
        s1.homework = new int[]{5, 6, 5};
        s1.quiz = new int[]{8, 8};
        s1.extraCredit = 8;
        morphium.store(s1);

        s1 = new Student();
        s1.name = "Peter";
        s1.homework = new int[]{12, 10, 11};
        s1.quiz = new int[]{11, 12};
        s1.extraCredit = 13;
        morphium.store(s1);
    }


    @Test
    public void setTest() throws Exception {
        prepareData();

        Aggregator<Student, Student> agg = morphium.createAggregator(Student.class, Student.class);
        agg.set(UtilsMap.of("total_homework", Expr.sum(Expr.field("homework")), "total_quiz", Expr.sum(Expr.field("quiz")))
        );
        agg.set(UtilsMap.of("total_score", Expr.add(Expr.field("total_homework"),
                Expr.field("total_quiz"), Expr.field("extra_credit"))));

        List<Student> lst = agg.aggregate();
        for (Student s : lst) {
            log.info(s.toString());
            assert (s.totalHomework != 0);
            assert (s.totalQuiz != 0);
            assert (s.totalScore != 0);
        }

    }


    @Entity
    public static class Student {
        @Id
        public MorphiumId id;

        @Property(fieldName = "student")
        public String name;
        public int[] homework;
        public int[] quiz;
        public int extraCredit;

        @ReadOnly
        public int totalHomework;
        @ReadOnly
        public int totalQuiz;
        @ReadOnly
        public int totalScore;

        @Override
        public String toString() {
            return "Student{" +
                    "name='" + name + '\'' +
                    ", homework=" + Arrays.toString(homework) +
                    ", quiz=" + Arrays.toString(quiz) +
                    ", extraCredit=" + extraCredit +
                    ", totalHomework=" + totalHomework +
                    ", totalQuiz=" + totalQuiz +
                    ", totalScore=" + totalScore +
                    '}';
        }
    }
}
