package de.caluga.morphium;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Reference;
import de.caluga.morphium.driver.MorphiumId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * MOR-041: {@link AnnotationAndReflectionHelper#resolveFieldPath(Class, String)} resolves dotted keys segment by segment through single
 * embedded fields, each segment with the naming rules of the class it belongs to, and stops (rest verbatim) at everything it cannot know.
 */
@Tag("core")
public class ResolveFieldPathTest {

    @Embedded
    public static class Deep {
        public String deepValue;
    }

    /** default naming */
    @Embedded
    public static class Child {
        public String fileName;
        public Deep innerChild;
    }

    /** own flag false */
    @Embedded(translateCamelCase = false)
    public static class RawChild {
        public String fileName;
        public Deep deepNode;
    }

    @Embedded(polymorph = true)
    public static class Base {
        public String baseName;
    }

    @Embedded(polymorph = true)
    public static class Sub extends Base {
        public String subOnly;
    }

    @Entity
    public static class Target {
        @Id
        public MorphiumId id;
        public String targetLabel;
    }

    /** default naming owner */
    @Entity
    public static class Owner {
        @Id
        public MorphiumId id;
        public Child childEntry;
        public RawChild rawChildEntry;
        public List<Child> childList;
        public Child[] childArray;
        public Map<String, Child> childMap;
        public Object anything;
        @Reference
        public Target target;
        public Base polyValue;
    }

    /** non translating owner, embedded with default naming */
    @Entity(translateCamelCase = false)
    public static class RawOwner {
        @Id
        public MorphiumId id;
        public Child defaultChild;
        public RawChild rawChild;
    }

    private String p(Class<?> c, String path) {
        return new AnnotationAndReflectionHelper(true).resolveFieldPath(c, path);
    }

    @Test
    public void singleSegmentDelegatesToTheStoredNameResolver() {
        assertEquals("child_entry", p(Owner.class, "childEntry"));
        assertEquals("_id", p(Owner.class, "id"));
        assertEquals("x", p(Owner.class, "x"));
    }

    @Test
    public void embeddedSegmentsUseTheNamingOfTheirOwnClass() {
        // owner translates, Child translates
        assertEquals("child_entry.file_name", p(Owner.class, "childEntry.fileName"));
        assertEquals("child_entry.inner_child.deep_value", p(Owner.class, "childEntry.innerChild.deepValue"));
        // owner translates, RawChild keeps Java names, Deep translates
        assertEquals("raw_child_entry.fileName", p(Owner.class, "rawChildEntry.fileName"));
        assertEquals("raw_child_entry.deepNode.deep_value", p(Owner.class, "rawChildEntry.deepNode.deepValue"));
        // owner does not translate, the embedded classes do their own thing (hypothesis 2 of the research)
        assertEquals("defaultChild.file_name", p(RawOwner.class, "defaultChild.fileName"));
        assertEquals("defaultChild.inner_child.deep_value", p(RawOwner.class, "defaultChild.innerChild.deepValue"));
        assertEquals("rawChild.fileName", p(RawOwner.class, "rawChild.fileName"));
    }

    @Test
    public void storedNamesInThePathStayUnchanged() {
        assertEquals("child_entry.file_name", p(Owner.class, "child_entry.file_name"));
        assertEquals("child_entry.inner_child.deep_value", p(Owner.class, "child_entry.inner_child.deep_value"));
    }

    @Test
    public void collectionsAndMapsStopTheWalkButTheirOwnNameIsTranslated() {
        assertEquals("child_list.0.fileName", p(Owner.class, "childList.0.fileName"));
        assertEquals("child_list.$.fileName", p(Owner.class, "childList.$.fileName"));
        assertEquals("child_list.$[x].fileName", p(Owner.class, "childList.$[x].fileName"));
        assertEquals("child_list.$[].fileName", p(Owner.class, "childList.$[].fileName"));
        assertEquals("child_array.1.fileName", p(Owner.class, "childArray.1.fileName"));
        assertEquals("child_map.someKey.fileName", p(Owner.class, "childMap.someKey.fileName"));
    }

    @Test
    public void objectTypedReferenceAndUnknownSegmentsStopTheWalk() {
        assertEquals("anything.fooBar.bazQux", p(Owner.class, "anything.fooBar.bazQux"));
        assertEquals("target.targetLabel", p(Owner.class, "target.targetLabel"));
        assertEquals("unknown.fooBar", p(Owner.class, "unknown.fooBar"));
        assertEquals("child_entry.unknownField", p(Owner.class, "childEntry.unknownField"));
    }

    @Test
    public void polymorphicBaseTypeIsResolvedButSubclassFieldsPassThrough() {
        assertEquals("poly_value.base_name", p(Owner.class, "polyValue.baseName"));
        assertEquals("poly_value.subOnly", p(Owner.class, "polyValue.subOnly"));
    }

    @Test
    public void odditiesNeverThrow() {
        assertEquals("", p(Owner.class, ""));
        assertEquals("child_entry.", p(Owner.class, "childEntry."));
        assertEquals(".a", p(Owner.class, ".a"));
        assertEquals("a..b", p(Owner.class, "a..b"));
        assertEquals("$[x].a", p(Owner.class, "$[x].a"));
        assertEquals("$set.a", p(Owner.class, "$set.a"));
        assertEquals("a.b", new AnnotationAndReflectionHelper(true).resolveFieldPath(null, "a.b"));
        assertEquals("a.b", new AnnotationAndReflectionHelper(true).resolveFieldPath(Runnable.class, "a.b"));
        assertEquals(null, new AnnotationAndReflectionHelper(true).resolveFieldPath(Owner.class, null));
    }

    @Test
    public void pathResolutionIsIdempotent() {
        for (String path : List.of("childEntry.fileName", "childEntry.innerChild.deepValue", "rawChildEntry.deepNode.deepValue", "childList.0.fileName",
                                   "childMap.someKey.fileName", "polyValue.baseName", "polyValue.subOnly", "unknown.fooBar", "childList.$[x].fileName", "id",
                                   "anything.fooBar", "target.targetLabel")) {
            String once = p(Owner.class, path);
            assertEquals(once, p(Owner.class, once), path);
        }
    }
}
