package de.caluga.morphium;


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 11:48
 * <p>
 * Mongo Data types according to documentation:
 * see also: http://www.mongodb.org/display/DOCS/Advanced+Queries#AdvancedQueries-ConditionalOperators
 */
public enum MongoType {
    DOUBLE("double", 1),
    STRING("string", 2),
    OBJECT("object", 3),
    ARRAY("array", 4),
    BINARY_DATA("binData", 5),
    @Deprecated UNDEFINED("undefined", 6),
    OBJECT_ID("objectId", 7),
    BOOLEAN("bool", 8),
    DATE("date", 9),
    NULL("null", 10),
    REGEX("regex", 11),
    @Deprecated DB_PTR("dbPointer", 12),
    JAVASCRIPT("javascript", 13),
    @Deprecated SYMBOL("symbol", 14),
    @Deprecated JAVASCRIPT_SCOPE("javascriptWithScope", 15),
    INTEGER("int", 16),
    TIMESTAMP("timestamp", 17),
    LONG("long", 18),
    DECIMAL("decimal", 19),
    MIN_KEY("minKey", 255),
    MAX_KEY("maxKey", 127);

    private static final MongoType[] ID_CACHE = new MongoType[MIN_KEY.getId() + 1];
    private static final Map<String, MongoType> NAME_CACHE = new ConcurrentHashMap<>();

    static {
        MongoType[] var0 = values();
        int var1 = var0.length;

        for (int var2 = 0; var2 < var1; ++var2) {
            MongoType cur = var0[var2];
            ID_CACHE[cur.getId()] = cur;
            NAME_CACHE.put(cur.getTxt(), cur);
        }

    }

    String txt;
    Integer id;

    MongoType(String n, Integer id) {
        this.txt = n;
        this.id = id;
    }

    public static MongoType findByValue(int value) {
        return ID_CACHE[value & 255];
    }

    public static MongoType findByTxt(String txt) {
        return NAME_CACHE.get(txt);
    }

    public String getTxt() {
        return txt;
    }

    public Integer getId() {
        return id;
    }
}
