package de.caluga.morphium;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 11:48
 * <p/>
 * Mongo Data types according to documentation:
 * see also: http://www.mongodb.org/display/DOCS/Advanced+Queries#AdvancedQueries-ConditionalOperators
 */
public enum MongoType {
    DOUBLE(1),
    STRING(2),
    OBJECT(3),
    ARRAY(4),
    BINARY(5),
    OBJECT_ID(7),
    BOOLEAN(8),
    DATE(9),
    NULL(10),
    REGEX(11),
    JS_CODE(13),
    SYMBOL(14),
    JS_CODE_W_SCOPE(15),
    INTEGER32(16),
    TIMESTAMP(17),
    INTEGER64(18),
    MIN_KEY(255),
    MAX_KEY(127);

    private final int number;

    MongoType(int nr) {
        number = nr;
    }

    public int getNumber() {
        return number;
    }
    //
    //    Object	 3
    //    Array	 4
    //    Binary data	 5
    //    Object id	 7
    //    Boolean	 8
    //    Date	 9
    //    Null	 10
    //    Regular expression	 11
    //    JavaScript code	 13
    //    Symbol	 14
    //    JavaScript code with scope	 15
    //            32-bit integer	 16
    //    Timestamp	 17
    //            64-bit integer	 18
    //    Min key	 255
    //    Max key	 127
}
