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
    @SuppressWarnings("unused")DOUBLE(1),
    @SuppressWarnings("unused")STRING(2),
    @SuppressWarnings("unused")OBJECT(3),
    @SuppressWarnings("unused")ARRAY(4),
    @SuppressWarnings("unused")BINARY(5),
    @SuppressWarnings("unused")OBJECT_ID(7),
    @SuppressWarnings("unused")BOOLEAN(8),
    @SuppressWarnings("unused")DATE(9),
    @SuppressWarnings("unused")NULL(10),
    @SuppressWarnings("unused")REGEX(11),
    @SuppressWarnings("unused")JS_CODE(13),
    @SuppressWarnings("unused")SYMBOL(14),
    @SuppressWarnings("unused")JS_CODE_W_SCOPE(15),
    @SuppressWarnings("unused")INTEGER32(16),
    @SuppressWarnings("unused")TIMESTAMP(17),
    @SuppressWarnings("unused")INTEGER64(18),
    @SuppressWarnings("unused")MIN_KEY(255),
    @SuppressWarnings("unused")MAX_KEY(127);

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
