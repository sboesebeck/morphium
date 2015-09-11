package de.caluga.morphium.query;

import java.util.Iterator;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 23.11.12
 * Time: 11:35
 * <p/>
 * iterator, makes paging through huge collections a lot easier. Default Window (~page) size is 10.<p/>
 * This iterator only reads as many objects from mongo as specified in window-size. It can be used like a
 * normal java iterator:
 * <code>
 * for (Object o:query.asIterable()) {
 * //do something here
 * };
 * </code>
 */
@SuppressWarnings("UnusedDeclaration")
public interface MorphiumIterator<T> extends Iterable<T>, Iterator<T> {
    void setWindowSize(int sz);

    int getWindowSize();

    void setQuery(Query<T> q);

    Query<T> getQuery();

    /**
     * retruns the number of elements now in buffer. Max windowsize
     *
     * @return list
     */
    int getCurrentBufferSize();

    /**
     * get the current buffer. Maximum length is specified windowsize
     *
     * @return list
     */
    List<T> getCurrentBuffer();

    /**
     * how many elements are to be processed.
     * Attention: this count is not updated. It shows how many elements are there at the beginning of the interation!
     *
     * @return count
     */
    long getCount();

    /**
     * returns current cursor position
     *
     * @return int
     */
    int getCursor();

    /**
     * move the cursor position ahead
     *
     * @param jump number of elements to jump
     */
    void ahead(int jump);

    /**
     * get back some positions
     *
     * @param jump number of elements to jump back
     */
    void back(int jump);

    void setNumberOfPrefetchWindows(int n);

    int getNumberOfAvailableThreads();

    int getNumberOfThreads();

    boolean isMultithreaddedAccess();

    void setMultithreaddedAccess(boolean mu);
}
