package de.caluga.morphium.query;

import java.util.Iterator;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 23.11.12
 * Time: 11:35
 * <p/>
 * TODO: Add documentation here
 */
public interface MorphiumIterator<T> extends Iterable<T>, Iterator<T> {
    public void setWindowSize(int sz);

    public int getWindowSize();

    public void setQuery(Query<T> q);

    public Query<T> getQuery();

    /**
     * retruns the number of elements now in buffer. Max windowsize
     *
     * @return
     */
    public int getCurrentBufferSize();

    /**
     * get the current buffer. Maximum length is specified windowsize
     *
     * @return
     */
    public List<T> getCurrentBuffer();

    /**
     * how many elements are to be processed.
     * Attention: this count is not updated. It shows how many elements are there at the beginning of the interation!
     *
     * @return
     */
    public long getCount();

    /**
     * returns current cursor position
     *
     * @return
     */
    public int getCursor();

    /**
     * move the cursor position ahead
     *
     * @param jump
     */
    public void ahead(int jump);

    /**
     * get back some positions
     *
     * @param jump
     */
    public void back(int jump);

}
