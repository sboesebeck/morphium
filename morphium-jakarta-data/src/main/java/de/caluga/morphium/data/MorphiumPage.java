package de.caluga.morphium.data;

import jakarta.data.page.Page;
import jakarta.data.page.PageRequest;

import java.util.Iterator;
import java.util.List;

/**
 * Morphium-backed implementation of Jakarta Data's {@link Page}.
 * <p>
 * Returned as the result of offset-based pagination: {@link AbstractMorphiumRepository#doFindAllPaged},
 * {@link FindMethodBridge}, and {@link JdqlMethodBridge} construct instances of this class once the
 * page content and (optionally) the total element count have been fetched via a Morphium
 * {@link de.caluga.morphium.query.Query}. Keyset (cursor-based) pagination uses
 * {@code jakarta.data.page.impl.CursoredPageRecord} directly instead, with cursor extraction
 * handled by {@link CursorHelper}.
 *
 * @param <T> the entity type
 */
public class MorphiumPage<T> implements Page<T> {

    private final List<T> content;
    private final long totalElements;
    private final PageRequest pageRequest;

    /**
     * Creates a new page.
     *
     * @param content       the entities on this page
     * @param totalElements the total number of matching entities across all pages, or a negative
     *                      value if the total was not requested (see {@link #hasTotals()})
     * @param pageRequest   the page request this page was created for
     */
    public MorphiumPage(List<T> content, long totalElements, PageRequest pageRequest) {
        this.content = content;
        this.totalElements = totalElements;
        this.pageRequest = pageRequest;
    }

    /**
     * @return the entities on this page
     */
    @Override
    public List<T> content() {
        return content;
    }

    /**
     * @return true if the total element/page count was requested and is available
     */
    @Override
    public boolean hasTotals() {
        return totalElements >= 0;
    }

    /**
     * @return the total number of matching entities across all pages
     * @throws IllegalStateException if the total was not requested ({@link #hasTotals()} is false)
     */
    @Override
    public long totalElements() {
        if (!hasTotals()) {
            throw new IllegalStateException("Total not requested. Use PageRequest.withTotal().");
        }
        return totalElements;
    }

    /**
     * @return the total number of pages, given the page size of {@link #pageRequest()}
     * @throws IllegalStateException if the total was not requested ({@link #hasTotals()} is false)
     */
    @Override
    public long totalPages() {
        if (!hasTotals()) {
            throw new IllegalStateException("Total not requested. Use PageRequest.withTotal().");
        }
        if (pageRequest.size() <= 0) return 1;
        return (totalElements + pageRequest.size() - 1) / pageRequest.size();
    }

    /**
     * @return the page request this page was created for
     */
    @Override
    public PageRequest pageRequest() {
        return pageRequest;
    }

    /**
     * @return the page request for the next page, or {@code null} if there is no next page
     */
    @Override
    public PageRequest nextPageRequest() {
        if (!hasNext()) {
            return null;
        }
        return PageRequest.ofPage(pageRequest.page() + 1, pageRequest.size(), pageRequest.requestTotal());
    }

    /**
     * @return the page request for the previous page, or {@code null} if this is the first page
     */
    @Override
    public PageRequest previousPageRequest() {
        if (pageRequest.page() <= 1) {
            return null;
        }
        return PageRequest.ofPage(pageRequest.page() - 1, pageRequest.size(), pageRequest.requestTotal());
    }

    /**
     * @return true if this page has at least one entity
     */
    @Override
    public boolean hasContent() {
        return !content.isEmpty();
    }

    /**
     * @return the number of entities on this page
     */
    @Override
    public int numberOfElements() {
        return content.size();
    }

    /**
     * @return true if a next page is likely to exist. When {@link #hasTotals()} is false this is a
     *         heuristic based on whether this page is full, since the total page count is unknown
     */
    @Override
    public boolean hasNext() {
        if (!hasTotals()) {
            // If no totals, check if we got a full page (heuristic)
            return content.size() >= pageRequest.size();
        }
        return pageRequest.page() < totalPages();
    }

    /**
     * @return true if this is not the first page
     */
    @Override
    public boolean hasPrevious() {
        return pageRequest.page() > 1;
    }

    /**
     * @return an iterator over the entities on this page
     */
    @Override
    public Iterator<T> iterator() {
        return content.iterator();
    }
}
