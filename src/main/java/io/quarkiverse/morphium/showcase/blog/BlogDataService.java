package io.quarkiverse.morphium.showcase.blog;

import de.caluga.morphium.driver.MorphiumId;
import io.quarkiverse.morphium.showcase.blog.entity.BlogPost;
import jakarta.data.page.Page;
import jakarta.data.page.PageRequest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.Optional;

/**
 * Jakarta Data counterpart to {@link BlogService}.
 *
 * <p>Shows which blog operations map cleanly to Jakarta Data, and where
 * the direct Morphium API is still needed.</p>
 *
 * <h3>Jakarta Data handles:</h3>
 * <ul>
 *   <li>Finding published/unpublished posts</li>
 *   <li>Title search via JDQL LIKE</li>
 *   <li>Pagination of published posts</li>
 *   <li>Count and exists checks</li>
 * </ul>
 *
 * <h3>Morphium API still needed for:</h3>
 * <ul>
 *   <li>@Reference resolution (author, reviewer) — automatic in Morphium, not in Jakarta Data</li>
 *   <li>Embedded comment manipulation (add/remove comments)</li>
 *   <li>@Version conflict handling (VersionMismatchException)</li>
 *   <li>@CreationTime / @LastChange / @LastAccess auto-timestamps</li>
 * </ul>
 */
@ApplicationScoped
public class BlogDataService {

    @Inject
    BlogPostRepository repository;

    // ---- Query Derivation ----

    public List<BlogPost> findPublished() {
        // Morphium: morphium.createQueryFor(BlogPost.class).f("published").eq(true).asList()
        return repository.findByPublishedTrue();
    }

    public List<BlogPost> findDrafts() {
        return repository.findByPublishedFalse();
    }

    public long countPublished() {
        // Morphium: morphium.createQueryFor(BlogPost.class).f("published").eq(true).countAll()
        return repository.countByPublishedTrue();
    }

    public boolean existsByTitle(String title) {
        return repository.existsByTitle(title);
    }

    // ---- JDQL ----

    public List<BlogPost> findRecentPublished() {
        // Morphium: .f("published").eq(true).sort(Map.of("createdAt", -1)).asList()
        // JDQL: WHERE published = true ORDER BY createdAt DESC
        return repository.findRecentPublished();
    }

    public List<BlogPost> searchByTitle(String pattern) {
        // Morphium: .f("title").matches("(?i)" + pattern).sort(Map.of("createdAt", -1)).asList()
        // JDQL: WHERE title LIKE :pattern ORDER BY createdAt DESC
        return repository.searchByTitle("%" + pattern + "%");
    }

    // ---- Pagination ----

    public Page<BlogPost> findPublishedPaged(int page, int size) {
        // Morphium: query.skip((page-1)*size).limit(size).asList() + separate countAll()
        // Jakarta Data: one call returns Page with total elements
        return repository.findPublishedPaged(PageRequest.ofPage(page, size, true));
    }

    // ---- CRUD ----

    public Optional<BlogPost> findById(String id) {
        return repository.findById(new MorphiumId(id));
    }

    public void deletePost(String id) {
        repository.findById(new MorphiumId(id)).ifPresent(repository::delete);
    }
}
