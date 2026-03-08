package io.quarkiverse.morphium.showcase.blog;

import de.caluga.morphium.driver.MorphiumId;
import io.quarkiverse.morphium.showcase.blog.entity.BlogPost;
import jakarta.data.page.Page;
import jakarta.data.page.PageRequest;
import jakarta.data.repository.BasicRepository;
import jakarta.data.repository.Param;
import jakarta.data.repository.Query;
import jakarta.data.repository.Repository;

import java.util.List;

/**
 * Jakarta Data repository for {@link BlogPost}.
 *
 * <p>Demonstrates JDQL (@Query) for blog-specific queries and pagination.</p>
 *
 * <p>All Morphium ORM features work transparently through this repository:
 * {@code @Version} (optimistic locking), {@code @CreationTime}, {@code @Reference}
 * (lazy/eager), and lifecycle callbacks ({@code @PreStore}, {@code @PostLoad}) all
 * fire normally because the generated implementation delegates to {@code morphium.store()},
 * {@code morphium.findById()} etc.</p>
 *
 * <p>For operations that have no Jakarta Data equivalent (adding/removing embedded comments,
 * aggregations, atomic field updates), use the direct Morphium API alongside this repository.</p>
 */
@Repository
public interface BlogPostRepository extends BasicRepository<BlogPost, MorphiumId> {

    // ---- Query Derivation ----

    /** Find all published posts — equivalent to {@code .f("published").eq(true).asList()} */
    List<BlogPost> findByPublishedTrue();

    /** Find all unpublished drafts */
    List<BlogPost> findByPublishedFalse();

    /** Count published posts */
    long countByPublishedTrue();

    /** Check if a post with this title exists */
    boolean existsByTitle(String title);

    // ---- @Query / JDQL ----

    /** Published posts, newest first — a very common blog query */
    @Query("WHERE published = true ORDER BY createdAt DESC")
    List<BlogPost> findRecentPublished();

    /** Search by title pattern (case-insensitive via LIKE) */
    @Query("WHERE title LIKE :pattern ORDER BY createdAt DESC")
    List<BlogPost> searchByTitle(@Param("pattern") String titlePattern);

    /** Count published posts via JDQL */
    @Query("WHERE published = true")
    long countPublished();

    /** Paginated published posts — combines JDQL filter with PageRequest */
    @Query("WHERE published = true ORDER BY createdAt DESC")
    Page<BlogPost> findPublishedPaged(PageRequest pageRequest);
}
