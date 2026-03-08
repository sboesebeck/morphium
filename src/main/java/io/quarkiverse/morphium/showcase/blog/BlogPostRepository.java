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
 * <p>Demonstrates JDQL (@Query) for blog-specific queries. Note that Jakarta Data cannot
 * handle {@code @Reference} resolution or {@code @Embedded} comment manipulation — those
 * features remain the domain of the direct Morphium API.</p>
 *
 * <h3>What Jakarta Data CAN do for Blog</h3>
 * <ul>
 *   <li>CRUD operations (save, findById, delete)</li>
 *   <li>Simple field queries (findByPublished, findByTitle)</li>
 *   <li>JDQL queries with WHERE, ORDER BY, LIKE, BETWEEN</li>
 *   <li>Pagination with Page/PageRequest</li>
 * </ul>
 *
 * <h3>What still requires direct Morphium API</h3>
 * <ul>
 *   <li>{@code @Reference} author/reviewer resolution (automatic in Morphium queries)</li>
 *   <li>Adding/removing embedded comments (requires load-modify-store pattern)</li>
 *   <li>{@code @Version} conflict handling with VersionMismatchException</li>
 *   <li>Lazy loading proxies ({@code @Reference(lazyLoading = true)})</li>
 * </ul>
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
