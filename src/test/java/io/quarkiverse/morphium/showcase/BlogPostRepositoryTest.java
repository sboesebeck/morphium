package io.quarkiverse.morphium.showcase;

import de.caluga.morphium.Morphium;
import io.quarkiverse.morphium.showcase.blog.BlogPostRepository;
import io.quarkiverse.morphium.showcase.blog.BlogDataService;
import io.quarkiverse.morphium.showcase.blog.entity.Author;
import io.quarkiverse.morphium.showcase.blog.entity.BlogPost;
import jakarta.data.page.Page;
import jakarta.data.page.PageRequest;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link BlogPostRepository} and {@link BlogDataService}.
 */
@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BlogPostRepositoryTest {

    @Inject
    BlogPostRepository repository;

    @Inject
    BlogDataService dataService;

    @Inject
    Morphium morphium;

    @BeforeEach
    void setUp() {
        morphium.dropCollection(BlogPost.class);
        morphium.dropCollection(Author.class);
    }

    private BlogPost createPost(String title, boolean published) {
        BlogPost post = BlogPost.builder()
                .title(title)
                .content("Content for " + title)
                .published(published)
                .tags(List.of("test"))
                .build();
        morphium.store(post);
        return post;
    }

    // ---- Query Derivation ----

    @Test
    @Order(1)
    @DisplayName("Query Derivation: findByPublishedTrue")
    void shouldFindPublished() {
        createPost("Published Post", true);
        createPost("Draft Post", false);

        List<BlogPost> published = repository.findByPublishedTrue();
        assertThat(published).hasSize(1);
        assertThat(published.get(0).getTitle()).isEqualTo("Published Post");
    }

    @Test
    @Order(2)
    @DisplayName("Query Derivation: countByPublishedTrue")
    void shouldCountPublished() {
        createPost("P1", true);
        createPost("P2", true);
        createPost("Draft", false);

        assertThat(repository.countByPublishedTrue()).isEqualTo(2);
    }

    @Test
    @Order(3)
    @DisplayName("Query Derivation: existsByTitle")
    void shouldCheckExistsByTitle() {
        createPost("My Post", true);

        assertThat(repository.existsByTitle("My Post")).isTrue();
        assertThat(repository.existsByTitle("Nonexistent")).isFalse();
    }

    // ---- JDQL ----

    @Test
    @Order(10)
    @DisplayName("JDQL: findRecentPublished")
    void shouldFindRecentPublished() {
        createPost("Draft", false);
        createPost("Published 1", true);
        createPost("Published 2", true);

        List<BlogPost> recent = repository.findRecentPublished();
        assertThat(recent).hasSize(2);
        assertThat(recent).allSatisfy(p -> assertThat(p.isPublished()).isTrue());
    }

    @Test
    @Order(11)
    @DisplayName("JDQL: searchByTitle (LIKE)")
    void shouldSearchByTitle() {
        createPost("Getting Started with Morphium", true);
        createPost("Advanced Queries", true);
        createPost("Morphium Caching", false);

        List<BlogPost> result = repository.searchByTitle("%Morphium%");
        assertThat(result).hasSize(2);
        assertThat(result).allSatisfy(p -> assertThat(p.getTitle()).containsIgnoringCase("Morphium"));
    }

    @Test
    @Order(12)
    @DisplayName("JDQL: countPublished")
    void shouldCountPublishedViaJdql() {
        createPost("P1", true);
        createPost("P2", true);
        createPost("Draft", false);

        assertThat(repository.countPublished()).isEqualTo(2);
    }

    @Test
    @Order(13)
    @DisplayName("JDQL: findPublishedPaged")
    void shouldFindPublishedPaged() {
        for (int i = 1; i <= 5; i++) {
            createPost("Post " + i, true);
        }
        createPost("Draft", false);

        Page<BlogPost> page = repository.findPublishedPaged(
                PageRequest.ofPage(1, 2, true));
        assertThat(page.content()).hasSize(2);
        assertThat(page.content()).allSatisfy(p -> assertThat(p.isPublished()).isTrue());
    }

    // ---- BlogDataService ----

    @Test
    @Order(20)
    @DisplayName("DataService: findRecentPublished via Jakarta Data")
    void shouldFindRecentPublishedViaService() {
        createPost("Published", true);
        createPost("Draft", false);

        List<BlogPost> recent = dataService.findRecentPublished();
        assertThat(recent).hasSize(1);
        assertThat(recent.get(0).getTitle()).isEqualTo("Published");
    }

    @Test
    @Order(21)
    @DisplayName("DataService: searchByTitle via Jakarta Data")
    void shouldSearchByTitleViaService() {
        createPost("Morphium Guide", true);
        createPost("Other Topic", true);

        List<BlogPost> result = dataService.searchByTitle("Morphium");
        assertThat(result).hasSize(1);
    }
}
