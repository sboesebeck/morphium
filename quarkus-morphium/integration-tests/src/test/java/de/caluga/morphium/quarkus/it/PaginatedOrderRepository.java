package de.caluga.morphium.quarkus.it;

import jakarta.data.Order;
import jakarta.data.page.CursoredPage;
import jakarta.data.page.PageRequest;
import jakarta.data.repository.BasicRepository;
import jakarta.data.repository.By;
import jakarta.data.repository.Find;
import jakarta.data.repository.OrderBy;
import jakarta.data.repository.Param;
import jakarta.data.repository.Query;
import jakarta.data.repository.Repository;

/**
 * Test repository for CursoredPage (keyset pagination).
 */
@Repository
public interface PaginatedOrderRepository extends BasicRepository<OrderEntity, String> {

    @Find
    @OrderBy("amount")
    @OrderBy("id")
    CursoredPage<OrderEntity> findPagedByStatus(@By("status") String status, PageRequest pageRequest);

    @Query("WHERE status = :status")
    @OrderBy("amount")
    @OrderBy("id")
    CursoredPage<OrderEntity> queryPagedByStatus(@Param("status") String status, PageRequest pageRequest);

    CursoredPage<OrderEntity> findAll(PageRequest pageRequest, Order<OrderEntity> sortBy);
}
