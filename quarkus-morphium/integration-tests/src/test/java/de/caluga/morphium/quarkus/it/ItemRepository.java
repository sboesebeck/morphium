package de.caluga.morphium.quarkus.it;

import jakarta.data.Limit;
import jakarta.data.repository.By;
import jakarta.data.repository.CrudRepository;
import jakarta.data.repository.Delete;
import jakarta.data.repository.Find;
import jakarta.data.repository.Insert;
import jakarta.data.repository.OrderBy;
import jakarta.data.repository.Repository;
import jakarta.data.repository.Save;
import jakarta.data.repository.Update;

import java.util.List;

/**
 * Jakarta Data repository for {@link ItemEntity}.
 * Extends CrudRepository for full CRUD support plus custom query methods.
 */
@Repository
public interface ItemRepository extends CrudRepository<ItemEntity, String> {

    List<ItemEntity> findByName(String name);

    List<ItemEntity> findByPriceGreaterThan(double minPrice);

    long countByTag(String tag);

    boolean existsByName(String name);

    @Find
    List<ItemEntity> searchByTag(@By("tag") String tag);

    @Find
    ItemEntity findOneByName(@By("name") String name);

    @Find
    @OrderBy("price")
    List<ItemEntity> findByTagSortedByPrice(@By("tag") String tag);

    @Find
    @OrderBy(value = "price", descending = true)
    List<ItemEntity> findByTagSortedByPriceDesc(@By("tag") String tag);

    @Find
    List<ItemEntity> findWithLimit(@By("tag") String tag, Limit limit);

    @Delete
    void removeByTag(@By("tag") String tag);

    @Insert
    ItemEntity addItem(ItemEntity item);

    @Insert
    List<ItemEntity> addItems(List<ItemEntity> items);

    @Save
    ItemEntity storeItem(ItemEntity item);

    @Update
    ItemEntity updateItem(ItemEntity item);
}
