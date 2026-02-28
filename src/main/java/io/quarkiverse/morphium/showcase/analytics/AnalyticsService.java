/*
 * Copyright 2025 The Quarkiverse Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.quarkiverse.morphium.showcase.analytics;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.Expr;
import io.quarkiverse.morphium.showcase.analytics.entity.SalesRecord;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class AnalyticsService {

    @Inject
    Morphium morphium;

    @SuppressWarnings({"unchecked", "rawtypes"})
    public List<Map<String, Object>> salesByRegion() {
        Aggregator<SalesRecord, Map> agg = morphium.createAggregator(SalesRecord.class, Map.class);
        agg.group("$region")
                .sum("totalRevenue", Map.of("$multiply", List.of("$unit_price", "$quantity")))
                .sum("totalOrders", 1)
                .avg("avgOrderValue", Map.of("$multiply", List.of("$unit_price", "$quantity")))
                .end();
        agg.sort(Map.of("totalRevenue", -1));
        return (List<Map<String, Object>>) (List<?>) agg.aggregate();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public List<Map<String, Object>> salesByProduct() {
        Aggregator<SalesRecord, Map> agg = morphium.createAggregator(SalesRecord.class, Map.class);
        agg.group("$product_name")
                .sum("totalQuantity", "$quantity")
                .sum("totalRevenue", Map.of("$multiply", List.of("$unit_price", "$quantity")))
                .avg("avgPrice", "$unit_price")
                .end();
        agg.sort(Map.of("totalRevenue", -1));
        agg.limit(10);
        return (List<Map<String, Object>>) (List<?>) agg.aggregate();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public List<Map<String, Object>> monthlySales() {
        Aggregator<SalesRecord, Map> agg = morphium.createAggregator(SalesRecord.class, Map.class);
        agg.project(Map.of(
                "month", Map.of("$month", "$sale_date"),
                "year", Map.of("$year", "$sale_date"),
                "revenue", Map.of("$multiply", List.of("$unit_price", "$quantity"))
        ));
        agg.group(Map.of("year", "$year", "month", "$month"))
                .sum("totalRevenue", "$revenue")
                .sum("totalOrders", 1)
                .end();
        agg.sort(Map.of("_id.year", 1, "_id.month", 1));
        return (List<Map<String, Object>>) (List<?>) agg.aggregate();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public List<Map<String, Object>> topSalesReps() {
        Aggregator<SalesRecord, Map> agg = morphium.createAggregator(SalesRecord.class, Map.class);
        agg.group("$sales_rep")
                .sum("totalSales", 1)
                .sum("totalRevenue", Map.of("$multiply", List.of("$unit_price", "$quantity")))
                .end();
        agg.sort(Map.of("totalRevenue", -1));
        agg.limit(5);
        return (List<Map<String, Object>>) (List<?>) agg.aggregate();
    }

    public long totalRecords() {
        return morphium.createQueryFor(SalesRecord.class).countAll();
    }

    public double totalRevenue() {
        List<Map<String, Object>> result = salesByRegion();
        return result.stream()
                .mapToDouble(m -> ((Number) m.getOrDefault("totalRevenue", 0)).doubleValue())
                .sum();
    }

    @SuppressWarnings("unchecked")
    public List<String> distinctRegions() {
        return (List<String>) (List<?>) morphium.createQueryFor(SalesRecord.class)
                .distinct("region");
    }

    @SuppressWarnings("unchecked")
    public List<String> distinctProducts() {
        return (List<String>) (List<?>) morphium.createQueryFor(SalesRecord.class)
                .distinct("product_name");
    }

    public void seedData() {
        if (morphium.createQueryFor(SalesRecord.class).countAll() > 0) return;

        String[] products = {"Laptop Pro 15", "Wireless Mouse", "Standing Desk", "Mechanical Keyboard", "Monitor 27\"", "USB-C Hub"};
        double[] prices = {1299.99, 29.99, 449.00, 89.99, 399.99, 49.99};
        String[] regions = {"Europe", "North America", "Asia Pacific", "Latin America"};
        String[] reps = {"Alice", "Bob", "Charlie", "Diana", "Eve"};

        java.util.Random rand = new java.util.Random(42);
        ArrayList<SalesRecord> records = new ArrayList<>();

        for (int i = 0; i < 200; i++) {
            int pi = rand.nextInt(products.length);
            records.add(SalesRecord.builder()
                    .productName(products[pi])
                    .unitPrice(prices[pi])
                    .quantity(1 + rand.nextInt(10))
                    .discount(rand.nextDouble() * 0.2)
                    .region(regions[rand.nextInt(regions.length)])
                    .salesRep(reps[rand.nextInt(reps.length)])
                    .saleDate(LocalDateTime.now().minusDays(rand.nextInt(365)))
                    .categories(List.of("electronics"))
                    .build());
        }
        morphium.storeList(records);
    }
}