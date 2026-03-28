package com.app.extractor;

import com.app.model.ExtractionResult;
import com.app.model.FileDetail;
import com.app.model.ColumnUsage;
import org.junit.Test;
import static org.junit.Assert.*;

public class SqlExtractorDebugTest {

    @Test
    public void debugUpdateWithAlias() throws Exception {
        SqlExtractor ex = new SqlExtractor();

        // Simpler: single UPDATE SET with subquery, aliased table in both outer and inner
        String sql = "UPDATE products p SET p.total_sold = (" +
            "SELECT COALESCE(SUM(oi.quantity), 0) FROM order_items oi WHERE oi.product_id = p.id" +
            ") WHERE p.active = 1";

        ExtractionResult result = new ExtractionResult();
        FileDetail fd = new FileDetail("test.kjb", "/tmp/test.kjb");
        ex.extract(sql, fd, result);

        System.out.println("=== SIMPLE UPDATE SQL columns ===");
        for (ColumnUsage cu : result.getColumnUsagesSortedByTableAndCount()) {
            System.out.println("  " + cu.getTableName() + "." + cu.getColumnName() + " = " + cu.getUsageCount());
        }
    }

    @Test
    public void debugUpdateFull() throws Exception {
        SqlExtractor ex = new SqlExtractor();

        // FROM etl_sales.kjb - Update Product Stats (full query)
        String sql = "UPDATE products p SET p.total_sold = (" +
            "SELECT COALESCE(SUM(oi.quantity), 0) FROM order_items oi WHERE oi.product_id = p.id" +
            "), p.last_sold_date = (" +
            "SELECT MAX(o.order_date) FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE oi.product_id = p.id" +
            ") WHERE p.active = 1";

        ExtractionResult result = new ExtractionResult();
        FileDetail fd = new FileDetail("test.kjb", "/tmp/test.kjb");
        ex.extract(sql, fd, result);

        System.out.println("\n=== FULL UPDATE SQL columns ===");
        for (ColumnUsage cu : result.getColumnUsagesSortedByTableAndCount()) {
            System.out.println("  " + cu.getTableName() + "." + cu.getColumnName() + " = " + cu.getUsageCount());
        }

        assertFalse("products.p should NOT appear",
            result.getColumnUsagesSortedByTableAndCount().stream()
                .anyMatch(cu -> "p".equals(cu.getTableName())));
    }
}
