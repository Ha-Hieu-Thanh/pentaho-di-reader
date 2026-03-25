package com.app.extractor;

import com.app.model.ColumnUsage;
import com.app.model.ExtractionResult;
import com.app.model.FileDetail;
import com.app.model.TableUsage;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for SqlExtractor.
 */
public class SqlExtractorTest {

    private SqlExtractor extractor;
    private ExtractionResult result;

    @Before
    public void setUp() {
        extractor = new SqlExtractor();
        result = new ExtractionResult();
    }

    // ─── SELECT tests ────────────────────────────────────────────────────────

    @Test
    public void testSimpleSelect_extractsTableAndColumns() {
        FileDetail fd = new FileDetail("test.ktr", "/tmp/test.ktr");
        extractor.extract("SELECT id, name, email FROM customers", fd, result);

        List<TableUsage> tables = result.getTableUsagesSortedByCount();
        assertEquals(1, tables.size());
        assertEquals("customers", tables.get(0).getTableName());
        assertEquals(1, tables.get(0).getUsageCount());
    }

    @Test
    public void testSelectWithJoin_extractsBothTables() {
        FileDetail fd = new FileDetail("test.ktr", "/tmp/test.ktr");
        extractor.extract(
            "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id",
            fd, result);

        List<TableUsage> tables = result.getTableUsagesSortedByCount();
        assertEquals(2, tables.size());
        // Both tables should appear
        assertTrue(tables.stream().anyMatch(t -> t.getTableName().equals("orders")));
        assertTrue(tables.stream().anyMatch(t -> t.getTableName().equals("customers")));
    }

    @Test
    public void testSelectWithWhere_extractsColumns() {
        FileDetail fd = new FileDetail("test.ktr", "/tmp/test.ktr");
        extractor.extract(
            "SELECT id, name FROM products WHERE price > 100 AND category = 'Electronics'",
            fd, result);

        List<ColumnUsage> cols = result.getColumnUsagesSortedByTableAndCount();
        assertFalse(cols.isEmpty());
        assertTrue(cols.stream().anyMatch(c -> c.getColumnName().equals("price")));
        assertTrue(cols.stream().anyMatch(c -> c.getColumnName().equals("category")));
    }

    // ─── INSERT tests ────────────────────────────────────────────────────────

    @Test
    public void testInsert_extractsTargetTable() {
        FileDetail fd = new FileDetail("test.ktr", "/tmp/test.ktr");
        extractor.extract(
            "INSERT INTO order_items (id, order_id, product_id, quantity) VALUES (1, 10, 5, 2)",
            fd, result);

        List<TableUsage> tables = result.getTableUsagesSortedByCount();
        assertTrue(tables.stream().anyMatch(t -> t.getTableName().equals("order_items")));
    }

    @Test
    public void testInsertSelect_extractsBothTables() {
        FileDetail fd = new FileDetail("test.ktr", "/tmp/test.ktr");
        extractor.extract(
            "INSERT INTO archived_orders (id, name) SELECT id, name FROM orders WHERE status = 'archived'",
            fd, result);

        List<TableUsage> tables = result.getTableUsagesSortedByCount();
        assertEquals(2, tables.size());
    }

    // ─── UPDATE tests ─────────────────────────────────────────────────────────

    @Test
    public void testUpdate_extractsTable() {
        FileDetail fd = new FileDetail("test.ktr", "/tmp/test.ktr");
        extractor.extract(
            "UPDATE products SET price = 999 WHERE id = 5",
            fd, result);

        List<TableUsage> tables = result.getTableUsagesSortedByCount();
        assertEquals(1, tables.size());
        assertEquals("products", tables.get(0).getTableName());
    }

    // ─── DELETE tests ─────────────────────────────────────────────────────────

    @Test
    public void testDelete_extractsTable() {
        FileDetail fd = new FileDetail("test.ktr", "/tmp/test.ktr");
        extractor.extract("DELETE FROM logs WHERE created_at < '2024-01-01'", fd, result);

        List<TableUsage> tables = result.getTableUsagesSortedByCount();
        assertEquals(1, tables.size());
        assertEquals("logs", tables.get(0).getTableName());
    }

    // ─── Multiple statements (semicolon-separated) ───────────────────────────

    @Test
    public void testMultipleStatementsSeparatedBySemicolon() {
        FileDetail fd = new FileDetail("test.ktr", "/tmp/test.ktr");
        extractor.extract(
            "SELECT * FROM customers; SELECT * FROM orders; SELECT * FROM products",
            fd, result);

        List<TableUsage> tables = result.getTableUsagesSortedByCount();
        assertEquals(3, tables.size());
    }

    // ─── Empty/null handling ─────────────────────────────────────────────────

    @Test
    public void testNullSql_handledGracefully() {
        FileDetail fd = new FileDetail("test.ktr", "/tmp/test.ktr");
        extractor.extract(null, fd, result);
        assertTrue(result.getTableUsagesSortedByCount().isEmpty());
    }

    @Test
    public void testEmptySql_handledGracefully() {
        FileDetail fd = new FileDetail("test.ktr", "/tmp/test.ktr");
        extractor.extract("", fd, result);
        assertTrue(result.getTableUsagesSortedByCount().isEmpty());
    }

    // ─── Aggregation ─────────────────────────────────────────────────────────

    @Test
    public void testDuplicateTableInMultipleQueries_countsAggregated() {
        FileDetail fd = new FileDetail("test.ktr", "/tmp/test.ktr");
        extractor.extract("SELECT * FROM customers", fd, result);
        extractor.extract("SELECT * FROM customers WHERE id = 1", fd, result);
        extractor.extract("INSERT INTO customers (name) VALUES ('John')", fd, result);

        List<TableUsage> tables = result.getTableUsagesSortedByCount();
        assertEquals(1, tables.size());
        assertEquals("customers", tables.get(0).getTableName());
        assertEquals(3, tables.get(0).getUsageCount());
    }

    // ─── FileDetail tracking ─────────────────────────────────────────────────

    @Test
    public void testFileDetail_collectsTablesAndColumns() {
        FileDetail fd = new FileDetail("etl_orders.ktr", "/tmp/etl_orders.ktr");
        extractor.extract(
            "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id",
            fd, result);

        assertTrue(fd.getTables().contains("orders"));
        assertTrue(fd.getTables().contains("customers"));
        assertFalse(fd.getTables().isEmpty());
    }

    // ─── FileDetail tracking ─────────────────────────────────────────────────

    @Test
    public void testInvalidSql_fallsBackToRegex() {
        FileDetail fd = new FileDetail("test.ktr", "/tmp/test.ktr");
        // JSQLParser might not handle this complex CTE, but regex should catch the table
        extractor.extract("WITH tmp AS (SELECT id FROM orders) SELECT * FROM tmp", fd, result);

        List<TableUsage> tables = result.getTableUsagesSortedByCount();
        assertFalse(tables.isEmpty());
    }
}
