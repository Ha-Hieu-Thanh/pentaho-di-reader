package com.app.report;

import com.app.model.ColumnUsage;
import com.app.model.ExtractionResult;
import com.app.model.FileDetail;
import com.app.model.TableUsage;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for ExcelReportGenerator.
 */
public class ExcelReportGeneratorTest {

    private ExcelReportGenerator generator;
    private File tempDir;

    @Before
    public void setUp() throws Exception {
        generator = new ExcelReportGenerator();
        tempDir = Files.createTempDirectory("excel_test").toFile();
        tempDir.deleteOnExit();
    }

    @Test
    public void testGenerateReport_withData() throws Exception {
        ExtractionResult result = buildSampleResult();

        File outputFile = new File(tempDir, "report.xlsx");
        generator.generate(result, outputFile);

        assertTrue("Report file should be created", outputFile.exists());
        assertTrue("Report file should not be empty", outputFile.length() > 0);
    }

    @Test
    public void testGenerateReport_emptyResult() throws Exception {
        ExtractionResult empty = new ExtractionResult();

        File outputFile = new File(tempDir, "empty_report.xlsx");
        generator.generate(empty, outputFile);

        assertTrue("Empty report should still be created", outputFile.exists());
        assertTrue("Empty report should not be null-size", outputFile.length() >= 0);
    }

    @Test
    public void testGenerateReport_overwritesExistingFile() throws Exception {
        ExtractionResult result = buildSampleResult();

        File outputFile = new File(tempDir, "overwrite_report.xlsx");
        // Create file first
        outputFile.createNewFile();

        generator.generate(result, outputFile);

        assertTrue("Report should overwrite existing file", outputFile.exists());
        assertTrue("Report should have content after overwrite", outputFile.length() > 0);
    }

    @Test
    public void testTableUsagesSortedByCount_descending() {
        ExtractionResult result = new ExtractionResult();

        FileDetail fd = new FileDetail("test.ktr", "/tmp/test.ktr");
        result.addFileDetail(fd);

        // Add same table multiple times
        result.recordTable("orders", fd);
        result.recordTable("orders", fd);
        result.recordTable("orders", fd);

        result.recordTable("customers", fd);
        result.recordTable("customers", fd);

        result.recordTable("products", fd);

        List<TableUsage> sorted = result.getTableUsagesSortedByCount();
        assertEquals("orders", sorted.get(0).getTableName());     // 3
        assertEquals("customers", sorted.get(1).getTableName()); // 2
        assertEquals("products", sorted.get(2).getTableName());  // 1
    }

    @Test
    public void testColumnUsagesSortedByTableAndCount() {
        ExtractionResult result = new ExtractionResult();
        FileDetail fd = new FileDetail("test.ktr", "/tmp/test.ktr");
        result.addFileDetail(fd);

        result.recordColumn("customers", "id", fd);
        result.recordColumn("customers", "id", fd);
        result.recordColumn("customers", "id", fd);
        result.recordColumn("customers", "name", fd);
        result.recordColumn("customers", "name", fd);
        result.recordColumn("customers", "email", fd);

        List<ColumnUsage> cols = result.getColumnUsagesSortedByTableAndCount();

        assertEquals("customers", cols.get(0).getTableName());
        assertEquals("id", cols.get(0).getColumnName());
        assertEquals(3, cols.get(0).getUsageCount());

        assertEquals("customers", cols.get(1).getTableName());
        assertEquals("name", cols.get(1).getColumnName());
        assertEquals(2, cols.get(1).getUsageCount());

        assertEquals("customers", cols.get(2).getTableName());
        assertEquals("email", cols.get(2).getColumnName());
        assertEquals(1, cols.get(2).getUsageCount());
    }

    @Test
    public void testFileDetail_tablesAndColumnsCommaSeparated() {
        FileDetail fd = new FileDetail("etl_orders.ktr", "/tmp/etl_orders.ktr");
        fd.addTable("orders");
        fd.addTable("customers");
        fd.addTable("products");
        fd.addColumn("id");
        fd.addColumn("name");
        fd.addColumn("order_date");

        assertEquals("customers, orders, products", fd.getTablesAsString());
        assertEquals("id, name, order_date", fd.getColumnsAsString());
    }

    private ExtractionResult buildSampleResult() {
        ExtractionResult result = new ExtractionResult();

        FileDetail fd1 = new FileDetail("etl_orders.ktr", "/tmp/etl_orders.ktr");
        result.addFileDetail(fd1);
        result.recordTable("customers", fd1);
        result.recordTable("customers", fd1);
        result.recordTable("orders", fd1);
        result.recordColumn("customers", "id", fd1);
        result.recordColumn("customers", "name", fd1);
        result.recordColumn("orders", "id", fd1);

        FileDetail fd2 = new FileDetail("etl_sales.kjb", "/tmp/etl_sales.kjb");
        result.addFileDetail(fd2);
        result.recordTable("orders", fd2);
        result.recordTable("products", fd2);
        result.recordColumn("orders", "total_amount", fd2);
        result.recordColumn("products", "price", fd2);

        return result;
    }
}
