package com.app.report;

import com.app.model.ColumnUsage;
import com.app.model.ExtractionResult;
import com.app.model.FileDetail;
import com.app.model.TableUsage;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.awt.Color;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Generates a 3-sheet Excel report (.xlsx) from an ExtractionResult.
 *
 * Sheet 1 - Summary:        Table | Usage Count
 * Sheet 2 - Table Detail:  Table | Column | Usage Count
 * Sheet 3 - File Detail:   File Name | Tables | Columns
 */
public class ExcelReportGenerator {

    private static final Color HEADER_BG_COLOR = new Color(68, 114, 196); // Blue
    private static final Color TOTAL_BG_COLOR  = new Color(217, 217, 217); // Light gray

    /**
     * Generates the Excel report and saves it to the specified output file.
     *
     * @param result     the aggregated extraction result
     * @param outputFile the target .xlsx file
     * @throws IOException if file writing fails
     */
    public void generate(ExtractionResult result, File outputFile) throws IOException {
        XSSFWorkbook workbook = new XSSFWorkbook();

        createSummarySheet(workbook, result.getTableUsagesSortedByCount());
        createTableDetailSheet(workbook, result.getColumnUsagesSortedByTableAndCount());
        createFileDetailSheet(workbook, result.getFileDetails());

        autoSizeAllColumns(workbook);

        try (FileOutputStream fos = new FileOutputStream(outputFile)) {
            workbook.write(fos);
        } finally {
            workbook.close();
        }
    }

    // ─── Sheet 1: Summary ────────────────────────────────────────────────────

    private void createSummarySheet(XSSFWorkbook workbook, List<TableUsage> tableUsages) {
        Sheet sheet = workbook.createSheet("Summary");
        CellStyle headerStyle = buildHeaderStyle(workbook);

        Row header = sheet.createRow(0);
        Cell h1 = header.createCell(0); h1.setCellValue("Table");       h1.setCellStyle(headerStyle);
        Cell h2 = header.createCell(1); h2.setCellValue("Usage Count"); h2.setCellStyle(headerStyle);

        int rowIdx = 1;
        for (TableUsage tu : tableUsages) {
            Row row = sheet.createRow(rowIdx++);
            row.createCell(0).setCellValue(tu.getTableName());
            row.createCell(1).setCellValue(tu.getUsageCount());
        }

        // Total row
        Row totalRow = sheet.createRow(rowIdx);
        Cell totalLabel = totalRow.createCell(0); totalLabel.setCellValue("Total");
        Cell totalValue = totalRow.createCell(1);
        totalValue.setCellValue(tableUsages.stream().mapToInt(TableUsage::getUsageCount).sum());
        CellStyle totalStyle = buildTotalStyle(workbook);
        totalLabel.setCellStyle(totalStyle);
        totalValue.setCellStyle(totalStyle);
    }

    // ─── Sheet 2: Table Detail ────────────────────────────────────────────────

    private void createTableDetailSheet(XSSFWorkbook workbook, List<ColumnUsage> columnUsages) {
        Sheet sheet = workbook.createSheet("Table Detail");
        CellStyle headerStyle = buildHeaderStyle(workbook);

        Row header = sheet.createRow(0);
        Cell h1 = header.createCell(0); h1.setCellValue("Table");       h1.setCellStyle(headerStyle);
        Cell h2 = header.createCell(1); h2.setCellValue("Column");     h2.setCellStyle(headerStyle);
        Cell h3 = header.createCell(2); h3.setCellValue("Usage Count"); h3.setCellStyle(headerStyle);

        int rowIdx = 1;
        for (ColumnUsage cu : columnUsages) {
            Row row = sheet.createRow(rowIdx++);
            row.createCell(0).setCellValue(cu.getTableName());
            row.createCell(1).setCellValue(cu.getColumnName());
            row.createCell(2).setCellValue(cu.getUsageCount());
        }
    }

    // ─── Sheet 3: File Detail ─────────────────────────────────────────────────

    private void createFileDetailSheet(XSSFWorkbook workbook, List<FileDetail> fileDetails) {
        Sheet sheet = workbook.createSheet("File Detail");
        CellStyle headerStyle = buildHeaderStyle(workbook);

        Row header = sheet.createRow(0);
        Cell h1 = header.createCell(0); h1.setCellValue("File Name"); h1.setCellStyle(headerStyle);
        Cell h2 = header.createCell(1); h2.setCellValue("Tables");    h2.setCellStyle(headerStyle);
        Cell h3 = header.createCell(2); h3.setCellValue("Columns");   h3.setCellStyle(headerStyle);

        int rowIdx = 1;
        for (FileDetail fd : fileDetails) {
            Row row = sheet.createRow(rowIdx++);
            row.createCell(0).setCellValue(fd.getFileName());
            row.createCell(1).setCellValue(fd.getTablesAsString());
            row.createCell(2).setCellValue(fd.getColumnsAsString());
        }
    }

    // ─── Cell Styles ─────────────────────────────────────────────────────────

    private CellStyle buildHeaderStyle(XSSFWorkbook workbook) {
        XSSFCellStyle style = workbook.createCellStyle();
        style.setFillForegroundColor(new XSSFColor(HEADER_BG_COLOR, null));
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);

        Font font = workbook.createFont();
        font.setBold(true);
        font.setColor(IndexedColors.WHITE.getIndex());
        font.setFontHeightInPoints((short) 11);
        style.setFont(font);

        style.setBorderTop(BorderStyle.THIN);
        style.setBorderBottom(BorderStyle.THIN);
        style.setBorderLeft(BorderStyle.THIN);
        style.setBorderRight(BorderStyle.THIN);

        return style;
    }

    private CellStyle buildTotalStyle(XSSFWorkbook workbook) {
        XSSFCellStyle style = workbook.createCellStyle();
        style.setFillForegroundColor(new XSSFColor(TOTAL_BG_COLOR, null));
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);

        Font font = workbook.createFont();
        font.setBold(true);
        style.setFont(font);

        style.setBorderTop(BorderStyle.MEDIUM);
        return style;
    }

    // ─── Auto-size ─────────────────────────────────────────────────────────────

    private void autoSizeAllColumns(XSSFWorkbook workbook) {
        for (Sheet sheet : workbook) {
            Row headerRow = sheet.getRow(0);
            if (headerRow == null) continue;
            int colCount = headerRow.getLastCellNum();
            for (int i = 0; i < colCount; i++) {
                sheet.autoSizeColumn(i);
                int currentWidth = sheet.getColumnWidth(i);
                int maxWidth = 50 * 256; // 50 characters
                if (currentWidth > maxWidth) {
                    sheet.setColumnWidth(i, maxWidth);
                }
            }
        }
    }
}
