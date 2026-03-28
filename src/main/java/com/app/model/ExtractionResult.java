package com.app.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Aggregates extraction results from all processed files.
 * Holds global counts (across all files) and per-file details.
 */
public class ExtractionResult {
    // Global table usage: tableName -> total count
    private final Map<String, Integer> tableCounts = new TreeMap<>();

    // Global column usage: tableName -> (columnName -> count)
    private final Map<String, Map<String, Integer>> columnCounts = new TreeMap<>();

    // Per-file details
    private final List<FileDetail> fileDetails = new ArrayList<>();

    /**
     * Records a table occurrence within a file context.
     */
    public void recordTable(String tableName, FileDetail fileDetail) {
        if (tableName == null || tableName.trim().isEmpty()) return;

        String t = tableName.trim();
        tableCounts.put(t, tableCounts.getOrDefault(t, 0) + 1);

        // Also add to file detail
        fileDetail.addTable(t);

        // Ensure column map exists for this table
        columnCounts.putIfAbsent(t, new TreeMap<>());
    }

    /**
     * Records a column occurrence within a table and file context.
     */
    public void recordColumn(String tableName, String columnName, FileDetail fileDetail) {
        if (tableName == null || columnName == null) return;

        String t = tableName.trim();
        String c = columnName.trim();
        if (t.isEmpty() || c.isEmpty()) return;

        Map<String, Integer> cols = columnCounts.get(t);
        if (cols == null) {
            cols = new TreeMap<>();
            columnCounts.put(t, cols);
        }
        cols.put(c, cols.getOrDefault(c, 0) + 1);

        // Also add qualified "table.column" to file detail for Sheet 3
        fileDetail.addColumn(t, c);
    }

    /**
     * Adds a file detail to the result.
     */
    public void addFileDetail(FileDetail fileDetail) {
        fileDetails.add(fileDetail);
    }

    public List<FileDetail> getFileDetails() {
        return fileDetails;
    }

    public List<TableUsage> getTableUsagesSortedByCount() {
        List<TableUsage> result = new ArrayList<>();
        // Sort by count descending, then by name
        tableCounts.entrySet().stream()
                .sorted((a, b) -> {
                    int cmp = Integer.compare(b.getValue(), a.getValue());
                    return cmp != 0 ? cmp : a.getKey().compareTo(b.getKey());
                })
                .forEach(e -> result.add(new TableUsage(e.getKey(), e.getValue())));
        return result;
    }

    public List<ColumnUsage> getColumnUsagesSortedByTableAndCount() {
        List<ColumnUsage> result = new ArrayList<>();
        columnCounts.forEach((table, cols) -> {
            cols.entrySet().stream()
                    .sorted((a, b) -> {
                        int cmp = Integer.compare(b.getValue(), a.getValue());
                        return cmp != 0 ? cmp : a.getKey().compareTo(b.getKey());
                    })
                    .forEach(e -> result.add(new ColumnUsage(table, e.getKey(), e.getValue())));
        });
        return result;
    }
}
