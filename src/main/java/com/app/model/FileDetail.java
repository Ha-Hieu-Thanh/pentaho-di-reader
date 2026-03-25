package com.app.model;

import java.util.Set;
import java.util.TreeSet;

/**
 * Represents the tables and columns found in a single file.
 */
public class FileDetail {
    private final String fileName;
    private final String filePath;
    private final Set<String> tables = new TreeSet<>();
    private final Set<String> columns = new TreeSet<>();

    public FileDetail(String fileName, String filePath) {
        this.fileName = fileName;
        this.filePath = filePath;
    }

    public String getFileName() {
        return fileName;
    }

    public String getFilePath() {
        return filePath;
    }

    public Set<String> getTables() {
        return tables;
    }

    public Set<String> getColumns() {
        return columns;
    }

    public void addTable(String table) {
        if (table != null && !table.trim().isEmpty()) {
            tables.add(table.trim());
        }
    }

    public void addColumn(String column) {
        if (column != null && !column.trim().isEmpty()) {
            columns.add(column.trim());
        }
    }

    /**
     * Returns comma-separated list of tables.
     */
    public String getTablesAsString() {
        return String.join(", ", tables);
    }

    /**
     * Returns comma-separated list of columns.
     */
    public String getColumnsAsString() {
        return String.join(", ", columns);
    }
}
