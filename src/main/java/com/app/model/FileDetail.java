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
    /** Stores entries as "table.column" (qualified) for Sheet 3 display */
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

    /**
     * Adds a column qualified by its table name (e.g. "subscriber.isdn").
     * This drives Sheet 3 ("Columns") output.
     */
    public void addColumn(String table, String column) {
        if (table != null && column != null) {
            String t = table.trim();
            String c = column.trim();
            if (!t.isEmpty() && !c.isEmpty()) {
                columns.add(t + "." + c);
            }
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
