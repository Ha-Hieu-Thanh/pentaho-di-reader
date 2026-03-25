package com.app.model;

/**
 * Represents the usage count of a column within a specific table.
 */
public class ColumnUsage {
    private final String tableName;
    private final String columnName;
    private final int usageCount;

    public ColumnUsage(String tableName, String columnName, int usageCount) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.usageCount = usageCount;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public int getUsageCount() {
        return usageCount;
    }
}
