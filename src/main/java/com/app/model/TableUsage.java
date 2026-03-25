package com.app.model;

/**
 * Represents the usage count of a table across all processed files.
 */
public class TableUsage {
    private final String tableName;
    private final int usageCount;

    public TableUsage(String tableName, int usageCount) {
        this.tableName = tableName;
        this.usageCount = usageCount;
    }

    public String getTableName() {
        return tableName;
    }

    public int getUsageCount() {
        return usageCount;
    }
}
