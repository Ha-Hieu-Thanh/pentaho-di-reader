# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Goal

A Java CLI tool that:
1. Accepts a file or folder path (`.ktr` or `.kjb` Pentaho DI files) via command-line argument or interactive prompt
2. Recursively scans files in a directory
3. Extracts all embedded SQL queries from the XML files
4. Generates a 3-sheet Excel report (`.xlsx`):
   - **Summary** — unique tables and total usage count
   - **Table Detail** — unique (table, column) pairs and usage count
   - **File Detail** — each processed file with its tables and columns

## Build & Run

```bash
# Set JAVA_HOME first (Windows):
# JAVA_HOME=C:\Program Files\BellSoft\LibericaJDK-17

# Compile & test
mvn clean test -Dmaven.resolver.transport.wagon

# Package
mvn package -Dmaven.resolver.transport.wagon -DskipTests

# Run (from project root) — pass folder path as argument
mvn exec:java -Dexec.mainClass=com.app.Main \
  -Dexec.args="C:\path\to\sample-files" \
  -Dmaven.resolver.transport.wagon
```

## Architecture

- **`cli/CliInput`** — Validates and resolves input path (file or folder). Falls back to interactive prompt if no args given.
- **`parser/KettleParser`** — Parses `.ktr`/`.kjb` XML using `DocumentBuilder`. Scans for SQL field elements (`<sql>`, `<query>`, etc.) and extracts raw SQL strings. Handles CDATA sections.
- **`extractor/SqlExtractor`** — Two-layer extraction:
  1. `TablesNamesFinder` extracts real table names (correctly ignores aliases)
  2. `SelectColumnExtractor` (visitor) traverses the SELECT AST, builds an `alias→realTable` map, then scans expression text via regex — resolving each `table.column` reference against the alias map before recording
- **`report/ExcelReportGenerator`** — Creates a 3-sheet `.xlsx` using Apache POI `XSSFWorkbook`. Blue header row (white bold text), light-gray total row, auto-sized columns.
- **`model/`** — `ExtractionResult` (aggregates global counts + per-file details), `FileDetail`, `TableUsage`, `ColumnUsage`.

## JSQLParser 4.9 API Notes

The following are **not** available in this version — code must work around them:
- `GroupByElement.getExpr()` — use `GroupByElement.getGroupByExpressions().getExpressions()` instead
- `GroupByElement` iteration — iterate the raw `getGroupByExpressions()` (which is a raw `ExpressionList` extending `ArrayList`); cast each `Object` to `Expression`
- `Insert.getItemsList()` — use `getSetUpdateSets()` / `getDuplicateUpdateSets()` for MySQL-style INSERT
- `ParenthesedFromItem.getSelect()` — does not exist; use `ParenthesedFromItem.getFromItem()` recursively
- `Merge.getOn()` — use `getOnCondition()`
- `SelectVisitorAdapter` — does not exist; implement `SelectVisitor` directly with all required visit methods
- `SelectItemVisitorAdapter.visit(SelectItem<?>)` — name clash with raw `visit(SelectItem)`; use raw `visit` method only
- `Join.getUsingColumns()` — returns `List<Column>`, not `List<String>`
- `LateralView.getExpr()` / `getTable()` — do not exist; scan `toString()` via regex instead

## Excel Output Structure

| Sheet | Columns |
|---|---|
| **Summary** | Table, Usage Count (+ Total row) |
| **Table Detail** | Table, Column, Usage Count |
| **File Detail** | File Name, Tables, Columns |

## File Formats

- **`.ktr`** — Pentaho Transformation XML
- **`.kjb`** — Pentaho Job XML

## Dependencies

- **JSQLParser 4.9** — SQL parsing
- **Apache POI 4.1.2** — Excel `.xlsx` generation
- **Junit 4.13.2** — Testing

## Maven Build Notes

- Use `-Dmaven.resolver.transport.wagon` flag to resolve SSL certificate issues in some environments
- Dependencies are copied to `target/lib` via `maven-dependency-plugin`
- Main class is configured in `pom.xml` manifest: `com.app.Main`
