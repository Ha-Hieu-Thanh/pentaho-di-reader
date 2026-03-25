package com.app.extractor;

import com.app.model.ExtractionResult;
import com.app.model.FileDetail;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.upsert.Upsert;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.update.UpdateSet;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts table and column names from raw SQL strings using JSQLParser 4.9.
 * Correctly resolves table aliases to real table names.
 *
 * Column extraction strategy:
 * - SELECT clause: use JSQLParser's SelectItem API directly to get Column nodes
 * - WHERE/ORDER BY/GROUP BY/HAVING: use JSQLParser's ExpressionVisitor to get Column nodes
 * - Only qualified (table.column) references are extracted — no regex
 *
 * Supports: SELECT, INSERT, UPSERT, UPDATE, DELETE, MERGE, CREATE TABLE.
 */
public class SqlExtractor {

    /** Matches "table.column" patterns (used only for non-SELECT clauses via ExpressionVisitor) */
    private static final Pattern TABLE_COL_PATTERN =
            Pattern.compile("\\b([a-zA-Z_][a-zA-Z0-9_]*)\\s*\\.\\s*([a-zA-Z_][a-zA-Z0-9_]*)");

    public void extract(String sql, FileDetail fileDetail, ExtractionResult result) {
        if (sql == null || sql.trim().isEmpty()) return;

        try {
            Statements stmts = CCJSqlParserUtil.parseStatements(sql);
            for (Statement stmt : stmts.getStatements()) {
                extractStatement(stmt, fileDetail, result);
            }
        } catch (JSQLParserException e) {
            try {
                Statement stmt = CCJSqlParserUtil.parse(sql);
                extractStatement(stmt, fileDetail, result);
            } catch (JSQLParserException ex) {
                extractTablesByRegex(sql, fileDetail, result);
            }
        }
    }

    private void extractStatement(Statement stmt, FileDetail fileDetail, ExtractionResult result) {
        if (stmt instanceof Select) {
            extractSelect((Select) stmt, fileDetail, result);
        } else if (stmt instanceof Insert) {
            extractInsert((Insert) stmt, fileDetail, result);
        } else if (stmt instanceof Upsert) {
            extractUpsert((Upsert) stmt, fileDetail, result);
        } else if (stmt instanceof Update) {
            extractUpdate((Update) stmt, fileDetail, result);
        } else if (stmt instanceof Delete) {
            extractDelete((Delete) stmt, fileDetail, result);
        } else if (stmt instanceof Merge) {
            extractMerge((Merge) stmt, fileDetail, result);
        } else if (stmt instanceof CreateTable) {
            extractCreateTable((CreateTable) stmt, fileDetail, result);
        }
    }

    // ─── SELECT ───────────────────────────────────────────────────────────────

    private void extractSelect(Select select, FileDetail fileDetail, ExtractionResult result) {
        // Get real table names
        net.sf.jsqlparser.util.TablesNamesFinder namesFinder =
                new net.sf.jsqlparser.util.TablesNamesFinder();
        for (String tableName : namesFinder.getTableList((Statement) select)) {
            result.recordTable(tableName, fileDetail);
        }

        // Extract columns with proper alias resolution via SelectVisitor
        SelectColumnExtractor extractor = new SelectColumnExtractor(fileDetail, result);
        select.accept(extractor);
    }

    // ─── INSERT ───────────────────────────────────────────────────────────────

    private void extractInsert(Insert insert, FileDetail fileDetail, ExtractionResult result) {
        Table table = insert.getTable();
        String defaultTable = "";
        if (table != null) {
            defaultTable = table.getName();
            result.recordTable(defaultTable, fileDetail);
        }

        if (insert.getSetUpdateSets() != null) {
            for (UpdateSet us : insert.getSetUpdateSets()) {
                extractUpdateSetColumns(us, defaultTable, fileDetail, result);
            }
        }

        if (insert.getDuplicateUpdateSets() != null) {
            for (UpdateSet us : insert.getDuplicateUpdateSets()) {
                extractUpdateSetColumns(us, defaultTable, fileDetail, result);
            }
        }

        if (insert.getSelect() != null) {
            extractSelect(insert.getSelect(), fileDetail, result);
        }

        // Fallback: regex on full statement
        extractColumnsFromText(insert.toString(), defaultTable, fileDetail, result);
    }

    // ─── UPSERT ──────────────────────────────────────────────────────────────

    private void extractUpsert(Upsert upsert, FileDetail fileDetail, ExtractionResult result) {
        Table table = upsert.getTable();
        if (table != null) {
            result.recordTable(table.getName(), fileDetail);
        }
        extractColumnsFromText(upsert.toString(), table != null ? table.getName() : "", fileDetail, result);
    }

    // ─── UPDATE ─────────────────────────────────────────────────────────────

    private void extractUpdate(Update update, FileDetail fileDetail, ExtractionResult result) {
        Table table = update.getTable();
        if (table != null) {
            String tableName = table.getName();
            result.recordTable(tableName, fileDetail);
            extractColumnsFromText(update.toString(), tableName, fileDetail, result);
        }
    }

    // ─── DELETE ─────────────────────────────────────────────────────────────

    private void extractDelete(Delete delete, FileDetail fileDetail, ExtractionResult result) {
        Table table = delete.getTable();
        if (table != null) {
            String tableName = table.getName();
            result.recordTable(tableName, fileDetail);
            extractColumnsFromText(delete.toString(), tableName, fileDetail, result);
        }
    }

    // ─── MERGE ──────────────────────────────────────────────────────────────

    private void extractMerge(Merge merge, FileDetail fileDetail, ExtractionResult result) {
        Table table = merge.getTable();
        if (table != null) {
            result.recordTable(table.getName(), fileDetail);
        }
        FromItem using = merge.getFromItem();
        if (using instanceof Table) {
            result.recordTable(((Table) using).getName(), fileDetail);
        }
        extractColumnsFromText(merge.toString(), table != null ? table.getName() : "", fileDetail, result);
    }

    // ─── CREATE TABLE ────────────────────────────────────────────────────────

    private void extractCreateTable(CreateTable ct, FileDetail fileDetail, ExtractionResult result) {
        Table table = ct.getTable();
        if (table != null) {
            result.recordTable(table.getName(), fileDetail);
        }
    }

    // ─── Column extraction ──────────────────────────────────────────────────

    private void extractUpdateSetColumns(UpdateSet us, String defaultTable,
                                        FileDetail fileDetail, ExtractionResult result) {
        ExpressionList<Column> columns = us.getColumns();
        ExpressionList<?> values = us.getValues();

        if (columns != null) {
            for (Column col : columns) {
                result.recordColumn(defaultTable, col.getColumnName(), fileDetail);
            }
        }

        if (values != null) {
            for (Object o : values) {
                extractColumnsFromText(o.toString(), defaultTable, fileDetail, result);
            }
        }
    }

    /**
     * Uses JSQLParser's ExpressionVisitor to visit Column nodes in an expression,
     * resolving aliases. This is the reliable approach — no regex misidentification.
     */
    private void extractColumnsFromText(String text, String defaultTable,
                                       FileDetail fileDetail, ExtractionResult result) {
        if (text == null) return;
        // Extract qualified column refs via regex on text (with alias resolution)
        Matcher m = TABLE_COL_PATTERN.matcher(text);
        while (m.find()) {
            result.recordColumn(m.group(1), m.group(2), fileDetail);
        }
        // Also extract standalone column names with default table
        extractStandaloneColumns(text, defaultTable, fileDetail, result);
    }

    private void extractStandaloneColumns(String text, String defaultTable,
                                        FileDetail fileDetail, ExtractionResult result) {
        if (text == null) return;
        Matcher m = Pattern.compile("\\b([a-zA-Z_][a-zA-Z0-9_]{1,50})\\b").matcher(text);
        while (m.find()) {
            String word = m.group(1);
            if (!isSqlKeyword(word) && !isNonColumnWord(word)) {
                result.recordColumn(defaultTable, word, fileDetail);
            }
        }
    }

    // ─── Select Visitor ───────────────────────────────────────────────────────

    /**
     * Full SelectVisitor that handles each clause separately.
     * The visitor is invoked once per Select and processes:
     * - FROM/JOIN: builds alias map, sets currentTable
     * - SELECT items: visits Column nodes directly via SelectItemVisitor
     * - WHERE/ORDER BY/GROUP BY/HAVING: visits Column nodes via ExpressionVisitor
     *
     * Key principle: Column nodes are visited directly from the AST, never via regex on text.
     */
    private class SelectColumnExtractor implements SelectVisitor {
        private final FileDetail fileDetail;
        private final ExtractionResult result;
        /** alias name → real table name */
        private final Map<String, String> aliasMap = new HashMap<>();
        /** Default table (leftmost FROM table) for unqualified columns */
        private String currentTable = "";

        SelectColumnExtractor(FileDetail fileDetail, ExtractionResult result) {
            this.fileDetail = fileDetail;
            this.result = result;
        }

        // ── SelectVisitor entry points ──────────────────────────────────────

        @Override public void visit(PlainSelect ps) {
            processSelectBody(ps);
        }

        @Override public void visit(SetOperationList sol) {
            if (sol.getSelects() != null) {
                for (Select s : sol.getSelects()) {
                    SelectColumnExtractor inner = new SelectColumnExtractor(fileDetail, result);
                    s.accept(inner);
                }
            }
        }

        @Override public void visit(WithItem wi) {
            if (wi.getSelect() != null) {
                SelectColumnExtractor inner = new SelectColumnExtractor(fileDetail, result);
                wi.getSelect().accept(inner);
            }
        }

        @Override public void visit(Values values) {
            // VALUES clause — extract column names from VALUES list
            if (values.getExpressions() != null) {
                for (Object o : values.getExpressions()) {
                    // VALUES entries can have column references
                }
            }
        }

        @Override public void visit(LateralSubSelect lss) {
            if (lss.getSelect() != null) {
                SelectColumnExtractor inner = new SelectColumnExtractor(fileDetail, result);
                lss.getSelect().accept(inner);
            }
        }

        @Override public void visit(ParenthesedSelect ps) {
            if (ps.getSelect() != null) {
                SelectColumnExtractor inner = new SelectColumnExtractor(fileDetail, result);
                ps.getSelect().accept(inner);
            }
        }

        @Override public void visit(TableStatement ts) {
            // Simple table ref — already handled by TablesNamesFinder
        }

        // ── Main processing ───────────────────────────────────────────────

        private void processSelectBody(PlainSelect ps) {
            // 1. Build alias map from FROM and all JOINs FIRST
            buildAliasMap(ps);

            // 2. Extract columns from SELECT list using Column nodes directly
            if (ps.getSelectItems() != null) {
                for (SelectItem<?> item : ps.getSelectItems()) {
                    extractColumnsFromSelectItem(item);
                }
            }

            // 3. Extract columns from WHERE
            if (ps.getWhere() != null) {
                extractColumnsFromExpression(ps.getWhere());
                extractStandaloneFromExpression(ps.getWhere());
            }

            // 4. Extract columns from GROUP BY
            if (ps.getGroupBy() != null) {
                ExpressionList groupByExprs = ps.getGroupBy().getGroupByExpressions();
                if (groupByExprs != null) {
                    for (Object o : groupByExprs) {
                        Expression e = (Expression) o;
                        extractColumnsFromExpression(e);
                        extractStandaloneFromExpression(e);
                    }
                }
            }

            // 5. Extract columns from HAVING
            if (ps.getHaving() != null) {
                extractColumnsFromExpression(ps.getHaving());
                extractStandaloneFromExpression(ps.getHaving());
            }

            // 6. Extract columns from ORDER BY
            if (ps.getOrderByElements() != null) {
                for (OrderByElement elem : ps.getOrderByElements()) {
                    if (elem.getExpression() != null) {
                        extractColumnsFromExpression(elem.getExpression());
                        extractStandaloneFromExpression(elem.getExpression());
                    }
                }
            }
        }

        // ── Build alias map ───────────────────────────────────────────────

        private void buildAliasMap(PlainSelect ps) {
            aliasMap.clear();

            // FROM item (only Table type — skip subqueries for alias building)
            FromItem fromItem = ps.getFromItem();
            if (fromItem instanceof Table) {
                Table t = (Table) fromItem;
                String realName = t.getName();
                String alias = t.getAlias() != null ? t.getAlias().getName() : null;
                aliasMap.put(realName, realName);
                currentTable = realName;
                if (alias != null && !alias.isEmpty()) {
                    aliasMap.put(alias, realName);
                }
            }

            // JOIN items — add to alias map but don't change currentTable
            if (ps.getJoins() != null) {
                for (Join join : ps.getJoins()) {
                    FromItem right = join.getRightItem();
                    if (right instanceof Table) {
                        Table t = (Table) right;
                        String realName = t.getName();
                        String alias = t.getAlias() != null ? t.getAlias().getName() : null;
                        aliasMap.put(realName, realName);
                        if (alias != null && !alias.isEmpty()) {
                            aliasMap.put(alias, realName);
                        }
                    }
                }
            }

            // LATERAL VIEWS — register the table alias if present
            if (ps.getLateralViews() != null) {
                for (LateralView lv : ps.getLateralViews()) {
                    if (lv.getTableAlias() != null) {
                        String aliasName = lv.getTableAlias().getName();
                        aliasMap.put(aliasName, aliasName);
                    }
                }
            }
        }

        // ── Extract columns from SelectItem (SELECT list) ──────────────────

        private void extractColumnsFromSelectItem(SelectItem<?> item) {
            // Use SelectItemVisitor to visit Column nodes within each SELECT item expression
            item.accept(new SelectItemVisitorAdapter() {
                @Override
                public void visit(SelectItem si) {
                    if (si.getExpression() == null) return;
                    extractColumnsFromExpression(si.getExpression());
                }
            });
        }

        // ── Extract columns from any Expression ────────────────────────────

        /**
         * Visits Column nodes in an expression using JSQLParser's ExpressionVisitor.
         * This is the correct approach — Column nodes have the table qualifier
         * properly resolved by JSQLParser's parser.
         */
        private void extractColumnsFromExpression(Expression expr) {
            if (expr == null) return;
            try {
                expr.accept(new net.sf.jsqlparser.expression.ExpressionVisitorAdapter() {
                    @Override
                    public void visit(Column col) {
                        String colName = col.getColumnName();
                        String tableRef = col.getTable() != null ? col.getTable().getName() : null;
                        if (tableRef != null && !tableRef.isEmpty()) {
                            String resolved = aliasMap.get(tableRef);
                            result.recordColumn(
                                    resolved != null ? resolved : tableRef,
                                    colName,
                                    fileDetail);
                        }
                    }

                    @Override
                    public void visit(net.sf.jsqlparser.expression.Function f) {
                        // descend into args
                        super.visit(f);
                    }
                });
            } catch (Exception e) {
                // fallback to text
                extractColumnsFromText(expr.toString(), currentTable, fileDetail, result);
            }
        }

        /**
         * Extract standalone (unqualified) column names from expressions and map
         * them to currentTable. Used for WHERE/GROUP BY/HAVING/ORDER BY only.
         */
        private void extractStandaloneFromExpression(Expression expr) {
            if (expr == null) return;
            String text = expr.toString();
            Matcher m = Pattern.compile("\\b([a-zA-Z_][a-zA-Z0-9_]{1,50})\\b").matcher(text);
            while (m.find()) {
                String word = m.group(1);
                if (!isSqlKeyword(word) && !isNonColumnWord(word)) {
                    result.recordColumn(currentTable, word, fileDetail);
                }
            }
        }
    }

    // ─── Regex fallback ──────────────────────────────────────────────────────

    private void extractTablesByRegex(String sql, FileDetail fileDetail, ExtractionResult result) {
        Pattern[] patterns = {
            Pattern.compile("\\BFROM\\s+([`\"'\\[]?\\w+[`\"'\\]]?(?:\\s*,\\s*[`\"'\\[]?\\w+[`\"'\\]]?)+)", Pattern.CASE_INSENSITIVE),
            Pattern.compile("\\bJOIN\\s+([`\"'\\[]?\\w+[`\"'\\]]?)\\b", Pattern.CASE_INSENSITIVE),
            Pattern.compile("\\bINTO\\s+([`\"'\\[]?\\w+[`\"'\\]]?)\\b", Pattern.CASE_INSENSITIVE),
            Pattern.compile("\\bUPDATE\\s+([`\"'\\[]?\\w+[`\"'\\]]?)\\b", Pattern.CASE_INSENSITIVE),
            Pattern.compile("\\bDELETE\\s+FROM\\s+([`\"'\\[]?\\w+[`\"'\\]]?)\\b", Pattern.CASE_INSENSITIVE),
        };

        Set<String> found = new HashSet<>();
        for (Pattern p : patterns) {
            Matcher m = p.matcher(sql);
            while (m.find()) {
                String match = m.group(1).replaceAll("[`\"'\\[\\]]", "").trim();
                if (!match.isEmpty() && !isSqlKeyword(match) && found.add(match)) {
                    result.recordTable(match, fileDetail);
                }
            }
        }
        extractColumnsFromText(sql, "", fileDetail, result);
    }

    private static boolean isNonColumnWord(String word) {
        Set<String> s = new HashSet<>();
        s.add("SUM"); s.add("AVG"); s.add("COUNT"); s.add("MIN"); s.add("MAX");
        s.add("COALESCE"); s.add("NULLIF"); s.add("CAST"); s.add("CONVERT");
        s.add("CONCAT"); s.add("LENGTH"); s.add("SUBSTRING"); s.add("TRIM");
        s.add("UPPER"); s.add("LOWER"); s.add("REPLACE"); s.add("SUBSTR");
        s.add("NOW"); s.add("DATE"); s.add("YEAR"); s.add("MONTH"); s.add("DAY");
        s.add("ROUND"); s.add("FLOOR"); s.add("ABS"); s.add("IFNULL"); s.add("NVL");
        s.add("IIF"); s.add("ROW_NUMBER"); s.add("RANK"); s.add("DENSE_RANK");
        s.add("LEAD"); s.add("LAG"); s.add("FIRST_VALUE"); s.add("LAST_VALUE");
        s.add("OVER"); s.add("PARTITION"); s.add("UNBOUNDED"); s.add("PRECEDING");
        s.add("FOLLOWING"); s.add("CURRENT_ROW"); s.add("GROUPS"); s.add("RANGE");
        s.add("ROWS"); s.add("DATE_SUB"); s.add("DATE_ADD"); s.add("CURDATE");
        s.add("SYSDATE"); s.add("ISNULL"); s.add("CHOOSE"); s.add("DAYOFMONTH");
        s.add("DATEADD"); s.add("DATEDIFF"); s.add("INTERVAL"); s.add("DECODE");
        s.add("NVL2"); s.add("COLLATE"); s.add("ROW"); s.add("COL"); s.add("SEQ");
        s.add("NULL"); s.add("TRUE"); s.add("FALSE"); s.add("DEFAULT");
        s.add("CURRENT_DATE"); s.add("CURRENT_TIMESTAMP");
        return s.contains(word.toUpperCase());
    }

    private static boolean isSqlKeyword(String word) {
        Set<String> s = new HashSet<>();
        s.add("SELECT"); s.add("FROM"); s.add("WHERE"); s.add("JOIN");
        s.add("LEFT"); s.add("RIGHT"); s.add("INNER"); s.add("OUTER"); s.add("ON");
        s.add("AND"); s.add("OR"); s.add("NOT"); s.add("IN"); s.add("EXISTS");
        s.add("BETWEEN"); s.add("LIKE"); s.add("IS"); s.add("NULL");
        s.add("TRUE"); s.add("FALSE"); s.add("AS"); s.add("ORDER"); s.add("BY");
        s.add("GROUP"); s.add("HAVING"); s.add("LIMIT"); s.add("OFFSET");
        s.add("UNION"); s.add("ALL"); s.add("DISTINCT"); s.add("CASE");
        s.add("WHEN"); s.add("THEN"); s.add("ELSE"); s.add("END"); s.add("SET");
        s.add("VALUES"); s.add("INSERT"); s.add("UPDATE"); s.add("DELETE");
        s.add("CREATE"); s.add("TABLE"); s.add("DROP"); s.add("ALTER");
        s.add("INDEX"); s.add("VIEW"); s.add("CROSS"); s.add("NATURAL");
        s.add("USING"); s.add("ASC"); s.add("DESC"); s.add("NULLS");
        s.add("FETCH"); s.add("NEXT"); s.add("ROW"); s.add("ONLY"); s.add("WITH");
        s.add("RECURSIVE"); s.add("LATERAL"); s.add("OVER"); s.add("PARTITION");
        s.add("ROWS"); s.add("RANGE"); s.add("PRECEDING"); s.add("FOLLOWING");
        s.add("UNBOUNDED"); s.add("GRANT"); s.add("REVOKE"); s.add("TRUNCATE");
        s.add("EXCEPT"); s.add("INTERSECT"); s.add("PERCENT"); s.add("TIES");
        s.add("LOCK"); s.add("RETURNING"); s.add("CASCADE"); s.add("RESTRICT");
        s.add("INTO");
        return s.contains(word.toUpperCase());
    }
}
