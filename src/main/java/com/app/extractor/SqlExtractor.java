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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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

    /**
     * @param parentAliasMap alias map from the enclosing INSERT/UPDATE/DELETE/MERGE context.
     *                       Pass an empty map when extracting a standalone SELECT.
     */
    private void extractSelect(Select select, FileDetail fileDetail, ExtractionResult result,
                              Map<String, String> parentAliasMap) {
        // Get real table names
        net.sf.jsqlparser.util.TablesNamesFinder namesFinder =
                new net.sf.jsqlparser.util.TablesNamesFinder();
        for (String tableName : namesFinder.getTableList((Statement) select)) {
            result.recordTable(tableName, fileDetail);
        }

        // Extract columns with proper alias resolution via SelectVisitor
        SelectColumnExtractor extractor = new SelectColumnExtractor(fileDetail, result, parentAliasMap);
        select.accept(extractor);
    }

    private void extractSelect(Select select, FileDetail fileDetail, ExtractionResult result) {
        extractSelect(select, fileDetail, result, new HashMap<>());
    }

    // ─── INSERT ───────────────────────────────────────────────────────────────

    private void extractInsert(Insert insert, FileDetail fileDetail, ExtractionResult result) {
        Table table = insert.getTable();
        String defaultTable = "";
        Map<String, String> parentAliasMap = new HashMap<>();
        if (table != null) {
            defaultTable = table.getName();
            result.recordTable(defaultTable, fileDetail);
            parentAliasMap.put(defaultTable, defaultTable);
            if (table.getAlias() != null) {
                parentAliasMap.put(table.getAlias().getName(), defaultTable);
            }
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
            // SELECT columns are processed via extractSelect — no regex on full string needed.
            // The regex fallback below would incorrectly scan subquery bodies.
            extractSelect(insert.getSelect(), fileDetail, result, parentAliasMap);
        } else {
            // INSERT ... VALUES (no embedded SELECT) — regex on full statement is safe here.
            extractColumnsFromText(insert.toString(), defaultTable, fileDetail, result);
        }
    }

    // ─── UPSERT ──────────────────────────────────────────────────────────────

    private void extractUpsert(Upsert upsert, FileDetail fileDetail, ExtractionResult result) {
        Table table = upsert.getTable();
        Map<String, String> parentAliasMap = new HashMap<>();
        if (table != null) {
            String name = table.getName();
            result.recordTable(name, fileDetail);
            parentAliasMap.put(name, name);
            if (table.getAlias() != null) {
                parentAliasMap.put(table.getAlias().getName(), name);
            }
        }
        if (upsert.getSelect() != null) {
            extractSelect(upsert.getSelect(), fileDetail, result, parentAliasMap);
        }
        // No regex fallback on full upsert.toString() — subquery table refs would be misattributed.
        // For bare VALUES columns (no SELECT), use the regex path on that specific clause instead.
    }

    // ─── UPDATE ─────────────────────────────────────────────────────────────

    private void extractUpdate(Update update, FileDetail fileDetail, ExtractionResult result) {
        Table table = update.getTable();
        Map<String, String> parentAliasMap = new HashMap<>();
        if (table != null) {
            String tableName = table.getName();
            result.recordTable(tableName, fileDetail);
            parentAliasMap.put(tableName, tableName);
            if (table.getAlias() != null) {
                parentAliasMap.put(table.getAlias().getName(), tableName);
            }
        }
        // Process embedded SELECT subqueries (UPDATE SET = subqueries, UPDATE WHERE)
        if (update.getSelect() != null) {
            extractSelect(update.getSelect(), fileDetail, result, parentAliasMap);
        }
        // Process UPDATE WHERE clause directly with parent alias map.
        // Do NOT use extractColumnsFromText on the full UPDATE string —
        // that would regex-match subquery table refs (oi., o.) with no alias map.
        if (update.getWhere() != null) {
            Expression where = update.getWhere();
            SelectColumnExtractor outer = new SelectColumnExtractor(fileDetail, result, parentAliasMap);
            where.accept(new net.sf.jsqlparser.expression.ExpressionVisitorAdapter() {
                @Override public void visit(Column col) {
                    outer.processColumn(col);
                }
                @Override public void visit(net.sf.jsqlparser.expression.Function f) {
                    // Traverse into function arguments (e.g. COALESCE(a, b) → visit a and b)
                    super.visit(f);
                }
            });
        }
    }

    // ─── DELETE ─────────────────────────────────────────────────────────────

    private void extractDelete(Delete delete, FileDetail fileDetail, ExtractionResult result) {
        Table table = delete.getTable();
        Map<String, String> parentAliasMap = new HashMap<>();
        if (table != null) {
            String tableName = table.getName();
            result.recordTable(tableName, fileDetail);
            parentAliasMap.put(tableName, tableName);
            if (table.getAlias() != null) {
                parentAliasMap.put(table.getAlias().getName(), tableName);
            }
        }
        extractColumnsFromText(delete.toString(), table != null ? table.getName() : "", fileDetail, result);
    }

    // ─── MERGE ──────────────────────────────────────────────────────────────

    private void extractMerge(Merge merge, FileDetail fileDetail, ExtractionResult result) {
        Table table = merge.getTable();
        Map<String, String> parentAliasMap = new HashMap<>();
        if (table != null) {
            result.recordTable(table.getName(), fileDetail);
            parentAliasMap.put(table.getName(), table.getName());
            if (table.getAlias() != null) {
                parentAliasMap.put(table.getAlias().getName(), table.getName());
            }
        }
        FromItem using = merge.getFromItem();
        if (using instanceof Table) {
            String usingName = ((Table) using).getName();
            result.recordTable(usingName, fileDetail);
            parentAliasMap.put(usingName, usingName);
            if (((Table) using).getAlias() != null) {
                parentAliasMap.put(((Table) using).getAlias().getName(), usingName);
            }
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
     * Uses regex on expression text to extract qualified "table.column" references,
     * resolving each table identifier through aliasMap before recording.
     * This is a fallback path — the preferred route is extractColumnsFromExpression (AST-based).
     */
    private void extractColumnsFromText(String text, String defaultTable,
                                       FileDetail fileDetail, ExtractionResult result) {
        if (text == null) return;
        extractColumnsFromText(text, defaultTable, new HashMap<>(), fileDetail, result);
    }

    private void extractColumnsFromText(String text, String defaultTable,
                                       Map<String, String> aliasMap,
                                       FileDetail fileDetail, ExtractionResult result) {
        if (text == null) return;
        // Extract qualified column refs via regex on text (with alias resolution)
        Matcher m = TABLE_COL_PATTERN.matcher(text);
        while (m.find()) {
            String tableRef = m.group(1);
            String colName  = m.group(2);
            // Resolve alias to real table name; fall back to the raw identifier
            String resolved = aliasMap.get(tableRef);
            result.recordColumn(
                    resolved != null ? resolved : tableRef,
                    colName,
                    fileDetail);
        }
        // Also extract standalone column names with default table
        extractStandaloneColumns(text, defaultTable, aliasMap, fileDetail, result);
    }

    private void extractStandaloneColumns(String text, String defaultTable,
                                        FileDetail fileDetail, ExtractionResult result) {
        if (text == null) return;
        extractStandaloneColumns(text, defaultTable, new HashMap<>(), fileDetail, result);
    }

    private void extractStandaloneColumns(String text, String defaultTable,
                                        Map<String, String> aliasMap,
                                        FileDetail fileDetail, ExtractionResult result) {
        if (text == null) return;
        Matcher m = Pattern.compile("\\b([a-zA-Z_][a-zA-Z0-9_]{1,50})\\b").matcher(text);
        while (m.find()) {
            String word = m.group(1);
            if (!isSqlKeyword(word) && !isNonColumnWord(word)
                    && !aliasMap.containsKey(word)) {
                result.recordColumn(defaultTable, word, fileDetail);
            }
        }
    }

    // ─── Select Visitor ───────────────────────────────────────────────────────

    /**
     * Full SelectVisitor that handles each clause separately.
     * Uses a scope stack to correctly handle nested subqueries:
     * - Each scope has its own local aliasMap (never pollutes outer scopes)
     * - Alias resolution searches from innermost scope outward
     * - Outer (DML) aliases are available as the innermost (read-only) scope
     */
    private class SelectColumnExtractor implements SelectVisitor {
        private final FileDetail fileDetail;
        private final ExtractionResult result;
        /** Scope stack — each entry is a scope-local aliasMap. Search from end outward. */
        private final List<Map<String, String>> scopeStack = new ArrayList<>();
        /** subquery alias → set of column names it produces (for safe recording of subquery cols) */
        private final Map<String, Set<String>> subqueryColMap = new HashMap<>();
        /** Default table (leftmost FROM table) for unqualified columns */
        private String currentTable = "";

        SelectColumnExtractor(FileDetail fileDetail, ExtractionResult result, Map<String, String> parentAliasMap) {
            this.fileDetail = fileDetail;
            this.result = result;
            // Bottom of scope stack: the read-only parent (DML) context aliases
            scopeStack.add(new HashMap<>(parentAliasMap));
        }

        SelectColumnExtractor(FileDetail fileDetail, ExtractionResult result) {
            this(fileDetail, result, new HashMap<>());
        }

        // ── Scope helpers ──────────────────────────────────────────────────

        /** Push a new local scope onto the stack. */
        private void pushScope() {
            scopeStack.add(new HashMap<>());
        }

        /** Pop the current scope and return it. */
        private Map<String, String> popScope() {
            if (scopeStack.size() <= 1) {
                throw new IllegalStateException("Cannot pop the root scope");
            }
            return scopeStack.remove(scopeStack.size() - 1);
        }

        /**
         * Register an alias → real-table entry into the CURRENT (innermost) scope.
         * Does NOT affect outer scopes — no pollution.
         */
        private void putAlias(String alias, String realTable) {
            scopeStack.get(scopeStack.size() - 1).put(alias, realTable);
        }

        /**
         * Resolve an alias to its real table name.
         * Searches from innermost scope outward; returns null if not found.
         */
        private String resolve(String alias) {
            for (int i = scopeStack.size() - 1; i >= 0; i--) {
                String found = scopeStack.get(i).get(alias);
                if (found != null) return found;
            }
            return null;
        }

        /**
         * Processes a single Column node, resolving its table qualifier against the scope stack.
         * Exposed so it can be called from ExpressionVisitorAdapter in extractUpdate.
         */
        private void processColumn(Column col) {
            String colName = col.getColumnName();
            String tableRef = col.getTable() != null ? col.getTable().getName() : null;
            if (tableRef != null && !tableRef.isEmpty()) {
                String resolved = resolve(tableRef);
                if ("__subquery__".equals(resolved)) {
                    Set<String> subCols = subqueryColMap.get(tableRef.toLowerCase());
                    if (subCols != null && subCols.contains(colName.toLowerCase())) {
                        result.recordColumn(tableRef, colName, fileDetail);
                    }
                } else {
                    result.recordColumn(resolved != null ? resolved : tableRef, colName, fileDetail);
                }
            }
        }

        // ── SelectVisitor entry points ──────────────────────────────────────

        @Override public void visit(PlainSelect ps) {
            processSelectBody(ps);
        }

        @Override public void visit(SetOperationList sol) {
            if (sol.getSelects() != null) {
                for (Select s : sol.getSelects()) {
                    SelectColumnExtractor inner = new SelectColumnExtractor(fileDetail, result, new HashMap<>());
                    s.accept(inner);
                }
            }
        }

        @Override public void visit(WithItem wi) {
            if (wi.getSelect() != null) {
                SelectColumnExtractor inner = new SelectColumnExtractor(fileDetail, result, new HashMap<>());
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
            // ExpressionVisitorAdapter may skip this too; visit inner select directly.
            if (lss.getSelect() != null) {
                lss.getSelect().accept(this);
            }
        }

        @Override public void visit(ParenthesedSelect ps) {
            // IMPORTANT: ExpressionVisitorAdapter's visit(ParenthesedSelect) silently delegates to
            // visit(Select) → selectVisitor (which is null for anonymous inner classes).
            // So the subquery is COMPLETELY SKIPPED unless we manually visit the inner SELECT here.
            if (ps.getSelect() != null) {
                ps.getSelect().accept(this);
            }
        }

        @Override public void visit(TableStatement ts) {
            // Simple table ref — already handled by TablesNamesFinder
        }

        // ── Main processing ───────────────────────────────────────────────

        private void processSelectBody(PlainSelect ps) {
            // Push a new scope so this SELECT's aliases don't overwrite the parent context.
            // This is critical when a subquery (ParenthesedSelect in UPDATE SET) is visited
            // via ExpressionVisitor.Function → super.visit → traverse → ParenthesedSelect.visit
            // without going through SetOperationList (which already pushes its own scope).
            pushScope();
            try {
                buildAliasMap(ps);

            // 2. Extract columns from SELECT list using Column nodes directly.
            //    The AST visitor handles qualified names (e.g. o.id → orders.id).
            //    Unqualified names are skipped here to avoid misattribution
            //    when multiple tables are joined (e.g. "status" wrongly recorded
            //    as the first table's column instead of its actual table).
            if (ps.getSelectItems() != null) {
                for (SelectItem<?> item : ps.getSelectItems()) {
                    extractColumnsFromSelectItem(item);
                }
            }

            // 3. Extract columns from WHERE (AST + standalone fallback)
            if (ps.getWhere() != null) {
                extractColumnsFromExpression(ps.getWhere());
                // Standalone fallback: only when there are no JOINs (single-table queries
                // like "SELECT id FROM customers WHERE active = 1").
                if (!hasJoin(ps)) {
                    extractStandaloneFromExpression(ps.getWhere());
                }
            }

            // 4. Extract columns from GROUP BY
            if (ps.getGroupBy() != null) {
                ExpressionList groupByExprs = ps.getGroupBy().getGroupByExpressions();
                if (groupByExprs != null) {
                    for (Object o : groupByExprs) {
                        Expression e = (Expression) o;
                        extractColumnsFromExpression(e);
                        if (!hasJoin(ps)) {
                            extractStandaloneFromExpression(e);
                        }
                    }
                }
            }

            // 5. Extract columns from HAVING
            if (ps.getHaving() != null) {
                extractColumnsFromExpression(ps.getHaving());
                if (!hasJoin(ps)) {
                    extractStandaloneFromExpression(ps.getHaving());
                }
            }

            // 6. Extract columns from ORDER BY
            if (ps.getOrderByElements() != null) {
                for (OrderByElement elem : ps.getOrderByElements()) {
                    if (elem.getExpression() != null) {
                        extractColumnsFromExpression(elem.getExpression());
                        if (!hasJoin(ps)) {
                            extractStandaloneFromExpression(elem.getExpression());
                        }
                    }
                }
            }
            } finally {
                popScope();
            }
        }

        private boolean hasJoin(PlainSelect ps) {
            return ps.getJoins() != null && !ps.getJoins().isEmpty();
        }

        // ── Build alias map ───────────────────────────────────────────────

        private void buildAliasMap(PlainSelect ps) {
            subqueryColMap.clear();

            // FROM item (Table or SubSelect)
            FromItem fromItem = ps.getFromItem();
            registerFromItem(fromItem, true);   // true = set as currentTable

            // JOIN items — add to alias map but don't change currentTable
            if (ps.getJoins() != null) {
                for (Join join : ps.getJoins()) {
                    FromItem right = join.getRightItem();
                    registerFromItem(right, false);
                }
            }

            // LATERAL VIEWS — register the table alias if present
            if (ps.getLateralViews() != null) {
                for (LateralView lv : ps.getLateralViews()) {
                    if (lv.getTableAlias() != null) {
                        String aliasName = lv.getTableAlias().getName();
                        putAlias(aliasName, aliasName);
                    }
                }
            }
        }

        /**
         * Registers a FROM item in the current scope's alias map.
         * - Table: realName → realName, alias → realName, currentTable = realName (if first)
         * - ParenthesedSelect (subquery): subquery alias → "__subquery__" marker; also records its
         *   column names so we can safely record subquery cols without polluting real table counts.
         */
        private void registerFromItem(FromItem fi, boolean setAsCurrent) {
            if (fi instanceof Table) {
                Table t = (Table) fi;
                String realName = t.getName();
                String alias = t.getAlias() != null ? t.getAlias().getName() : null;
                putAlias(realName, realName);
                if (setAsCurrent) currentTable = realName;
                if (alias != null && !alias.isEmpty()) {
                    putAlias(alias, realName);
                }
            } else if (fi instanceof ParenthesedSelect) {
                ParenthesedSelect ss = (ParenthesedSelect) fi;
                String alias = ss.getAlias() != null ? ss.getAlias().getName() : null;
                if (alias != null && !alias.isEmpty()) {
                    // Mark subquery alias so column refs against it are recognised but not
                    // attributed to a real table name.
                    putAlias(alias, "__subquery__");
                    // Collect column names produced by the subquery so we can safely
                    // record them when they appear in the outer query.
                    Set<String> subCols = new HashSet<>();
                    PlainSelect innerPs = ss.getSelect().getPlainSelect();
                    if (innerPs != null && innerPs.getSelectItems() != null) {
                        for (SelectItem<?> si : innerPs.getSelectItems()) {
                            String colName = extractColName(si);
                            if (colName != null) subCols.add(colName.toLowerCase());
                        }
                    }
                    subqueryColMap.put(alias.toLowerCase(), subCols);
                }
            }
        }

        /**
         * Reads the output column name from a SelectItem:
         * - Aliased expression → alias (via "expr AS alias" — detected via toString)
         * - Strip "table." prefix if no alias found
         */
        private String extractColName(SelectItem<?> si) {
            String s = si.toString();
            int asIdx = s.toUpperCase().lastIndexOf(" AS ");
            if (asIdx > 0) return s.substring(asIdx + 4).trim();
            int dot = s.lastIndexOf('.');
            return dot > 0 ? s.substring(dot + 1).trim() : s.trim();
        }

        // ── Extract columns from SelectItem (SELECT list) ──────────────────

        private void extractColumnsFromSelectItem(SelectItem<?> item) {
            // Use SelectItemVisitor to visit Column nodes within each SELECT item expression.
            // IMPORTANT: use the raw visit(SelectItem) method — the generic visit(SelectItem<?>)
            // clashes with it at erasure. Per CLAUDE.md: use raw visit method only.
            SelectItemVisitor vis = new SelectItemVisitorAdapter() {
                @Override
                public void visit(SelectItem si) {
                    if (si.getExpression() == null) return;
                    extractColumnsFromExpression(si.getExpression());
                }
            };
            item.accept(vis);
        }

        // ── Extract columns from any Expression ────────────────────────────

        /**
         * Visits Column nodes in an expression using JSQLParser's ExpressionVisitor.
         * This is the correct approach — Column nodes have the table qualifier
         * properly resolved by JSQLParser's parser.
         *
         * For subquery alias refs (e.g. "b.sub_id" where b is a subquery):
         * - if the column IS in the subquery's column list → record as b.column (shown as-is in output)
         * - if the column is NOT in the subquery's column list → it references an outer table;
         *   the column is recorded under the real outer table (propagated up via the outer extractor)
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
                            String resolved = resolve(tableRef);
                            if ("__subquery__".equals(resolved)) {
                                // tableRef is the alias of a subquery in this scope.
                                // Check if this column is one of the columns the subquery produces.
                                Set<String> subCols = subqueryColMap.get(tableRef.toLowerCase());
                                if (subCols != null && subCols.contains(colName.toLowerCase())) {
                                    // Column is produced by this subquery — record as "alias.column"
                                    result.recordColumn(tableRef, colName, fileDetail);
                                }
                                // else: column belongs to an outer table; skip here.
                            } else {
                                result.recordColumn(
                                        resolved != null ? resolved : tableRef,
                                        colName,
                                        fileDetail);
                            }
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
         * Extract standalone (unqualified) column names from expressions.
         * Resolves via scope stack first (handles subquery alias refs like "b.sub_id"
         * where "b" is mapped to "__subquery__" — records as "b.col"), then falls back
         * to currentTable for unqualified names.
         */
        private void extractStandaloneFromExpression(Expression expr) {
            if (expr == null) return;
            String text = expr.toString();
            // IMPORTANT: [a-zA-Z_][a-zA-Z0-9_]* uses . as ANY-CHAR meta-character,
            // so ".product_id" is matched as a single word. Fix: use literal-dot escape "[^.\s]+"
            // or just change the pattern to not use . in the character class.
            // Use [^.\s] to match any char except dot (literal) or whitespace.
            Matcher m = Pattern.compile("\\b([a-zA-Z_][a-zA-Z0-9_]*)\\b").matcher(text);
            while (m.find()) {
                String word = m.group(1);
                if (word.contains(".")) continue;  // skip qualified refs (handled by resolve below)
                if (isSqlKeyword(word) || isNonColumnWord(word)) continue;

                String resolved = resolve(word);
                if (resolved != null) {
                    // Known alias — record with resolved table name.
                    result.recordColumn(resolved, word, fileDetail);
                } else {
                    // Unknown word — record as currentTable.column
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
