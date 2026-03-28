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
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseLeftShift;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseRightShift;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.IntegerDivision;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.conditional.XorExpression;

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
 * - WHERE/ORDER BY/GROUP BY/HAVING: manual AST traversal to extract Column nodes
 *   (ExpressionVisitorAdapter.visit(Column) is a no-op in JSQLParser 4.9)
 * - Only qualified (table.column) references are extracted — no regex for alias-bearing cols
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

        // Fix 4: extract INSERT column list (e.g. INSERT INTO t (col1, col2) SELECT ...)
        if (insert.getColumns() != null) {
            for (Column col : insert.getColumns()) {
                result.recordColumn(defaultTable, col.getColumnName(), fileDetail);
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
        if (update.getWhere() != null) {
            SelectColumnExtractor outer = new SelectColumnExtractor(fileDetail, result, parentAliasMap);
            outer.traverseExpr(update.getWhere());
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
        extractColumnsFromText(delete.toString(), table != null ? table.getName() : "",
                parentAliasMap, fileDetail, result);
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
        extractColumnsFromText(merge.toString(), table != null ? table.getName() : "",
                parentAliasMap, fileDetail, result);
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
        extractUpdateSetColumns(us, defaultTable, new HashMap<>(), fileDetail, result);
    }

    private void extractUpdateSetColumns(UpdateSet us, String defaultTable,
                                        Map<String, String> aliasMap,
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
                extractColumnsFromText(o.toString(), defaultTable, aliasMap, fileDetail, result);
            }
        }
    }

    /**
     * Uses regex on expression text to extract qualified "table.column" references,
     * resolving each table identifier through aliasMap before recording.
     * Falls back to defaultTable when the table ref cannot be resolved.
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
            // Resolve alias to real table name; fall back to defaultTable for safety
            String resolved = aliasMap.get(tableRef);
            result.recordColumn(
                    resolved != null ? resolved : defaultTable,
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
            if (lss.getSelect() != null) {
                lss.getSelect().accept(this);
            }
        }

        @Override public void visit(ParenthesedSelect ps) {
            if (ps.getSelect() != null) {
                // When the FROM clause is a subquery "(SELECT ...) AS x", we need to:
                // 1. Process the inner SELECT to extract its columns/aliases (recursive, using
                //    SAME extractor so outer scope aliases like "aa" are available).
                // 2. NOT let the inner query overwrite currentTable, so outer-level bare columns
                //    (which reference the subquery output) still use the correct default table.
                String savedCurrentTable = currentTable;
                ps.getSelect().accept(this);        // process inner select with full outer scope
                currentTable = savedCurrentTable;    // restore so outer bare cols work correctly
            }
        }

        @Override public void visit(TableStatement ts) {
            // Simple table ref — already handled by TablesNamesFinder
        }

        // ── Main processing ───────────────────────────────────────────────

        private void processSelectBody(PlainSelect ps) {
            pushScope();
            try {
                buildAliasMap(ps);

                // 1. Extract columns from SELECT list using Column nodes directly.
                if (ps.getSelectItems() != null) {
                    for (SelectItem<?> item : ps.getSelectItems()) {
                        extractColumnsFromSelectItem(item);
                    }
                }

                // 2. Extract columns from WHERE (AST + standalone fallback)
                if (ps.getWhere() != null) {
                    traverseExpr(ps.getWhere());
                    if (!hasJoin(ps)) {
                        extractStandaloneFromExpression(ps.getWhere());
                    }
                }

                // 3. Extract columns from GROUP BY
                if (ps.getGroupBy() != null) {
                    ExpressionList groupByExprs = ps.getGroupBy().getGroupByExpressions();
                    if (groupByExprs != null) {
                        for (Object o : groupByExprs) {
                            Expression e = (Expression) o;
                            traverseExpr(e);
                            if (!hasJoin(ps)) {
                                extractStandaloneFromExpression(e);
                            }
                        }
                    }
                }

                // 4. Extract columns from HAVING
                if (ps.getHaving() != null) {
                    traverseExpr(ps.getHaving());
                    if (!hasJoin(ps)) {
                        extractStandaloneFromExpression(ps.getHaving());
                    }
                }

                // 5. Extract columns from ORDER BY
                if (ps.getOrderByElements() != null) {
                    for (OrderByElement elem : ps.getOrderByElements()) {
                        if (elem.getExpression() != null) {
                            traverseExpr(elem.getExpression());
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
            registerFromItem(fromItem, true);

            // JOIN items
            if (ps.getJoins() != null) {
                for (Join join : ps.getJoins()) {
                    FromItem right = join.getRightItem();
                    registerFromItem(right, false);
                    // Fix 3: also traverse JOIN ON expressions to extract columns there
                    if (join.getOnExpression() != null) {
                        traverseExpr(join.getOnExpression());
                    }
                }
            }

            // LATERAL VIEWS
            if (ps.getLateralViews() != null) {
                for (LateralView lv : ps.getLateralViews()) {
                    if (lv.getTableAlias() != null) {
                        putAlias(lv.getTableAlias().getName(), lv.getTableAlias().getName());
                    }
                }
            }
        }

        /**
         * Registers a FROM item in the current scope's alias map.
         * - Table: realName → realName, alias → realName, currentTable = realName (if first)
         * - ParenthesedSelect (subquery with alias): alias → "__subquery__"; records its column names.
         * - ParenthesedSelect (anonymous, no alias): processes inner SELECT recursively so outer
         *   scope aliases are available; preserves currentTable so outer-level bare columns still work.
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
                    // Named subquery — register alias and record its output column names
                    putAlias(alias, "__subquery__");
                    Set<String> subCols = new HashSet<>();
                    PlainSelect innerPs = ss.getSelect().getPlainSelect();
                    if (innerPs != null && innerPs.getSelectItems() != null) {
                        for (SelectItem<?> si : innerPs.getSelectItems()) {
                            String colName = extractColName(si);
                            if (colName != null) subCols.add(colName.toLowerCase());
                        }
                    }
                    subqueryColMap.put(alias.toLowerCase(), subCols);
                } else {
                    // Anonymous subquery — no alias available. Outer bare columns (like
                    // "action_code") cannot be attributed to a specific table. Instead,
                    // process the inner SELECT recursively so that qualified refs inside it
                    // (e.g. "aa.action_code") are resolved using the outer scope.  Save and
                    // restore currentTable so the outer-level SELECT bare columns remain
                    // attributed to whatever the outer FROM item set as currentTable.
                    String savedCurrentTable = currentTable;
                    if (setAsCurrent) currentTable = "";  // can't safely attribute outer bare cols
                    if (ss.getSelect() != null) {
                        ss.getSelect().accept(this);
                    }
                    currentTable = savedCurrentTable;
                }
            }
        }

        private String extractColName(SelectItem<?> si) {
            String s = si.toString();
            int asIdx = s.toUpperCase().lastIndexOf(" AS ");
            if (asIdx > 0) return s.substring(asIdx + 4).trim();
            int dot = s.lastIndexOf('.');
            return dot > 0 ? s.substring(dot + 1).trim() : s.trim();
        }

        // ── Extract columns from SelectItem (SELECT list) ──────────────────

        private void extractColumnsFromSelectItem(SelectItem<?> item) {
            SelectItemVisitor vis = new SelectItemVisitorAdapter() {
                @Override
                public void visit(SelectItem si) {
                    if (si.getExpression() == null) return;
                    traverseExpr(si.getExpression());
                }
            };
            item.accept(vis);
        }

        // ── Extract columns from any Expression ────────────────────────────

        /**
         * Extracts Column nodes from an Expression using a manual tree traversal.
         * JSQLParser 4.9's ExpressionVisitorAdapter.visit(Column) is a no-op, so
         * this method handles it directly with instanceof checks.
         */
        private void traverseExpr(Expression expr) {
            if (expr == null) return;

            if (expr instanceof Column) {
                handleColumn((Column) expr);
                return;
            }

            // BinaryExpression covers: ComparisonOperator (EqualsTo, NotEqualsTo, etc.),
            // arithmetic operators (Addition, Subtraction, etc.),
            // and conditional operators (AndExpression, OrExpression, XorExpression).
            if (expr instanceof net.sf.jsqlparser.expression.BinaryExpression) {
                net.sf.jsqlparser.expression.BinaryExpression be =
                        (net.sf.jsqlparser.expression.BinaryExpression) expr;
                if (be.getLeftExpression() instanceof Expression)
                    traverseExpr((Expression) be.getLeftExpression());
                if (be.getRightExpression() instanceof Expression)
                    traverseExpr((Expression) be.getRightExpression());
                return;
            }

            if (expr instanceof net.sf.jsqlparser.expression.Function) {
                net.sf.jsqlparser.expression.Function f =
                        (net.sf.jsqlparser.expression.Function) expr;
                if (f.getParameters() != null) {
                    for (Object a : f.getParameters().getExpressions()) {
                        if (a instanceof Expression) traverseExpr((Expression) a);
                    }
                }
                return;
            }

            if (expr instanceof net.sf.jsqlparser.expression.Parenthesis) {
                net.sf.jsqlparser.expression.Parenthesis p =
                        (net.sf.jsqlparser.expression.Parenthesis) expr;
                if (p.getExpression() instanceof Expression)
                    traverseExpr((Expression) p.getExpression());
                return;
            }

            if (expr instanceof net.sf.jsqlparser.expression.SignedExpression) {
                net.sf.jsqlparser.expression.SignedExpression se =
                        (net.sf.jsqlparser.expression.SignedExpression) expr;
                if (se.getExpression() instanceof Expression)
                    traverseExpr((Expression) se.getExpression());
                return;
            }

            if (expr instanceof net.sf.jsqlparser.expression.NotExpression) {
                net.sf.jsqlparser.expression.NotExpression ne =
                        (net.sf.jsqlparser.expression.NotExpression) expr;
                if (ne.getExpression() instanceof Expression)
                    traverseExpr((Expression) ne.getExpression());
                return;
            }

            if (expr instanceof net.sf.jsqlparser.expression.CaseExpression) {
                net.sf.jsqlparser.expression.CaseExpression ce =
                        (net.sf.jsqlparser.expression.CaseExpression) expr;
                if (ce.getSwitchExpression() instanceof Expression)
                    traverseExpr((Expression) ce.getSwitchExpression());
                if (ce.getElseExpression() instanceof Expression)
                    traverseExpr((Expression) ce.getElseExpression());
                for (net.sf.jsqlparser.expression.WhenClause w : ce.getWhenClauses()) {
                    if (w.getWhenExpression() instanceof Expression)
                        traverseExpr((Expression) w.getWhenExpression());
                    if (w.getThenExpression() instanceof Expression)
                        traverseExpr((Expression) w.getThenExpression());
                }
                return;
            }

            if (expr instanceof net.sf.jsqlparser.expression.operators.relational.ExistsExpression) {
                net.sf.jsqlparser.expression.operators.relational.ExistsExpression ee =
                        (net.sf.jsqlparser.expression.operators.relational.ExistsExpression) expr;
                if (ee.getRightExpression() instanceof Expression)
                    traverseExpr((Expression) ee.getRightExpression());
                return;
            }

            if (expr instanceof net.sf.jsqlparser.expression.operators.relational.IsNullExpression) {
                net.sf.jsqlparser.expression.operators.relational.IsNullExpression ine =
                        (net.sf.jsqlparser.expression.operators.relational.IsNullExpression) expr;
                if (ine.getLeftExpression() instanceof Expression)
                    traverseExpr((Expression) ine.getLeftExpression());
                return;
            }

            if (expr instanceof net.sf.jsqlparser.expression.operators.relational.InExpression) {
                net.sf.jsqlparser.expression.operators.relational.InExpression ie =
                        (net.sf.jsqlparser.expression.operators.relational.InExpression) expr;
                if (ie.getLeftExpression() instanceof Expression)
                    traverseExpr((Expression) ie.getLeftExpression());
                if (ie.getRightExpression() instanceof Expression)
                    traverseExpr((Expression) ie.getRightExpression());
                return;
            }

            if (expr instanceof net.sf.jsqlparser.expression.operators.relational.Between) {
                net.sf.jsqlparser.expression.operators.relational.Between b =
                        (net.sf.jsqlparser.expression.operators.relational.Between) expr;
                if (b.getLeftExpression() instanceof Expression)
                    traverseExpr((Expression) b.getLeftExpression());
                if (b.getBetweenExpressionStart() instanceof Expression)
                    traverseExpr((Expression) b.getBetweenExpressionStart());
                if (b.getBetweenExpressionEnd() instanceof Expression)
                    traverseExpr((Expression) b.getBetweenExpressionEnd());
                return;
            }

            if (expr instanceof net.sf.jsqlparser.expression.operators.relational.LikeExpression) {
                net.sf.jsqlparser.expression.operators.relational.LikeExpression le =
                        (net.sf.jsqlparser.expression.operators.relational.LikeExpression) expr;
                if (le.getLeftExpression() instanceof Expression)
                    traverseExpr((Expression) le.getLeftExpression());
                if (le.getRightExpression() instanceof Expression)
                    traverseExpr((Expression) le.getRightExpression());
                if (le.getEscape() instanceof Expression)
                    traverseExpr((Expression) le.getEscape());
                return;
            }

            if (expr instanceof net.sf.jsqlparser.expression.CastExpression) {
                net.sf.jsqlparser.expression.CastExpression ce =
                        (net.sf.jsqlparser.expression.CastExpression) expr;
                if (ce.getLeftExpression() instanceof Expression)
                    traverseExpr((Expression) ce.getLeftExpression());
                return;
            }

            // All other expression types — skip silently (literals, subqueries, etc.)
        }

        private void handleColumn(Column col) {
            String colName = col.getColumnName();
            String tableRef = col.getTable() != null ? col.getTable().getName() : null;
            if (tableRef != null && !tableRef.isEmpty()) {
                String resolved = resolve(tableRef);
                if ("__subquery__".equals(resolved)) {
                    // A column referencing a subquery alias (e.g. "b.col_name") is a subquery
                    // OUTPUT column — not a real table column. Do NOT record it here; the inner
                    // SELECT's own FROM/JOIN processing already extracts the real source columns
                    // (e.g. sd.att_detail_code) via the recursive anonymous-subquery path.
                    return;
                } else {
                    // Fix 2: fall back to currentTable (real table), not tableRef (alias string)
                    result.recordColumn(
                            resolved != null ? resolved : currentTable,
                            colName,
                            fileDetail);
                }
            }
        }

        /**
         * Extract standalone (unqualified) column names from expressions.
         * Uses TWO PASSES to avoid the regex-dot-any-char pitfall:
         * 1. Qualified pass — extract "table.col" tokens and resolve alias → real table.
         * 2. Unqualified pass — scan remaining text for bare words, filtering alias names.
         */
        private void extractStandaloneFromExpression(Expression expr) {
            if (expr == null) return;
            String text = expr.toString();

            // Pass 1: qualified refs — prevent "order_items.oi" from matching "oi" as standalone
            Matcher qual = Pattern.compile(
                    "(?:^|(?=[^\\w.]))([a-zA-Z_][a-zA-Z0-9_]*)\\.([a-zA-Z_][a-zA-Z0-9_]*)"
            ).matcher(text);
            while (qual.find()) {
                String tableRef = qual.group(1);
                String colName  = qual.group(2);
                String resolved = resolve(tableRef);
                result.recordColumn(
                        resolved != null ? resolved : currentTable,
                        colName,
                        fileDetail);
            }

            // Pass 2: unqualified bare words, but only outside qualified tokens
            String stripped = text.replaceAll(
                    "(?:^|(?=[^\\w.]))([a-zA-Z_][a-zA-Z0-9_]*)\\.([a-zA-Z_][a-zA-Z0-9_]*)",
                    ""
            );
            Matcher bare = Pattern.compile("\\b([a-zA-Z_][a-zA-Z0-9_]*)\\b").matcher(stripped);
            while (bare.find()) {
                String word = bare.group(1);
                if (isSqlKeyword(word) || isNonColumnWord(word)) continue;

                // Fix 1: skip words that are alias/table names — they are table refs, not columns
                String resolved = resolve(word);
                if (resolved != null) {
                    continue;
                }
                result.recordColumn(currentTable, word, fileDetail);
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
