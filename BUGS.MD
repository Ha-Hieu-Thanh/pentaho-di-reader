# BUGS.MD — SQL Alias Resolution Issues in `SqlExtractor`

Confirmed by running tests. Test output for the INSERT SELECT query:

```
=== INSERT SELECT columns ===
  order_items.quantity
  order_items.unit_price
  orders.order_date
  products.id
```

Expected columns missing: `daily_sales.product_id`, `daily_sales.sale_date`, `daily_sales.quantity_sold`, `daily_sales.revenue`, `order_items.order_id`, `order_items.product_id`.

Also confirmed: `products.p = 1` alias leak in UPDATE path.

---

## Bug 1: `handleColumn()` falls back to alias name instead of `currentTable`

**File:** `src/main/java/com/app/extractor/SqlExtractor.java`, line ~738 (`handleColumn` method)

**Severity:** Medium — produces garbage table names that pollute output.

**Root cause:** When a qualified column reference like `p.id` is encountered and the alias `p` is **not** found in the scope stack's alias map, the fallback incorrectly records `tableRef` (the alias string `"p"`) as the table name.

```java
// Current (BUGGY) — line ~738:
result.recordColumn(
    resolved != null ? resolved : tableRef,   // ← BUG: "p" recorded as table!
    colName,
    fileDetail);

// Fix:
result.recordColumn(
    resolved != null ? resolved : currentTable, // ← fall back to real table
    colName,
    fileDetail);
```

**Reproduction (from `SqlExtractorDebugTest.debugUpdateFull`):**
```sql
UPDATE products p SET p.total_sold = (
  SELECT COALESCE(SUM(oi.quantity), 0) FROM order_items oi WHERE oi.product_id = p.id
), p.last_sold_date = (...) WHERE p.active = 1
```
- `oi.product_id` → correctly resolved to `order_items.product_id`
- `p.id` → **BUG**: `products.p` is recorded (alias `"p"` used as table name)
- `p.active` → **BUG**: `products.p` recorded again

**Why the alias is missing from the map:** In `extractUpdate()`, `SelectColumnExtractor` is constructed directly for the WHERE clause (`outer.traverseExpr(update.getWhere())`). `buildAliasMap()` is never called, so only the parent scope's aliases are available (set at construction time from `parentAliasMap`). The outer UPDATE alias `p → products` IS in the parent scope — but wait, it IS registered in `parentAliasMap`. Let me re-examine...

Actually, looking at `extractUpdate()`:
```java
if (table.getAlias() != null) {
    parentAliasMap.put(table.getAlias().getName(), tableName);  // "p" → "products"
}
```
`p → products` IS in the parent scope. Then `resolve("p")` should find it. So `handleColumn` should resolve `p.id` → `products.id`.

But the test shows `products.p = 1`. So either:
1. `col.getTable().getName()` returns something other than `"p"`, OR
2. `resolve("p")` is not finding it

Let me add debug to understand. Actually — looking at the column `p.active` in the WHERE, `active` is not a known column. The record `products.p` means the table was recorded as `"p"`. This means `tableRef` (the fallback) was `"p"`, and `resolved` was null. That means `resolve("p")` returned null despite `p → products` being in the parent scope.

Wait — `extractUpdate` creates `parentAliasMap` from the UPDATE table. Then creates `SelectColumnExtractor` with that map in the scope stack root. `resolve()` searches from innermost to outermost scope. With a standalone `traverseExpr(update.getWhere())`, only the root scope exists (from constructor). So `resolve("p")` SHOULD find it.

Unless the `handleColumn` code path has a different issue. Let me trace: `tableRef = col.getTable().getName()` — for `p.active`, this is `"p"`. `resolved = resolve("p")`. If `p` is in the map, `resolved = "products"`. So the ternary `resolved != null ? resolved : tableRef` gives `"products"`. NOT `"p"`.

So the bug is: `resolved` is null for `p` — meaning `p → products` was NOT in the scope stack when `handleColumn` was called for `p.active`.

The issue: `extractUpdate` is called for `update.getWhere()`. But `update.getSelect()` is ALSO called, which calls `extractSelect(update.getSelect(), ...)`. `extractSelect` creates a NEW `SelectColumnExtractor` (not the same one used for the WHERE). That one runs `processSelectBody` → `pushScope()` → `buildAliasMap()`. The new scope from `processSelectBody` does NOT inherit the UPDATE's `parentAliasMap` — it has its own root scope.

But that shouldn't affect the WHERE path. Let me look at `extractUpdate` again:

```java
private void extractUpdate(Update update, FileDetail fileDetail, ExtractionResult result) {
    Table table = update.getTable();
    Map<String, String> parentAliasMap = new HashMap<>();
    if (table != null) {
        String tableName = table.getName();
        result.recordTable(tableName, fileDetail);
        parentAliasMap.put(tableName, tableName);          // "products" → "products"
        if (table.getAlias() != null) {
            parentAliasMap.put(table.getAlias().getName(), tableName); // "p" → "products"
        }
    }
    if (update.getSelect() != null) {
        extractSelect(update.getSelect(), fileDetail, result, parentAliasMap);
    }
    if (update.getWhere() != null) {
        SelectColumnExtractor outer = new SelectColumnExtractor(fileDetail, result, parentAliasMap);
        outer.traverseExpr(update.getWhere());
    }
}
```

Hmm, `extractSelect(update.getSelect(), ...)` is called first, then `outer.traverseExpr(update.getWhere())`. Both use the same `parentAliasMap`. But they create different `SelectColumnExtractor` instances.

For the SELECT path (in `extractSelect`):
```java
SelectColumnExtractor extractor = new SelectColumnExtractor(fileDetail, result, parentAliasMap);
select.accept(extractor);
```
The `select.accept(extractor)` visits `PlainSelect` → `processSelectBody` → `pushScope()` → `buildAliasMap()`. `buildAliasMap` gets `ps.getFromItem()` and `ps.getJoins()`. For the UPDATE SET subqueries, the FROM item is a subquery. The alias map is built for those subqueries, not for the outer UPDATE.

But wait, the UPDATE WHERE clause (`p.active = 1`) is not inside the UPDATE SET subqueries. It's at the UPDATE level. So `update.getSelect()` is the scalar subquery in the SET clause, not the WHERE. The WHERE is `update.getWhere()`.

So: `extractSelect(update.getSelect(), ...)` handles the scalar subqueries. Then `outer.traverseExpr(update.getWhere())` handles the WHERE. These are independent.

For `outer.traverseExpr(update.getWhere())`:
- `SelectColumnExtractor outer = new SelectColumnExtractor(fileDetail, result, parentAliasMap)` creates one with scope stack = `[parentAliasMap]`.
- `parentAliasMap` has `"products" → "products"` and `"p" → "products"`.
- `resolve("p")` should find it in the root scope.

Unless... `col.getTable().getName()` for `p.active` is NOT `"p"`. Let me check what JSQLParser returns for `p.active` when `p` is an alias on the UPDATE table. `p.active` — the table `p` in a Column is the alias `"p"`. JSQLParser stores the table name as the string that appears. For an aliased table, `table.getName()` returns the alias.

Actually, wait. Let me re-read `extractUpdate`:
```java
parentAliasMap.put(table.getAlias().getName(), tableName);
```

`table.getAlias().getName()` = `"p"`. `tableName` = `"products"`. So `p → products`.

But in `handleColumn`:
```java
String resolved = resolve(tableRef);  // tableRef = "p"
```
`resolve("p")` searches the scope stack. The root scope has `{"products": "products", "p": "products"}`. So `resolve("p")` returns `"products"`.

So `resolved != null` is true → records `"products".active`. Not `"p".active`.

But the test shows `products.p = 1`. This means the fallback `tableRef` was used, meaning `resolved` was null. Why?

Unless... the `ParenthesedSelect` alias registration clears the map? No, that's inside the subquery extractor.

Actually, let me look at `buildAliasMap` again:
```java
private void buildAliasMap(PlainSelect ps) {
    subqueryColMap.clear();
    ...
}
```

`subqueryColMap.clear()` — this is the instance-level map cleared at the start of each `buildAliasMap`. But this is for subquery column tracking, not alias resolution.

Let me reconsider. The issue might be with the `currentTable` field. When `handleColumn` processes an unqualified column, it uses `currentTable`. But for qualified columns, it uses `resolve`. If `resolve` returns null for `"p"` even though `"p"` is in the root scope, the fallback is `tableRef` = `"p"`.

Is it possible that `parentAliasMap` in `extractUpdate` is modified later? No, it's a local variable.

Actually, wait — let me look at what `parentAliasMap` contains more carefully:
```java
parentAliasMap.put(tableName, tableName);          // "products" → "products"
parentAliasMap.put(table.getAlias().getName(), tableName); // "p" → "products"
```

For the UPDATE table `products p`, `table.getName()` = `"products"`. `table.getAlias().getName()` = `"p"`. So map = `{"products": "products", "p": "products"}`.

`resolve("p")` searches `scopeStack` = `[{"products": "products", "p": "products"}]`. `scopeStack.get(0).get("p")` = `"products"`. Not null.

So the logic seems correct. But the test shows `products.p = 1`. Let me just run the debug test again to confirm.

Actually, looking at the debug test output again:
```
products.p = 1
products.active = 1
products.id = 1
```

`products.active = 1` and `products.id = 1` are correct! They show `products` as the table. Only `products.p = 1` is wrong. So `p.active` and `p.id` WERE resolved to `products`, but `p` alone was also recorded.

This means the column `p` (just the alias name without a column) was recorded as a column of table `products`. How?

Looking at `extractStandaloneFromExpression` — Pass 2:
```java
Matcher bare = Pattern.compile("\\b([a-zA-Z_][a-zA-Z0-9_]*)\\b").matcher(stripped);
while (bare.find()) {
    String word = bare.group(1);
    if (isSqlKeyword(word) || isNonColumnWord(word)) continue;

    String resolved = resolve(word);
    if (resolved != null) {
        result.recordColumn(resolved, word, fileDetail);  // ← records "products"."p"!
    } else {
        result.recordColumn(currentTable, word, fileDetail);
    }
}
```

The WHERE clause text is `p.active = 1`. After stripping qualified refs from Pass 1: `p` was NOT stripped in Pass 1 (Pass 1 uses a different pattern that only matches `table.col`). Then Pass 2: `bare.find()` finds `p`, `active`, `1`. `isSqlKeyword("p")` → false. `isNonColumnWord("p")` → false. `resolve("p")` → `"products"`. So `result.recordColumn("products", "p", fileDetail)` — recording `p` as a column name!

So the bug is in **Pass 2 of `extractStandaloneFromExpression`**: when the stripped text contains an alias name (like `p` from `p.active = 1`), it resolves to the table and records the alias as a column name. The fix should filter out alias names from the standalone column list:

```java
// In Pass 2, also skip words that are in the alias map (they're table refs, not columns)
if (isSqlKeyword(word) || isNonColumnWord(word)) continue;
String resolved = resolve(word);
if (resolved != null) continue;  // ← skip: it's an alias/table ref, not a column
result.recordColumn(currentTable, word, fileDetail);
```

But wait, this would also skip `products.id` if `products` is in the alias map. That's a real table name, not an alias. In Pass 2 of `extractStandaloneFromExpression`, `products` from the expression text should also be filtered.

The real issue: after stripping qualified tokens, the remaining text still contains the alias as a standalone word. The fix should skip any word that resolves via `resolve()` — whether it's an alias or a table name.

But for a real table name in an unqualified reference... when would that happen? If someone writes `WHERE products.id = 1` without an alias, `products` IS in the alias map (from `putAlias(realName, realName)`). So `resolve("products")` returns `"products"`. And we want to record `products.id`. But `products.id` is a qualified reference, so it would be handled in Pass 1, not Pass 2.

So Pass 2 is for truly unqualified columns — single words that are neither SQL keywords nor aliases/table names. The fix is: if `resolve(word)` returns non-null, skip it (it's a table ref, not a column).

```java
if (isSqlKeyword(word) || isNonColumnWord(word)) continue;
String resolved = resolve(word);
if (resolved != null) continue;  // skip: it's a table/alias name, not a column
result.recordColumn(currentTable, word, fileDetail);
```

Actually, wait. `resolve(word)` returns null if the word is not in the alias map. If the word IS in the alias map (either as an alias or as a real table name), `resolve` returns the real table. This means the word is a table reference, not a column. So skip it.

But what about when `products` appears as a standalone word? This shouldn't happen in normal SQL. In `WHERE products.id = 1`, `products.id` is handled by Pass 1. In `WHERE id = 1`, `id` is not in the alias map, so Pass 2 records it with `currentTable`.

So the fix is correct: skip words that `resolve()` finds in the alias map.

OK so the ACTUAL bug for `products.p = 1` is in `extractStandaloneFromExpression` Pass 2, not in `handleColumn`. The `handleColumn` code path would correctly resolve `p.id` to `products.id`. But `p` (the alias name) is ALSO extracted by the standalone regex, and since `resolve("p")` finds `"products"`, it records `products.p`.

The root cause: **Pass 2 doesn't filter out alias names when `resolve()` finds them**.

Fix: in Pass 2, if `resolve(word)` returns non-null, skip the word (it's a table/alias reference, not a column).

```java
// Pass 2: unqualified bare words
String stripped = text.replaceAll(...qualified pattern..., "");
Matcher bare = Pattern.compile("\\b([a-zA-Z_][a-zA-Z0-9_]*)\\b").matcher(stripped);
while (bare.find()) {
    String word = bare.group(1);
    if (isSqlKeyword(word) || isNonColumnWord(word)) continue;
    String resolved = resolve(word);
    if (resolved != null) continue;  // ← FIX: skip table/alias names
    result.recordColumn(currentTable, word, fileDetail);
}
```

Actually wait — there's also a subtlety with `currentTable`. When `handleColumn` can't resolve an alias, it uses `tableRef` (which is the alias name). This IS a bug. For example, if there's a column `x` in an expression like `WHERE x.id = 1` and `x` is not in the alias map, `handleColumn` would record `x.id` (table = `x`). But `x` is not a real table — it's something else (maybe a function, maybe a CTE name). In this case, `tableRef` is the right fallback (the text says `x.column`), but it would be recorded as table `x`. This is acceptable for the "best effort" approach.

But the more pressing issue is Bug 1's Pass 2 side effect.

Actually, let me reconsider Bug 1 more carefully. The original analysis said `handleColumn` falls back to `tableRef`. Let me verify with the actual test output:

```
products.active = 1   ← correct (p.active resolved)
products.id = 1       ← correct (p.id resolved)
products.p = 1        ← wrong (p recorded as column name)
```

`products.p = 1` comes from Pass 2 recording `p` as a column. `handleColumn` is correct for the qualified refs. So Bug 1's `tableRef` fallback is NOT the primary bug here — it's Pass 2's failure to filter alias names.

BUT — the original `tableRef` fallback in `handleColumn` is still a theoretical bug. If `resolve(tableRef)` returns null for a qualified column, we'd record `tableRef` as the table name. This could happen if a Column has a table name that's not in the alias map. In that case, `tableRef` would be the literal text (like a subquery alias or something weird). In practice this rarely happens because all FROM/JOIN tables are registered. But it's still wrong — should fall back to `currentTable`.

So Bug 1 has two manifestations:
1. **Primary (confirmed)**: `extractStandaloneFromExpression` Pass 2 records alias names as column names → `products.p = 1`. Fix: skip words where `resolve()` is non-null.
2. **Theoretical**: `handleColumn` falls back to `tableRef` instead of `currentTable`. Fix: change fallback to `currentTable`.

---

## Bug 2: `handleColumn()` fallback uses `tableRef` instead of `currentTable`

**File:** `src/main/java/com/app/extractor/SqlExtractor.java`, line ~738

**Severity:** Low (theoretical) — primarily masked by Bug 1's Pass 2 extraction. If Bug 1's Pass 2 is fixed, qualified column refs with unknown table qualifiers would still fall back to the literal (wrong) table name.

```java
// Current:
result.recordColumn(
    resolved != null ? resolved : tableRef,   // tableRef may be a wrong alias
    colName, fileDetail);

// Fix:
result.recordColumn(
    resolved != null ? resolved : currentTable, // always fall back to real table
    colName, fileDetail);
```

---

## Bug 3: `buildAliasMap()` never traverses JOIN ON expressions

**File:** `src/main/java/com/app/extractor/SqlExtractor.java`, `buildAliasMap()` method

**Severity:** High — columns in ON clauses are silently dropped.

**Root cause:** `buildAliasMap()` calls `registerFromItem()` for each JOIN's right-hand table, but **never visits the ON expression** (`join.getOnExpression()`). As a result, columns in ON clauses (like `oi.order_id` in `ON oi.order_id = o.id`) are never traversed and extracted.

```java
private void buildAliasMap(PlainSelect ps) {
    subqueryColMap.clear();
    FromItem fromItem = ps.getFromItem();
    registerFromItem(fromItem, true);  // ← only registers alias, no ON traversal

    if (ps.getJoins() != null) {
        for (Join join : ps.getJoins()) {
            FromItem right = join.getRightItem();
            registerFromItem(right, false); // ← only registers alias, no ON traversal
            // MISSING: join.getOnExpression() is never traversed!
        }
    }
}
```

**Reproduction:**
```sql
JOIN orders o ON oi.order_id = o.id
JOIN products p ON oi.product_id = p.id
```
- `oi.order_id` — not extracted (ON clause)
- `oi.product_id` — not extracted (ON clause)

**Fix:** Add `if (join.getOnExpression() != null) traverseExpr(join.getOnExpression())` inside the JOIN loop in `buildAliasMap()`.

---

## Bug 4: INSERT column list is not extracted

**File:** `src/main/java/com/app/extractor/SqlExtractor.java`, `extractInsert()` method

**Severity:** High — INSERT target table columns are entirely missing from output.

**Root cause:** `extractInsert()` records the target table name but never processes the INSERT column list (`insert.getColumns()`). The column list `(product_id, sale_date, quantity_sold, revenue)` is completely ignored.

```java
private void extractInsert(Insert insert, FileDetail fileDetail, ExtractionResult result) {
    Table table = insert.getTable();
    ...
    result.recordTable(defaultTable, fileDetail);  // ← table recorded
    // MISSING: loop over insert.getColumns() and record each column
    ...
}
```

**Reproduction:**
```sql
INSERT INTO daily_sales (product_id, sale_date, quantity_sold, revenue) SELECT ...
```
**Observed:** `daily_sales` table present, but no columns for it.
**Expected:** 4 columns recorded for `daily_sales`.

**Fix:** Add a loop over `insert.getColumns()` that calls `result.recordColumn()` for each column.
