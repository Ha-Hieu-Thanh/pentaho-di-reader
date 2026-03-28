# PLAN_TO_FIX.MD — Fixing SQL Alias Resolution in `SqlExtractor`

## Context

The `SelectColumnExtractor` inner class in `SqlExtractor.java` has multiple bugs that cause:
1. Alias names leaking into output as table names (`products.p`)
2. JOIN ON clause columns being silently dropped (`oi.order_id`, `oi.product_id`)
3. INSERT target column lists not being extracted

---

## Fixes

### Fix 1: `extractStandaloneFromExpression` Pass 2 — skip alias names

**File:** `src/main/java/com/app/extractor/SqlExtractor.java`
**Method:** `SelectColumnExtractor.extractStandaloneFromExpression`, ~line 780

**Change:**
```java
// Before (Pass 2, inside the bare.find() loop):
String resolved = resolve(word);
if (resolved != null) {
    result.recordColumn(resolved, word, fileDetail);
} else {
    result.recordColumn(currentTable, word, fileDetail);
}

// After:
String resolved = resolve(word);
if (resolved != null) {
    // word is a table or alias name — skip it entirely
    continue;
}
result.recordColumn(currentTable, word, fileDetail);
```

**Why:** After stripping qualified refs, the alias `p` from `p.active = 1` remains as a bare word. `resolve("p")` finds it in the map and records `products.p` (alias name as column name). The fix skips any word that is a known alias/table name.

---

### Fix 2: `handleColumn()` fallback to `currentTable` instead of `tableRef`

**File:** `src/main/java/com/app/extractor/SqlExtractor.java`
**Method:** `SelectColumnExtractor.handleColumn`, ~line 738

**Change:**
```java
// Before:
result.recordColumn(
    resolved != null ? resolved : tableRef,  // tableRef may be a stray alias
    colName, fileDetail);

// After:
result.recordColumn(
    resolved != null ? resolved : currentTable, // always fall back to real table
    colName, fileDetail);
```

**Why:** If a qualified column's table reference is not in the alias map (theoretical case), falling back to `tableRef` records the literal alias string as the table name. The correct fallback is `currentTable` (the leftmost FROM table), which is a real table name.

---

### Fix 3: `buildAliasMap()` — traverse JOIN ON expressions

**File:** `src/main/java/com/app/extractor/SqlExtractor.java`
**Method:** `SelectColumnExtractor.buildAliasMap`, inside the JOIN loop

**Change:**
```java
// Inside the for (Join join : ps.getJoins()) loop, after registerFromItem(right, false):
if (join.getOnExpression() != null) {
    traverseExpr(join.getOnExpression());
}
```

**Why:** `buildAliasMap()` registers aliases but never traverses the ON expression of JOINs. Columns like `oi.order_id` in `ON oi.order_id = o.id` are never extracted because `traverseExpr` is only called from the SELECT/WHERE/GROUP BY/ORDER BY paths, which don't include ON clauses.

---

### Fix 4: `extractInsert()` — extract INSERT column list

**File:** `src/main/java/com/app/extractor/SqlExtractor.java`
**Method:** `extractInsert`, after `result.recordTable(defaultTable, fileDetail)`

**Change:**
```java
// After recording the target table, extract its column list:
if (insert.getColumns() != null) {
    for (Column col : insert.getColumns()) {
        result.recordColumn(defaultTable, col.getColumnName(), fileDetail);
    }
}
```

**Why:** The INSERT column list `(product_id, sale_date, quantity_sold, revenue)` names the columns of the target table being written to. These are legitimate column references that should appear in the report.

---

## Critical Files

| File | Changes |
|---|---|
| `src/main/java/com/app/extractor/SqlExtractor.java` | Fix 1–4 in `SelectColumnExtractor` inner class + `extractInsert()` |
| `src/test/java/com/app/extractor/SqlExtractorTest.java` | Already added `testInsertSelectWithAliases_fullExample` |
| `src/test/java/com/app/extractor/SqlExtractorDebugTest.java` | Existing `debugUpdateWithAlias` / `debugUpdateFull` |

---

## Verification

After applying all fixes, run:

```bash
mvn test -Dmaven.resolver.transport.wagon
```

Expected outcomes:
- `SqlExtractorTest.testInsertSelectWithAliases_fullExample` — **PASS**: all 4 tables + all expected columns present, no alias leaks
- `SqlExtractorDebugTest.debugUpdateWithAlias` — **PASS**: no `products.p` in output
- `SqlExtractorDebugTest.debugUpdateFull` — **PASS**: no `products.p` in output, `assertFalse` passes
- All existing tests continue to pass

Then run against real sample files:
```bash
mvn exec:java -Dexec.mainClass=com.app.Main -Dexec.args="sample-files" -Dmaven.resolver.transport.wagon
```
