@echo off
setlocal enabledelayedexpansion
set "JDK=C:\Program Files\BellSoft\LibericaJDK-17\bin\javap.exe"
set "JAR=C:\Users\Thanh\.m2\repository\com\github\jsqlparser\jsqlparser\4.9\jsqlparser-4.9.jar"

for %%c in (
  "net.sf.jsqlparser.statement.select.Join"
  "net.sf.jsqlparser.statement.select.SelectItemVisitor"
) do (
    echo.=== %%c ===
    "%JDK%" -p -classpath "%JAR%" %%c
    echo.
)
