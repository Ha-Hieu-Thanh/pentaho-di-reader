package com.app;

import com.app.cli.CliInput;
import com.app.extractor.SqlExtractor;
import com.app.model.ExtractionResult;
import com.app.model.FileDetail;
import com.app.parser.KettleParser;
import com.app.report.ExcelReportGenerator;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

/**
 * Main entry point. Orchestrates CLI input → XML parsing → SQL extraction → Excel report.
 *
 * Usage:
 *   java -cp ... com.app.Main                          (interactive mode)
 *   java -cp ... com.app.Main /path/to/file.ktr       (direct path mode)
 */
public class Main {

    private final KettleParser kettleParser;
    private final SqlExtractor sqlExtractor;
    private final ExcelReportGenerator reportGenerator;

    public Main() {
        this.kettleParser = new KettleParser();
        this.sqlExtractor = new SqlExtractor();
        this.reportGenerator = new ExcelReportGenerator();
    }

    public static void main(String[] args) {
        new Main().run(args);
    }

    public void run(String[] args) {
        printBanner();

        List<File> files = collectFiles(args);
        if (files == null || files.isEmpty()) {
            return;
        }

        File firstFile = files.get(0);
        File outputDir = firstFile.isFile()
                ? firstFile.getParentFile()
                : firstFile;

        System.out.println("\nProcessing " + files.size() + " file(s)...\n");

        ExtractionResult result = new ExtractionResult();
        int processedCount = 0;
        int errorCount = 0;

        for (File file : files) {
            System.out.println("  " + file.getName());
            FileDetail fileDetail = new FileDetail(file.getName(), file.getAbsolutePath());
            result.addFileDetail(fileDetail);

            try {
                List<String> queries = kettleParser.parse(file);
                System.out.println("    Found " + queries.size() + " SQL query(ies)");

                for (String sql : queries) {
                    sqlExtractor.extract(sql, fileDetail, result);
                }
                processedCount++;

            } catch (Exception e) {
                System.err.println("    Error parsing " + file.getName() + ": " + e.getMessage());
                errorCount++;
            }
        }

        File outputFile = new File(outputDir, "sql_usage_report2.xlsx");

        try {
            reportGenerator.generate(result, outputFile);
            printSummary(result, processedCount, errorCount, outputFile);
        } catch (Exception e) {
            System.err.println("Error generating Excel report: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private List<File> collectFiles(String[] args) {
        if (args.length > 0 && !args[0].trim().isEmpty()) {
            return collectFromPath(args[0].trim());
        }
        CliInput cli = new CliInput();
        return cli.promptAndCollectFiles();
    }

    private List<File> collectFromPath(String pathStr) {
        Path path = Paths.get(pathStr).toAbsolutePath().normalize();
        File file = path.toFile();
        CliInput cli = new CliInput();

        if (!file.exists()) {
            System.out.println("Error: Path does not exist: " + path);
            return null;
        }

        if (file.isFile()) {
            if (!cli.isKettleFile(file)) {
                System.out.println("Error: File must have .ktr or .kjb extension.");
                return null;
            }
            return Collections.singletonList(file);
        }

        if (file.isDirectory()) {
            List<File> files = cli.collectKettleFiles(file);
            if (files.isEmpty()) {
                System.out.println("Error: No .ktr or .kjb files found in the directory.");
                return null;
            }
            return files;
        }

        System.out.println("Error: Invalid path.");
        return null;
    }

    private void printBanner() {
        System.out.println();
        System.out.println("╔════════════════════════════════════════════╗");
        System.out.println("║     Pentaho DI Reader - SQL Extractor    ║");
        System.out.println("╚════════════════════════════════════════════╝");
        System.out.println();
    }

    private void printSummary(ExtractionResult result, int processedCount,
                              int errorCount, File outputFile) {
        int tableCount = result.getTableUsagesSortedByCount().size();
        int columnCount = result.getColumnUsagesSortedByTableAndCount().size();

        System.out.println();
        System.out.println("══════════════════════════════════════════════");
        System.out.println("              REPORT GENERATED                 ");
        System.out.println("══════════════════════════════════════════════");
        System.out.printf("  Files processed  : %d%n", processedCount);
        System.out.printf("  Files with errors: %d%n", errorCount);
        System.out.printf("  Unique tables   : %d%n", tableCount);
        System.out.printf("  Unique columns  : %d%n", columnCount);
        System.out.printf("  Output file     : %s%n", outputFile.getAbsolutePath());
        System.out.println("══════════════════════════════════════════════");

        if (tableCount == 0) {
            System.out.println("\nNo SQL tables found. The Excel report is empty.");
            System.out.println("Check that your .ktr/.kjb files contain valid SQL queries.");
        }
    }
}
