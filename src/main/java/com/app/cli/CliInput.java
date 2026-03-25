package com.app.cli;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Handles interactive CLI input: prompts user for a file or folder path,
 * validates it, and collects all .ktr/.kjb files to process.
 */
public class CliInput {

    private static final String PROMPT = "Enter a .ktr or .kjb file path, or a folder path: ";
    private static final String INVALID_PATH = "Error: Path does not exist. Please try again.";
    private static final String NO_FILES_FOUND = "Error: No .ktr or .kjb files found in the directory.";
    private static final String WRONG_EXTENSION = "Error: File must have .ktr or .kjb extension.";

    private final InteractiveReader reader;

    public CliInput() {
        this.reader = new InteractiveReader();
    }

    /**
     * Prompts the user interactively and returns the list of files to process.
     *
     * @return list of File objects to process, never null
     */
    public List<File> promptAndCollectFiles() {
        String input = reader.readLine(PROMPT).trim();

        if (input.isEmpty()) {
            System.out.println("Error: No path entered.");
            return Collections.emptyList();
        }

        Path path = Paths.get(input).toAbsolutePath().normalize();
        File file = path.toFile();

        if (!file.exists()) {
            System.out.println(INVALID_PATH);
            return Collections.emptyList();
        }

        if (file.isFile()) {
            if (!isKettleFile(file)) {
                System.out.println(WRONG_EXTENSION);
                return Collections.emptyList();
            }
            return Collections.singletonList(file);
        }

        if (file.isDirectory()) {
            List<File> files = collectKettleFiles(file);
            if (files.isEmpty()) {
                System.out.println(NO_FILES_FOUND);
                return Collections.emptyList();
            }
            return files;
        }

        System.out.println(INVALID_PATH);
        return Collections.emptyList();
    }

    /**
     * Collects all .ktr and .kjb files recursively from a directory.
     */
    public List<File> collectKettleFiles(File directory) {
        List<File> result = new ArrayList<>();
        collectRecursive(directory, result);
        return result;
    }

    private void collectRecursive(File dir, List<File> result) {
        File[] files = dir.listFiles();
        if (files == null) return;

        for (File f : files) {
            if (f.isDirectory()) {
                collectRecursive(f, result);
            } else if (isKettleFile(f)) {
                result.add(f);
            }
        }
    }

    /**
     * Checks if a file has a valid Kettle extension.
     */
    public boolean isKettleFile(File file) {
        String name = file.getName().toLowerCase();
        return name.endsWith(".ktr") || name.endsWith(".kjb");
    }

    /**
     * Prompts for confirmation before proceeding.
     */
    public boolean confirmProceed(int fileCount) {
        String msg = String.format("Found %d file(s). Continue? (Y/N): ", fileCount);
        String response = reader.readLine(msg).trim().toUpperCase();
        return "Y".equals(response) || "YES".equals(response);
    }

    /**
     * Returns the output directory for the report based on input path.
     */
    public File getOutputDirectory(File inputPath) {
        if (inputPath.isFile()) {
            return inputPath.getParentFile();
        }
        return inputPath;
    }
}
