package com.app.cli;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Abstraction over stdin to allow easy testing with mocked input.
 */
public class InteractiveReader {

    private final BufferedReader reader;

    public InteractiveReader() {
        this.reader = new BufferedReader(new InputStreamReader(System.in));
    }

    /**
     * Reads a line of input from the user.
     */
    public String readLine(String prompt) {
        System.out.print(prompt);
        System.out.flush();
        try {
            return reader.readLine();
        } catch (Exception e) {
            return "";
        }
    }
}
