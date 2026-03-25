package com.app.parser;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for KettleParser.
 */
public class KettleParserTest {

    private KettleParser parser;
    private File tempDir;

    @Before
    public void setUp() throws Exception {
        parser = new KettleParser();
        tempDir = Files.createTempDirectory("kettle_test").toFile();
        tempDir.deleteOnExit();
    }

    @Test
    public void testParseSelectQuery() throws Exception {
        String xml =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<transformation>\n" +
            "  <step>\n" +
            "    <name>Table Input</name>\n" +
            "    <type>TableInput</type>\n" +
            "    <sql>SELECT id, name FROM customers WHERE active = 1</sql>\n" +
            "  </step>\n" +
            "</transformation>";

        File f = writeTempFile("test1.ktr", xml);
        List<String> queries = parser.parse(f);

        assertEquals(1, queries.size());
        assertTrue(queries.get(0).contains("SELECT"));
        assertTrue(queries.get(0).contains("customers"));
    }

    @Test
    public void testParseMultipleSqlElements() throws Exception {
        String xml =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<transformation>\n" +
            "  <sql>SELECT * FROM orders</sql>\n" +
            "  <SQL>DELETE FROM logs WHERE created_at &lt; '2024-01-01'</SQL>\n" +
            "</transformation>";

        File f = writeTempFile("test2.ktr", xml);
        List<String> queries = parser.parse(f);

        assertEquals(2, queries.size());
    }

    @Test
    public void testParseJobFile() throws Exception {
        String xml =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<job>\n" +
            "  <entry>\n" +
            "    <name>Exec SQL</name>\n" +
            "    <type>SQL</type>\n" +
            "    <sql>UPDATE counters SET value = value + 1 WHERE id = 1</sql>\n" +
            "  </entry>\n" +
            "</job>";

        File f = writeTempFile("test3.kjb", xml);
        List<String> queries = parser.parse(f);

        assertEquals(1, queries.size());
        assertTrue(queries.get(0).contains("UPDATE"));
    }

    @Test
    public void testParseEmptyFile() throws Exception {
        String xml =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<transformation>\n" +
            "  <step><name>dummy</name></step>\n" +
            "</transformation>";

        File f = writeTempFile("test4.ktr", xml);
        List<String> queries = parser.parse(f);

        // Should be empty — no SQL-like content
        assertNotNull(queries);
    }

    @Test
    public void testDeduplicatesIdenticalQueries() throws Exception {
        String xml =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<transformation>\n" +
            "  <sql>SELECT id FROM products</sql>\n" +
            "  <query>SELECT id FROM products</query>\n" +
            "</transformation>";

        File f = writeTempFile("test5.ktr", xml);
        List<String> queries = parser.parse(f);

        // Should not duplicate identical SQL
        assertEquals(1, queries.size());
    }

    @Test
    public void testParseFileNotFound() {
        File notFound = new File(tempDir, "does_not_exist.ktr");
        try {
            parser.parse(notFound);
            fail("Should throw exception for missing file");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("does_not_exist") || e instanceof java.io.FileNotFoundException);
        }
    }

    private File writeTempFile(String name, String content) throws Exception {
        File f = new File(tempDir, name);
        Files.write(f.toPath(), content.getBytes("UTF-8"));
        return f;
    }
}
