package com.app.parser;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses .ktr (Pentaho Transformation) and .kjb (Pentaho Job) XML files
 * and extracts raw SQL query strings.
 *
 * SQL queries are typically found in:
 * - XML elements with names like "sql", "SQL", "query", "stepexecutioninformation"
 * - Elements under step types such as TableInput, TableOutput, ExecuteSQLJobEntry
 * - CDATA sections within SQL-related fields
 */
public class KettleParser {

    private static final String[] SQL_ELEMENT_NAMES = {
        "sql", "SQL", "query", "Query", "sqlString", "SqlString",
        "execSql", "ExecSql", "sqlScript", "SqlScript", "statement"
    };

    private static final String[] SQL_STEP_TYPES = {
        "TableInput", "TableOutput", "Update", "InsertUpdate",
        "Delete", "ExecuteSQLJobEntry", "Sql", "Exec"
    };

    /**
     * Parses a .ktr or .kjb file and extracts all SQL query strings.
     *
     * @param file the XML file to parse
     * @return list of raw SQL strings found in the file
     */
    public List<String> parse(File file) throws Exception {
        List<String> queries = new ArrayList<>();

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        // Disable external DTD loading for security and performance
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);

        DocumentBuilder builder = factory.newDocumentBuilder();

        try (FileInputStream fis = new FileInputStream(file)) {
            Document doc = builder.parse(fis);
            doc.getDocumentElement().normalize();

            // Strategy 1: Find elements by known SQL field names
            for (String elemName : SQL_ELEMENT_NAMES) {
                queries.addAll(findSqlInElements(doc, elemName));
            }

            // Strategy 2: Find SQL within step entries by type
            queries.addAll(findSqlInSteps(doc));

            // Strategy 3: Find SQL within job entries
            queries.addAll(findSqlInJobEntries(doc));

            // Strategy 4: Scan all text nodes for SQL keywords as fallback
            queries.addAll(findSqlInAllNodes(doc));
        }

        // Deduplicate while preserving order
        return deduplicate(queries);
    }

    /**
     * Finds text content of elements matching a given name (case-insensitive).
     */
    private List<String> findSqlInElements(Document doc, String elemName) {
        List<String> results = new ArrayList<>();
        NodeList allNodes = doc.getElementsByTagName("*");

        for (int i = 0; i < allNodes.getLength(); i++) {
            Node node = allNodes.item(i);
            if (node.getNodeType() != Node.ELEMENT_NODE) continue;

            Element elem = (Element) node;
            if (!elem.getNodeName().equalsIgnoreCase(elemName)) continue;

            String sql = extractSqlFromElement(elem);
            if (isValidSql(sql)) {
                results.add(sql);
            }
        }
        return results;
    }

    /**
     * Finds SQL in step elements (for .ktr files).
     */
    private List<String> findSqlInSteps(Document doc) {
        List<String> results = new ArrayList<>();
        NodeList allNodes = doc.getElementsByTagName("*");

        for (int i = 0; i < allNodes.getLength(); i++) {
            Node node = allNodes.item(i);
            if (node.getNodeType() != Node.ELEMENT_NODE) continue;

            Element elem = (Element) node;
            String nodeName = elem.getNodeName();

            // Check if this is a step definition element
            if (nodeName.equalsIgnoreCase("step")
                    || nodeName.equalsIgnoreCase("entry")
                    || nodeName.equalsIgnoreCase("transformation")
                    || nodeName.equalsIgnoreCase("job")) {
                results.addAll(extractSqlFromStepElement(elem));
            }

            // Also check child elements of steps for SQL-like content
            NodeList children = elem.getChildNodes();
            for (int j = 0; j < children.getLength(); j++) {
                Node child = children.item(j);
                if (child.getNodeType() != Node.ELEMENT_NODE) continue;
                Element childElem = (Element) child;

                // Check step type attributes
                String type = getChildText(childElem, "type");
                if (type != null && isSqlStepType(type)) {
                    String sql = getChildText(childElem, "sql");
                    if (isValidSql(sql)) results.add(sql);

                    String query = getChildText(childElem, "query");
                    if (isValidSql(query)) results.add(query);
                }
            }
        }
        return results;
    }

    /**
     * Finds SQL in job entry elements (for .kjb files).
     */
    private List<String> findSqlInJobEntries(Document doc) {
        List<String> results = new ArrayList<>();
        NodeList entries = doc.getElementsByTagName("entry");

        for (int i = 0; i < entries.getLength(); i++) {
            Node node = entries.item(i);
            if (node.getNodeType() != Node.ELEMENT_NODE) continue;

            Element entry = (Element) node;
            Element[] sqlFields = {
                getDirectChild(entry, "sql"),
                getDirectChild(entry, "SQL"),
                getDirectChild(entry, "sqlString"),
                getDirectChild(entry, "sqlScript"),
                getDirectChild(entry, "query")
            };

            for (Element field : sqlFields) {
                if (field != null) {
                    String sql = getElementText(field);
                    if (isValidSql(sql)) {
                        results.add(sql);
                    }
                }
            }
        }
        return results;
    }

    /**
     * Fallback: scans all text nodes for SQL-like content.
     */
    private List<String> findSqlInAllNodes(Document doc) {
        List<String> results = new ArrayList<>();
        NodeList allNodes = doc.getDocumentElement().getChildNodes();

        for (int i = 0; i < allNodes.getLength(); i++) {
            Node node = allNodes.item(i);
            if (node.getNodeType() == Node.TEXT_NODE) {
                String text = node.getTextContent();
                if (looksLikeSql(text)) {
                    results.add(text.trim());
                }
            }
        }
        return results;
    }

    /**
     * Extracts SQL from a step or entry element by looking at common child fields.
     */
    private List<String> extractSqlFromStepElement(Element step) {
        List<String> results = new ArrayList<>();
        NodeList children = step.getChildNodes();

        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            if (child.getNodeType() != Node.ELEMENT_NODE) continue;

            Element childElem = (Element) child;
            String name = childElem.getNodeName().toLowerCase();

            if (name.equals("sql") || name.equals("query")
                    || name.equals("sqlstring") || name.equals("sqlscript")
                    || name.equals("statement") || name.equals("execsql")) {
                String sql = getElementText(childElem);
                if (isValidSql(sql)) {
                    results.add(sql);
                }
            }
        }
        return results;
    }

    /**
     * Extracts SQL text from an element, handling CDATA sections.
     */
    private String extractSqlFromElement(Element elem) {
        NodeList children = elem.getChildNodes();
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            if (child.getNodeType() == Node.CDATA_SECTION_NODE) {
                sb.append(child.getNodeValue());
            } else if (child.getNodeType() == Node.TEXT_NODE) {
                sb.append(child.getTextContent());
            }
        }

        return sb.length() > 0 ? sb.toString().trim() : elem.getTextContent().trim();
    }

    private String getElementText(Element elem) {
        StringBuilder sb = new StringBuilder();
        NodeList children = elem.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            if (child.getNodeType() == Node.CDATA_SECTION_NODE) {
                sb.append(child.getNodeValue());
            } else if (child.getNodeType() == Node.TEXT_NODE) {
                sb.append(child.getTextContent());
            }
        }
        return sb.length() > 0 ? sb.toString().trim() : elem.getTextContent().trim();
    }

    /**
     * Gets text content of a direct child element by name.
     */
    private String getChildText(Element parent, String childName) {
        Element child = getDirectChild(parent, childName);
        return child != null ? getElementText(child) : null;
    }

    /**
     * Gets a direct child element by name.
     */
    private Element getDirectChild(Element parent, String name) {
        NodeList children = parent.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            if (child.getNodeType() == Node.ELEMENT_NODE
                    && child.getNodeName().equals(name)) {
                return (Element) child;
            }
        }
        return null;
    }

    private boolean isSqlStepType(String type) {
        String t = type.toLowerCase();
        for (String sqlType : SQL_STEP_TYPES) {
            if (t.contains(sqlType.toLowerCase())) return true;
        }
        return false;
    }

    /**
     * Checks if a string looks like valid SQL (has keywords).
     */
    private boolean isValidSql(String sql) {
        if (sql == null || sql.trim().length() < 8) return false;
        String s = sql.toUpperCase();
        return s.contains("SELECT") || s.contains("INSERT")
                || s.contains("UPDATE") || s.contains("DELETE")
                || s.contains("CREATE ") || s.contains("ALTER ")
                || s.contains("DROP ") || s.contains("WITH ");
    }

    /**
     * Checks if text content looks like SQL (heuristic).
     */
    private boolean looksLikeSql(String text) {
        if (text == null) return false;
        String trimmed = text.trim();
        if (trimmed.length() < 10) return false;

        String upper = trimmed.toUpperCase();
        // Must have multiple SQL keywords or look like a SELECT
        int keywordCount = 0;
        for (String kw : new String[]{"SELECT", "FROM", "WHERE", "JOIN", "INSERT",
                "UPDATE", "DELETE", "ORDER BY", "GROUP BY", "SET ", "INTO"}) {
            if (upper.contains(kw)) keywordCount++;
        }
        return keywordCount >= 2;
    }

    /**
     * Deduplicates a list while preserving order.
     */
    private List<String> deduplicate(List<String> list) {
        List<String> result = new ArrayList<>();
        for (String s : list) {
            if (!result.contains(s)) {
                result.add(s);
            }
        }
        return result;
    }
}
