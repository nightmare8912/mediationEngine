package org.prats.fileHanldingUtilities.configParser;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.prats.definitions.Table;
import org.prats.definitions.TableContainer;
import org.prats.definitions.TableField;
import org.prats.loggers.ShellLogger;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class CsvToTableParser {

    private Table table;
    private String csvPath;

    public CsvToTableParser(
            Table table,
            String csvPath
    ) {
        this.table      = table;
        this.csvPath    = csvPath;
    }

    public TableContainer getTableData(
            String csvPath,
            Table table
    ) {
        List<Map<String, String>> tableRecords = new ArrayList<>();
        try (
                Reader reader = Files.newBufferedReader(Path.of(csvPath));
                CSVParser records = CSVFormat.DEFAULT.
                        builder().
                        setHeader().
                        setSkipHeaderRecord(true).
                        get().
                        parse(reader);
            ) {
            try {
                for (CSVRecord csvRecord : records) {

                    Map<String, String> row = new LinkedHashMap<>();

                    for (TableField field : table.fields()) {
                        row.put(field.id(), csvRecord.get(field.id()));
                    }

                }
            } catch (Exception exception) {
                ShellLogger.log("[CsvToTableParser] [loadTableData] Exception occurred while parsing the csv: " + csvPath + " to table: " + table.label() + "\nException Message: " + exception.getMessage());
            }

        } catch(Exception exception) {
            ShellLogger.log("[CsvToTableParser] [loadTableData] Exception occurred while parsing the csv: \" + csvPath + \" to table: \" + table.label() + \"\\nException Message: " + exception.getMessage());
        }

        return new TableContainer(table, tableRecords);
    }
}
