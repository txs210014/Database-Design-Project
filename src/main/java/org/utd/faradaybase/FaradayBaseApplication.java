package org.utd.faradaybase;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.utd.faradaybase.attribute.Attribute;
import org.utd.faradaybase.columninfo.ColumnInformation;
import org.utd.faradaybase.operation.Operation;
import org.utd.faradaybase.pageinfo.Page;
import org.utd.faradaybase.pageinfo.PageType;
import org.utd.faradaybase.tableinfo.TableMetaData;
import org.utd.faradaybase.tableinfo.TableRecord;
import org.utd.faradaybase.trees.BPlusTree;
import org.utd.faradaybase.trees.BTree;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

import static java.lang.System.out;

@Log4j2
public class FaradayBaseApplication {

    static String prompt = "faradaybase> ";
    static String version = "v1.0";
    static String copyright = "Copyright @ Team Faraday";
    static boolean isExit = false;

    static Scanner scanner = new Scanner(System.in).useDelimiter(";");

    // Main method
    public static void main(String[] args) {

        splashScreen();
        initFileSystem();

        String userCommand = "";

        while (!isExit) {
            System.out.print(prompt);
            userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim();
            parseUserCommand(userCommand);
        }
        System.out.println("Exiting...");
    }

    public static void initFileSystem() {
        log.info("Initializing file system!!!");
        File data_directory = new File("data");
        if (!new File(data_directory, FaradayBaseBinaryFile.tables_table + ".tbl").exists()
                || !new File(data_directory, FaradayBaseBinaryFile.columns_table + ".tbl").exists())
            FaradayBaseBinaryFile.createBootstrapFiles();
        else
            FaradayBaseBinaryFile.is_data_store_initialized = true;
    }

    public static void splashScreen() {
        System.out.println(line("-", 80));
        System.out.println("Welcome to Faradaylite Database!");
        displayVersion();
        System.out.println("\nType \"help;\" to display supported commands.");
        System.out.println(line("-", 80));
    }

    /**
     * @param s   The String to be repeated
     * @param num The number of time to repeat String s.
     * @return String A String object, which is the String s appended to itself
     * num times.
     */
    public static String line(String s, int num) {
        String a = "";
        for (int i = 0; i < num; i++) {
            a += s;
        }
        return a;
    }

    public static void printCmd(String s) {
        System.out.println("\n\t" + s + "\n");
    }

    public static void printDef(String s) {
        System.out.println("\t\t" + s);
    }

    public static void help() {
        out.println(line("*", 80));
        out.println("SUPPORTED COMMANDS\n");
        out.println("All commands below are case insensitive\n");

        out.println("SHOW TABLES;");
        out.println("\tDisplay the names of all tables.\n");

        out.println("CREATE TABLE <table_name> (<column_name> <data_type> <not_null> <unique>);");
        out.println("\tCreates a table with the given columns.\n");

        out.println("DROP TABLE <table_name>;");
        out.println("\tRemove table data (i.e. all records) and its schema.\n");

        out.println("UPDATE TABLE <table_name> SET <column_name> = <value> [WHERE <condition>];");
        out.println("\tModify records data whose optional <condition>");
        out.println("\tis <column_name> = <value>.\n");

        out.println("INSERT INTO <table_name> (<column_list>) VALUES (<values_list>);");
        out.println("\tInserts a new record into the table with the given values for the given columns.\n");

        out.println("SELECT <column_list> FROM <table_name> [WHERE <condition>];");
        out.println("\tDisplay table records whose optional <condition>");
        out.println("\tis <column_name> = <value>.\n");

        out.println("VERSION;");
        out.println("\tDisplay the program version.\n");

        out.println("HELP;");
        out.println("\tDisplay this help information.\n");

        out.println("EXIT;");
        out.println("\tExit the program.\n");

        out.println(line("*", 80));
    }

    public static String getVersion() {
        return version;
    }

    public static String getCopyright() {
        return copyright;
    }

    public static void displayVersion() {
        System.out.println("Database Version " + getVersion());
        System.out.println(getCopyright());
    }

    public static void parseUserCommand(String userCommand) {

        ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));

        switch (commandTokens.get(0).toLowerCase()) {
            case "show":
                if (commandTokens.get(1).equalsIgnoreCase("tables"))
                    parseUserCommand("select * from faradaybase_tables");
                else if (commandTokens.get(1).equalsIgnoreCase("rowid")) {
                    FaradayBaseBinaryFile.showing_rowid = true;
                    System.out.println("* Table will now display the contents of RowId.");
                } else
                    System.out.println("! I didn't understand the command: \"" + userCommand + "\"");
                break;
            case "select":
                parse_query(userCommand);
                break;
            case "drop":
                drop_table(userCommand);
                break;
            case "create":
                if (commandTokens.get(1).equalsIgnoreCase("table"))
                    parse_create_table(userCommand);
                else if (commandTokens.get(1).equalsIgnoreCase("index"))
                    parseCreateIndex(userCommand);
                break;
            case "update":
                parse_update(userCommand);
                break;
            case "insert":
                parse_insert(userCommand);
                break;
            case "delete":
                parse_delete(userCommand);
                break;
            case "help":
                help();
                break;
            case "version":
                displayVersion();
                break;
            case "exit":
                isExit = true;
                break;
            case "quit":
                isExit = true;
                break;
            default:
                System.out.println("! User command not recognized : \"" + userCommand + "\"");
                break;
        }
    }

    public static void parseCreateIndex(String createIndexString) {
        ArrayList<String> create_index_tokens = new ArrayList<String>(Arrays.asList(createIndexString.split(" ")));
        try {
            if (!create_index_tokens.get(2).equalsIgnoreCase("on") || !createIndexString.contains("(")
                    || !createIndexString.contains(")") && create_index_tokens.size() < 4) {
                System.out.println("Incorrect Syntax");
                return;
            }

            String table_name = createIndexString.split("(?i)on")[1].split("\\(")[0].trim();
            String column_name = createIndexString.split("(?i)on")[1].split("[\\(||//)]")[1].trim();

            if (new File(FaradayBaseApplication.getNDXFilePath(table_name, column_name)).exists()) {
                System.out.println("Index already there");
                return;
            }

            RandomAccessFile table_file = new RandomAccessFile(getTableFilePath(table_name), "rw");

            TableMetaData meta_data = new TableMetaData(table_name);

            if (!meta_data.isTableExisting()) {
                System.out.println("Incorrect Table name");
                table_file.close();
                return;
            }

            int column_ordinal = meta_data.getColumnNames().indexOf(column_name);

            if (column_ordinal < 0) {
                System.out.println("Incorrect column name");
                table_file.close();
                return;
            }

            RandomAccessFile index_file = new RandomAccessFile(getNDXFilePath(table_name, column_name), "rw");
            Page.add_new_pg(index_file, PageType.LEAFINDEX, -1, -1);
            if (meta_data.getRecordCount() > 0) {
                BPlusTree bPlusOneTree = new BPlusTree(table_file, meta_data.getRootPageNumber(), meta_data.getTableName());
                for (int pageNo : bPlusOneTree.get_allLeaves()) {
                    Page page = new Page(table_file, pageNo);
                    BTree bTree = new BTree(index_file);
                    for (TableRecord record : page.getPgRecords()) {
                        bTree.insert(record.getAttributes().get(column_ordinal), record.getRow_id());
                    }
                }
            }

            System.out.println("Index created on the column : " + column_name);
            index_file.close();
            table_file.close();

        } catch (IOException e) {

            System.err.println("ERROR: Unable to create Index");
            System.out.println(e);
        }

    }

    public static void drop_table(String dropTableString) {
        System.out.println("STUB: This is the drop_table method.");
        System.out.println("\tParsing the string:\"" + dropTableString + "\"");

        String[] tokens = dropTableString.split(" ");
        if (!(tokens[0].trim().equalsIgnoreCase("DROP") && tokens[1].trim().equalsIgnoreCase("TABLE"))) {
            System.out.println("Error");
            return;
        }

        ArrayList<String> drop_table_tokens = new ArrayList<String>(Arrays.asList(dropTableString.split(" ")));
        String table_name = drop_table_tokens.get(2);

        parse_delete(
                "delete from table " + FaradayBaseBinaryFile.tables_table + " where table_name = '" + table_name + "' ");
        parse_delete(
                "delete from table " + FaradayBaseBinaryFile.columns_table + " where table_name = '" + table_name + "' ");
        File table_file = new File("data/" + table_name + ".tbl");
        if (table_file.delete()) {
            System.out.println("table dropped");
        } else
            System.out.println("table not found");

        File f = new File("data/");
        File[] matchingFiles = f.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith(table_name) && name.endsWith("ndx");
            }
        });
        boolean iFlag = false;
        for (File file : matchingFiles) {
            if (file.delete()) {
                iFlag = true;
                System.out.println("index deleted");
            }
        }
        if (iFlag)
            System.out.println("drop " + table_name);
        else
            System.out.println("index not found");

    }

    public static void parse_query(String queryString) {
        String table_name = "";
        List<String> column_names = new ArrayList<String>();

        ArrayList<String> query_table_tokens = new ArrayList<>(Arrays.asList(queryString.split(" ")));
        int i = 0;

        for (i = 1; i < query_table_tokens.size(); i++) {
            if (query_table_tokens.get(i).equalsIgnoreCase("from")) {
                ++i;
                table_name = query_table_tokens.get(i);
                break;
            }
            if (!query_table_tokens.get(i).equalsIgnoreCase("*") && !query_table_tokens.get(i).equalsIgnoreCase(",")) {
                if (query_table_tokens.get(i).contains(",")) {
                    ArrayList<String> colList = new ArrayList<String>(
                            Arrays.asList(query_table_tokens.get(i).split(",")));
                    for (String col : colList) {
                        column_names.add(col.trim());
                    }
                } else
                    column_names.add(query_table_tokens.get(i));
            }
        }

        TableMetaData tableMetaData = new TableMetaData(table_name);
        if (!tableMetaData.isTableExisting()) {
            System.out.println("Table does not exists");
            return;
        }

        Operation operation = null;
        try {

            operation = condition_extraction_from_query(tableMetaData, queryString);

        } catch (Exception e) {
            log.error(e.getMessage());
            log.debug("ERROR:", e);
            return;
        }

        if (column_names.size() == 0) {
            column_names = tableMetaData.getColumnNames();
        }
        try {

            RandomAccessFile table_file = new RandomAccessFile(getTableFilePath(table_name), "r");
            FaradayBaseBinaryFile tableBinaryFile = new FaradayBaseBinaryFile(table_file);
            tableBinaryFile.selectRecords(tableMetaData, column_names, operation);
            table_file.close();
        } catch (IOException exception) {
            System.err.println("ERROR: Unable to select fields from table");
        }

    }

    public static void parse_update(String updateString) {
        ArrayList<String> update_tokens = new ArrayList<String>(Arrays.asList(updateString.split(" ")));

        String table_name = update_tokens.get(1);
        List<String> cols_to_update = new ArrayList<>();
        List<String> val_to_update = new ArrayList<>();

        if (!update_tokens.get(2).equalsIgnoreCase("set") || !update_tokens.contains("=")) {
            System.out.println("! Syntax error !");
            System.out.println(
                    "Expected Syntax: UPDATE [table_name] SET [Column_name] = val1 where [column_name] = val2;");
            return;
        }

        String update_col_info_string = updateString.split("(?i)set")[1].split("(?i)where")[0];

        List<String> column_newValueSet = Arrays.asList(update_col_info_string.split(","));

        for (String item : column_newValueSet) {
            cols_to_update.add(item.split("=")[0].trim());
            val_to_update.add(item.split("=")[1].trim().replace("\"", "").replace("'", ""));
        }

        TableMetaData metadata = new TableMetaData(table_name);

        if (!metadata.isTableExisting()) {
            System.out.println("Invalid Table name");
            return;
        }

        if (!metadata.is_column_existing(cols_to_update)) {
            System.out.println("Invalid column name(s)");
            return;
        }

        Operation operation = null;
        try {

            operation = condition_extraction_from_query(metadata, updateString);

        } catch (Exception e) {
            log.debug("ERROR:", e);
            log.error(e.getMessage());
            return;

        }

        try {
            RandomAccessFile file = new RandomAccessFile(getTableFilePath(table_name), "rw");
            FaradayBaseBinaryFile binaryFile = new FaradayBaseBinaryFile(file);
            int noOfRecordsupdated = binaryFile.update_records_operation(metadata, operation, cols_to_update,
                    val_to_update);
            if (noOfRecordsupdated > 0) {
                List<Integer> allRowids = new ArrayList<>();
                for (ColumnInformation colInfo : metadata.getColumnNameAttributes()) {
                    for (int i = 0; i < cols_to_update.size(); i++)
                        if (colInfo.getColName().equalsIgnoreCase(cols_to_update.get(i)) && colInfo.isHasIndex()) {
                            if (operation == null) {
                                if (allRowids.size() == 0) {
                                    BPlusTree bPlusOneTree = new BPlusTree(file, metadata.getRootPageNumber(),
                                            metadata.getTableName());
                                    for (int pageNo : bPlusOneTree.get_allLeaves()) {
                                        Page currentPage = new Page(file, pageNo);
                                        for (TableRecord record : currentPage.getPgRecords()) {
                                            allRowids.add(record.getRow_id());
                                        }
                                    }
                                }
                                RandomAccessFile indexFile = new RandomAccessFile(
                                        getNDXFilePath(table_name, cols_to_update.get(i)), "rw");
                                Page.add_new_pg(indexFile, PageType.LEAFINDEX, -1, -1);
                                BTree bTree = new BTree(indexFile);
                                bTree.insert(new Attribute(colInfo.getDataType(), val_to_update.get(i)), allRowids);
                            }
                        }
                }
            }

            file.close();

        } catch (Exception e) {
            log.error("ERROR: Unable to update the " + table_name + " file");
            log.debug("ERROR:", e);
        }

    }

    public static void parse_insert(String queryString) {
        ArrayList<String> insertTokens = new ArrayList<String>(Arrays.asList(queryString.split(" ")));

        if (!insertTokens.get(1).equalsIgnoreCase("into") || !StringUtils.containsIgnoreCase(queryString, ") values")) {
            System.out.println("! Syntax error !");
            System.out.println("Expected Syntax: INSERT INTO table_name ( columns ) VALUES ( values );");
            return;
        }

        try {
            String table_name = insertTokens.get(2);
            if (table_name.trim().length() == 0) {
                System.out.println("Tablename cannot be empty !");
                return;
            }

            // parsing logic
            if (table_name.indexOf("(") > -1) {
                table_name = table_name.substring(0, table_name.indexOf("("));
            }
            TableMetaData dstMetaData = new TableMetaData(table_name);

            if (!dstMetaData.isTableExisting()) {
                System.out.println("! Table not exist. !");
                return;
            }

            ArrayList<String> columnTokens = new ArrayList<String>(Arrays.asList(
                    queryString.substring(queryString.indexOf("(") + 1,
                            queryString.toLowerCase().indexOf(") values")).split(",")));

            // Column List validation
            for (String colToken : columnTokens) {
                if (!dstMetaData.getColumnNames().contains(colToken.trim())) {
                    System.out.println("! Invalid column : !" + colToken.trim());
                    return;
                }
            }

            String valuesString = queryString.substring(queryString.lastIndexOf("("));
            ArrayList<String> valueTokens = (ArrayList<String>) Arrays.stream(valuesString.replace("(", "")
                            .replace(")", "")
                            .split(","))
                    .map(s -> s.trim())
                    .collect(Collectors.toList());

            // fill attributes to insert
            List<Attribute> attributeToInsert = new ArrayList<>();

            for (ColumnInformation colInfo : dstMetaData.getColumnNameAttributes()) {
                int i = 0;
                boolean columnProvided = false;
                for (i = 0; i < columnTokens.size(); i++) {
                    if (columnTokens.get(i).trim().equalsIgnoreCase(colInfo.getColName())) {
                        columnProvided = true;
                        try {
                            String value = valueTokens.get(i).replace("'", "").replace("\"", "").trim();
                            if (valueTokens.get(i).trim().equalsIgnoreCase("null")) {
                                if (!colInfo.isNullCol()) {
                                    System.out.println("! Cannot Insert NULL into !" + colInfo.getColName());
                                    return;
                                }
                                colInfo.setDataType(DataType.NULL);
                                value = value.toUpperCase();
                            }
                            Attribute attr = new Attribute(colInfo.getDataType(), value);
                            attributeToInsert.add(attr);
                            break;
                        } catch (Exception e) {
                            log.error("Incorrect data format for " + columnTokens.get(i) + " values: "
                                    + valueTokens.get(i));
                            log.debug("ERROR:", e);
                            return;
                        }
                    }
                }
                if (columnTokens.size() > i) {
                    columnTokens.remove(i);
                    valueTokens.remove(i);
                }

                if (!columnProvided) {
                    if (colInfo.isNullCol())
                        attributeToInsert.add(new Attribute(DataType.NULL, "NULL"));
                    else {
                        System.err.println("ERROR: Unable to Insert NULL into " + colInfo.getColName());
                        return;
                    }
                }
            }

            // insert attributes to the page
            RandomAccessFile dstTable = new RandomAccessFile(getTableFilePath(table_name), "rw");
            int dstPageNo = BPlusTree.get_pagenum_for_insertion(dstTable, dstMetaData.getRootPageNumber());
            Page dstPage = new Page(dstTable, dstPageNo);

            int rowNo = dstPage.add_table_row(table_name, attributeToInsert);

            // update Index
            if (rowNo != -1) {

                for (int i = 0; i < dstMetaData.getColumnNameAttributes().size(); i++) {
                    ColumnInformation col = dstMetaData.getColumnNameAttributes().get(i);

                    if (col.isHasIndex()) {
                        RandomAccessFile indexFile = new RandomAccessFile(getNDXFilePath(table_name, col.getColName()),
                                "rw");
                        BTree bTree = new BTree(indexFile);
                        bTree.insert(attributeToInsert.get(i), rowNo);
                    }

                }
            }

            dstTable.close();
            if (rowNo != -1)
                System.out.println("Record Inserted into table");
            System.out.println();

        } catch (Exception ex) {
            System.err.println("ERROR: Unable to insert the record in table");
            ex.printStackTrace();
        }
    }

    /**
     * Create new table
     *
     * @param createTableString is a String of the user input
     */
    public static void parse_create_table(String createTableString) {

        ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(createTableString.split(" ")));
        // table and () check
        if (!createTableTokens.get(1).equalsIgnoreCase("table")) {
            System.out.println("Syntax Error");
            return;
        }
        String table_name = createTableTokens.get(2);
        if (table_name.trim().length() == 0) {
            System.out.println("Tablename cannot be empty");
            return;
        }
        try {

            if (table_name.indexOf("(") > -1) {
                table_name = table_name.substring(0, table_name.indexOf("("));
            }

            List<ColumnInformation> lstcolumnInformation = new ArrayList<>();
            ArrayList<String> columnTokens = new ArrayList<String>(Arrays.asList(createTableString
                    .substring(createTableString.indexOf("(") + 1, createTableString.lastIndexOf(")")).split(",")));

            short ordinal_pos = 1;

            String primary_key_col = "";

            for (String columnToken : columnTokens) {

                ArrayList<String> colInfoToken = new ArrayList<String>(Arrays.asList(columnToken.trim().split(" ")));
                ColumnInformation colInfo = new ColumnInformation();
                colInfo.setTableName(table_name);
                colInfo.setColName(colInfoToken.get(0));
                colInfo.setNullCol(true);
                colInfo.setDataType(DataType.get(colInfoToken.get(1).toUpperCase()));
                for (int i = 0; i < colInfoToken.size(); i++) {

                    if ((colInfoToken.get(i).equalsIgnoreCase("null"))) {
                        colInfo.setNullCol(true);
                    }
                    if (colInfoToken.get(i).equalsIgnoreCase("not") && (colInfoToken.get(i + 1).equalsIgnoreCase("null"))) {
                        colInfo.setNullCol(false);
                        i++;
                    }

                    if ((colInfoToken.get(i).equalsIgnoreCase("unique"))) {
                        colInfo.setUniqueCol(true);
                    } else if (colInfoToken.get(i).equalsIgnoreCase("primary") && (colInfoToken.get(i + 1).equalsIgnoreCase("key"))) {
                        colInfo.setKeyPrimary(true);
                        colInfo.setUniqueCol(true);
                        colInfo.setUniqueCol(false);
                        primary_key_col = colInfo.getColName();
                        i++;
                    }

                }
                colInfo.setOrdinalPos(ordinal_pos++);
                lstcolumnInformation.add(colInfo);

            }

            // update sys file
            RandomAccessFile faradaybase_catalog_of_tables = new RandomAccessFile(
                    getTableFilePath(FaradayBaseBinaryFile.tables_table), "rw");
            TableMetaData faradaybaseTableMetaData = new TableMetaData(FaradayBaseBinaryFile.tables_table);

            int pageNo = BPlusTree.get_pagenum_for_insertion(faradaybase_catalog_of_tables,
                    faradaybaseTableMetaData.getRootPageNumber());

            Page page = new Page(faradaybase_catalog_of_tables, pageNo);

            int rowNo = page.add_table_row(FaradayBaseBinaryFile.tables_table,
                    Arrays.asList(new Attribute[]{new Attribute(DataType.TEXT, table_name),
                            new Attribute(DataType.INT, "0"), new Attribute(DataType.SMALLINT, "0"),
                            new Attribute(DataType.SMALLINT, "0")}));
            faradaybase_catalog_of_tables.close();

            if (rowNo == -1) {
                System.out.println("table Name conflict");
                return;
            }
            RandomAccessFile table_file = new RandomAccessFile(getTableFilePath(table_name), "rw");
            Page.add_new_pg(table_file, PageType.LEAF, -1, -1);
            table_file.close();

            RandomAccessFile faradaybaseColumnsCatalog = new RandomAccessFile(
                    getTableFilePath(FaradayBaseBinaryFile.columns_table), "rw");
            TableMetaData faradaybaseColumnsMetaData = new TableMetaData(FaradayBaseBinaryFile.columns_table);
            pageNo = BPlusTree.get_pagenum_for_insertion(faradaybaseColumnsCatalog,
                    faradaybaseColumnsMetaData.getRootPageNumber());

            Page page1 = new Page(faradaybaseColumnsCatalog, pageNo);

            for (ColumnInformation column : lstcolumnInformation) {
                page1.add_new_col(column);
            }

            faradaybaseColumnsCatalog.close();

            System.out.println("* Table created *");

            if (primary_key_col.length() > 0) {
                parseCreateIndex("create index on " + table_name + "(" + primary_key_col + ")");
            }
        } catch (Exception e) {

            log.error("ERROR: Unable to create Table:" + e.getMessage());
            log.debug("ERROR:", e);
            parse_delete("delete from table " + FaradayBaseBinaryFile.tables_table + " where table_name = '" + table_name
                    + "' ");
            parse_delete("delete from table " + FaradayBaseBinaryFile.columns_table + " where table_name = '" + table_name
                    + "' ");
        }

    }

    private static void parse_delete(String deleteTableString) {
        ArrayList<String> del_table_tokens = new ArrayList<String>(Arrays.asList(deleteTableString.split(" ")));

        String table_name = "";

        try {

            if (!del_table_tokens.get(1).equalsIgnoreCase("from") || !del_table_tokens.get(2).equalsIgnoreCase("table")) {
                System.out.println("Syntax Error");
                return;
            }

            table_name = del_table_tokens.get(3);

            TableMetaData meta_data = new TableMetaData(table_name);
            Operation operation = null;
            try {
                operation = condition_extraction_from_query(meta_data, deleteTableString);

            } catch (Exception e) {
                log.error(e.getMessage());
                log.debug("ERROR:", e);
                return;
            }
            RandomAccessFile table_file = new RandomAccessFile(getTableFilePath(table_name), "rw");

            BPlusTree tree = new BPlusTree(table_file, meta_data.getRootPageNumber(), meta_data.getTableName());
            List<TableRecord> deletedRecords = new ArrayList<TableRecord>();
            int count = 0;
            for (int pageNo : tree.get_allLeaves(operation)) {
                short deleteCountPerPage = 0;
                Page page = new Page(table_file, pageNo);
                for (TableRecord record : page.getPgRecords()) {
                    if (operation != null) {
                        if (!operation
                                .condition_check(record.getAttributes().get(operation.getColumnOrdinal()).getFieldValue()))
                            continue;
                    }

                    deletedRecords.add(record);
                    page.del_table_record(table_name,
                            Integer.valueOf(record.getPg_head_index() - deleteCountPerPage).shortValue());
                    deleteCountPerPage++;
                    count++;
                }
            }

            if (operation == null) {

            } else {
                for (int i = 0; i < meta_data.getColumnNameAttributes().size(); i++) {
                    if (meta_data.getColumnNameAttributes().get(i).isHasIndex()) {
                        RandomAccessFile indexFile = new RandomAccessFile(
                                getNDXFilePath(table_name, meta_data.getColumnNameAttributes().get(i).getColName()), "rw");
                        BTree bTree = new BTree(indexFile);
                        for (TableRecord record : deletedRecords) {
                            bTree.delete(record.getAttributes().get(i), record.getRow_id());
                        }
                    }
                }
            }

            System.out.println();
            table_file.close();
            System.out.println(count + " record(s) deleted!");

        } catch (Exception e) {
            log.error("! Error on dropping rows in table : {} {}", table_name, e.getMessage());
            log.debug("ERROR:", e);
        }

    }

    public static String getTableFilePath(String table_name) {
        return "data/" + table_name + ".tbl";
    }

    public static String getNDXFilePath(String table_name, String columnName) {
        return "data/" + table_name + "_" + columnName + ".ndx";
    }

    private static Operation condition_extraction_from_query(TableMetaData tableMetaData, String query)
            throws Exception {
        if (StringUtils.containsIgnoreCase(query, "where")) {
            Operation operation = new Operation(DataType.TEXT);
            String where_clause = query.split("(?i)where")[1].trim();
            ArrayList<String> where_keyword_tokens = new ArrayList<>(Arrays.asList(where_clause.split(" ")));

            // WHERE NOT column operator value
            if (where_keyword_tokens.get(0).equalsIgnoreCase("not")) {
                operation.setNegation(true);
            }

            for (int i = 0; i < Operation.supportedOperators.length; i++) {
                if (where_clause.contains(Operation.supportedOperators[i])) {
                    where_keyword_tokens = new ArrayList<>(
                            Arrays.asList(where_clause.split(Operation.supportedOperators[i])));
                    {
                        operation.setOperator(Operation.supportedOperators[i]);
                        operation.setConditionValue(where_keyword_tokens.get(1).trim());
                        operation.setColumName(where_keyword_tokens.get(0).trim());
                        break;
                    }

                }
            }
            if (tableMetaData.isTableExisting()
                    && tableMetaData.is_column_existing(new ArrayList<>(Arrays.asList(operation.getColumnName())))) {
                operation.setColumnOrdinal(tableMetaData.getColumnNames().indexOf(operation.getColumnName()));
                operation.setDataType(tableMetaData.getColumnNameAttributes().get(operation.getColumnOrdinal()).getDataType());
            } else {
                throw new Exception(
                        "! Invalid Table/Column : " + tableMetaData.getTableName() + " . " + operation.getColumnName());
            }
            return operation;
        } else
            return null;
    }

}