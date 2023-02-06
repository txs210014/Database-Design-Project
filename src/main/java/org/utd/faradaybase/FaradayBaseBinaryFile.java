package org.utd.faradaybase;

import lombok.extern.log4j.Log4j2;
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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

import static java.lang.System.out;

@Log4j2
public class FaradayBaseBinaryFile {

    public static String columns_table = "faradaybase_columns";
    public static String tables_table = "faradaybase_tables";
    public static boolean showing_rowid = false;
    public static boolean is_data_store_initialized = false;

    public static int power_of_page_size = 9;
    public static int page_size = (int) Math.pow(2, power_of_page_size);

    RandomAccessFile file;

    public FaradayBaseBinaryFile(RandomAccessFile file) {
        this.file = file;
    }

    public boolean doesRecordExists(TableMetaData tablemetaData, List<String> columNames, Operation operation)
            throws IOException {

        BPlusTree bPlusOneTree = new BPlusTree(file, tablemetaData.getRootPageNumber(), tablemetaData.getTableName());

        for (Integer page_number : bPlusOneTree.get_allLeaves(operation)) {
            Page page = new Page(file, page_number);
            for (TableRecord record : page.getPgRecords()) {
                if (operation != null) {
                    if (!operation.condition_check(record.getAttributes().get(operation.getColumnOrdinal()).getFieldValue()))
                        continue;
                }
                return true;
            }
        }
        return false;
    }

    public int update_records_operation(TableMetaData tablemetaData, Operation operation, List<String> columNames,
                                        List<String> newValues) throws IOException {
        int count = 0;
        List<Integer> ordinal_postions = tablemetaData.get_ordinal_postions(columNames);
        int k = 0;
        Map<Integer, Attribute> newValueMap = new HashMap<>();
        for (String strnewValue : newValues) {
            int index = ordinal_postions.get(k);
            try {
                newValueMap.put(index,
                        new Attribute(tablemetaData.getColumnNameAttributes().get(index).getDataType(), strnewValue));
            } catch (Exception e) {
                log.error("Unsupported data format" + tablemetaData.getColumnNames().get(index) + " values: "
                        + strnewValue);
                return count;
            }
            k++;
        }
        BPlusTree bPlusOneTree = new BPlusTree(file, tablemetaData.getRootPageNumber(), tablemetaData.getTableName());
        for (Integer page_number : bPlusOneTree.get_allLeaves(operation)) {
            short delete_count_per_page = 0;
            Page page = new Page(file, page_number);
            for (TableRecord record : page.getPgRecords()) {
                if (operation != null) {
                    if (!operation.condition_check(record.getAttributes().get(operation.getColumnOrdinal()).getFieldValue()))
                        continue;
                }
                count++;
                for (int i : newValueMap.keySet()) {
                    Attribute oldValue = record.getAttributes().get(i);
                    int row_id = record.getRow_id();
                    if ((record.getAttributes().get(i).getDataType() == DataType.TEXT
                            && record.getAttributes().get(i).getFieldValue().length() == newValueMap.get(i).getFieldValue()
                            .length())
                            || (record.getAttributes().get(i).getDataType() != DataType.NULL
                            && record.getAttributes().get(i).getDataType() != DataType.TEXT)) {
                        page.update_record(record, i, newValueMap.get(i).getFieldValueByteObject());
                    } else {
                        page.del_table_record(tablemetaData.getTableName(),
                                Integer.valueOf(record.getPg_head_index() - delete_count_per_page).shortValue());
                        delete_count_per_page++;
                        List<Attribute> attrs = record.getAttributes();
                        Attribute attr = attrs.get(i);
                        attrs.remove(i);
                        attr = newValueMap.get(i);
                        attrs.add(i, attr);
                        row_id = page.add_table_row(tablemetaData.getTableName(), attrs);
                    }
                    if (tablemetaData.getColumnNameAttributes().get(i).isHasIndex() && operation != null) {
                        RandomAccessFile indexFile = new RandomAccessFile(
                                FaradayBaseApplication.getNDXFilePath(tablemetaData.getColumnNameAttributes().get(i).getTableName(),
                                        tablemetaData.getColumnNameAttributes().get(i).getColName()),
                                "rw");
                        BTree bTree = new BTree(indexFile);
                        bTree.delete(oldValue, record.getRow_id());
                        bTree.insert(newValueMap.get(i), row_id);
                        indexFile.close();
                    }
                }
            }
        }

        if (!tablemetaData.getTableName().equalsIgnoreCase(tables_table) && !tablemetaData.getTableName().equalsIgnoreCase(columns_table))
            System.out.println("* " + count + " record(s) modified.");
        return count;
    }

    public void selectRecords(TableMetaData tablemetaData, List<String> columNames, Operation operation)
            throws IOException {
        List<Integer> ordinal_postions = tablemetaData.get_ordinal_postions(columNames);
        System.out.println();
        List<Integer> print_position = new ArrayList<>();
        int column_printed_len = 0;
        print_position.add(column_printed_len);
        int total_table_printed_len = 0;
        if (showing_rowid) {
            System.out.print("row_id");
            System.out.print(FaradayBaseApplication.line(" ", 5));
            print_position.add(10);
            total_table_printed_len += 10;
        }

        String titleString = "";
        for (int i : ordinal_postions) {
            String column_name = tablemetaData.getColumnNameAttributes().get(i).getColName();
            column_printed_len = Math.max(column_name.length(),
                    tablemetaData.getColumnNameAttributes().get(i).getDataType().getPrintOffset()) + 5;
            print_position.add(column_printed_len);
            titleString += "| " + column_name;
            titleString += FaradayBaseApplication.line(" ", column_printed_len - column_name.length());
            total_table_printed_len += column_printed_len + 2;
        }
        System.out.println(FaradayBaseApplication.line("-", total_table_printed_len));
        System.out.println(titleString);
        System.out.println(FaradayBaseApplication.line("-", total_table_printed_len));
        BPlusTree bPlusOneTree = new BPlusTree(file, tablemetaData.getRootPageNumber(), tablemetaData.getTableName());
        String current_value = "";
        for (Integer page_number : bPlusOneTree.get_allLeaves(operation)) {
            Page page = new Page(file, page_number);
            for (TableRecord record : page.getPgRecords()) {
                if (operation != null) {
                    if (!operation.condition_check(record.getAttributes().get(operation.getColumnOrdinal()).getFieldValue()))
                        continue;
                }
                int column_count = 0;
                if (showing_rowid) {
                    current_value = Integer.valueOf(record.getRow_id()).toString();
                    System.out.print(current_value);
                    System.out.print(
                            FaradayBaseApplication.line(" ", print_position.get(++column_count) - current_value.length()));
                }
                for (int i : ordinal_postions) {
                    current_value = record.getAttributes().get(i).getFieldValue();
                    System.out.print("| " + current_value);
                    System.out.print(
                            FaradayBaseApplication.line(" ", print_position.get(++column_count) - current_value.length()));
                }
                System.out.println();
            }
        }
        System.out.println();
    }


    public static int getRootPageNumber(RandomAccessFile binaryfile) {
        int rootpage = 0;
        try {
            for (int i = 0; i < binaryfile.length() / FaradayBaseBinaryFile.page_size; i++) {
                binaryfile.seek(i * FaradayBaseBinaryFile.page_size + 0x0A);
                int a = binaryfile.readInt();
                if (a == -1) {
                    return i;
                }
            }
            return rootpage;
        } catch (Exception e) {
            log.error("root page no not found!! ");
            log.debug("ERROR:", e);
        }
        return -1;
    }


    public static void createBootstrapFiles() {

        try {
            File dataDir = new File("data");
            dataDir.mkdir();
            String[] oldTableFiles;
            oldTableFiles = dataDir.list();
            for (int i = 0; i < oldTableFiles.length; i++) {
                File anOldFile = new File(dataDir, oldTableFiles[i]);
                anOldFile.delete();
            }
        } catch (SecurityException se) {
            out.println("Data container directory not created!!");
            out.println(se);
        }


        try {

            int current_page_number = 0;

            RandomAccessFile faradaybase_catalog_of_tables = new RandomAccessFile(
                    FaradayBaseApplication.getTableFilePath(tables_table), "rw");
            Page.add_new_pg(faradaybase_catalog_of_tables, PageType.LEAF, -1, -1);
            Page page = new Page(faradaybase_catalog_of_tables, current_page_number);

            page.add_table_row(tables_table,
                    Arrays.asList(new Attribute[]{new Attribute(DataType.TEXT, FaradayBaseBinaryFile.tables_table),
                            new Attribute(DataType.INT, "2"), new Attribute(DataType.SMALLINT, "0"),
                            new Attribute(DataType.SMALLINT, "0")}));

            page.add_table_row(tables_table,
                    Arrays.asList(new Attribute[]{new Attribute(DataType.TEXT, FaradayBaseBinaryFile.columns_table),
                            new Attribute(DataType.INT, "11"), new Attribute(DataType.SMALLINT, "0"),
                            new Attribute(DataType.SMALLINT, "2")}));

            faradaybase_catalog_of_tables.close();
        } catch (Exception e) {
            log.error("Error creating database_tables file");
            log.debug("ERROR:", e);

        }


        try {
            RandomAccessFile faradaybaseColumnsCatalog = new RandomAccessFile(
                    FaradayBaseApplication.getTableFilePath(columns_table), "rw");
            Page.add_new_pg(faradaybaseColumnsCatalog, PageType.LEAF, -1, -1);
            Page page = new Page(faradaybaseColumnsCatalog, 0);

            short ordinal_position = 1;


            page.add_new_col(
                    new ColumnInformation(tables_table, DataType.TEXT, "table_name", true, false, ordinal_position++));
            page.add_new_col(new ColumnInformation(tables_table, DataType.INT, "record_count", false, false,
                    ordinal_position++));
            page.add_new_col(new ColumnInformation(tables_table, DataType.SMALLINT, "avg_length", false, false,
                    ordinal_position++));
            page.add_new_col(new ColumnInformation(tables_table, DataType.SMALLINT, "root_page", false, false,
                    ordinal_position++));


            ordinal_position = 1;

            page.add_new_col(new ColumnInformation(columns_table, DataType.TEXT, "table_name", false, false,
                    ordinal_position++));
            page.add_new_col(new ColumnInformation(columns_table, DataType.TEXT, "column_name", false, false,
                    ordinal_position++));
            page.add_new_col(new ColumnInformation(columns_table, DataType.SMALLINT, "data_type", false, false,
                    ordinal_position++));
            page.add_new_col(new ColumnInformation(columns_table, DataType.SMALLINT, "ordinal_position", false, false,
                    ordinal_position++));
            page.add_new_col(new ColumnInformation(columns_table, DataType.TEXT, "is_nullable", false, false,
                    ordinal_position++));
            page.add_new_col(new ColumnInformation(columns_table, DataType.SMALLINT, "column_key", false, true,
                    ordinal_position++));
            page.add_new_col(new ColumnInformation(columns_table, DataType.SMALLINT, "is_unique", false, false,
                    ordinal_position++));

            faradaybaseColumnsCatalog.close();
            is_data_store_initialized = true;
        } catch (Exception e) {
            log.error("Error creating database_columns file");
            log.debug("ERROR:", e);
        }
    }
}
