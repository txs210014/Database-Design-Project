package org.utd.faradaybase.tableinfo;

import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.utd.faradaybase.columninfo.ColumnInformation;
import org.utd.faradaybase.DataType;
import org.utd.faradaybase.FaradayBaseApplication;
import org.utd.faradaybase.FaradayBaseBinaryFile;
import org.utd.faradaybase.attribute.Attribute;
import org.utd.faradaybase.operation.Operation;
import org.utd.faradaybase.pageinfo.Page;
import org.utd.faradaybase.trees.BPlusTree;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Data
@Log4j2
public class TableMetaData {

    private int recordCount;
    private List<TableRecord> columnData;
    private List<ColumnInformation> columnNameAttributes;
    private List<String> columnNames;
    private String tableName;
    private boolean isTableExisting;
    private int rootPageNumber;
    private int lastRowid;

    public TableMetaData(String tableName) {
        this.tableName = tableName = tableName.toLowerCase().trim();
        isTableExisting = false;
        try {

            RandomAccessFile faradaybaseTablesCatalog = new RandomAccessFile(
                    FaradayBaseApplication.getTableFilePath(FaradayBaseBinaryFile.tables_table), "r");
            int root_page_number = FaradayBaseBinaryFile.getRootPageNumber(faradaybaseTablesCatalog);

            BPlusTree bplusTree = new BPlusTree(faradaybaseTablesCatalog, root_page_number, tableName);
            for (Integer pageNo : bplusTree.get_allLeaves()) {
                Page page = new Page(faradaybaseTablesCatalog, pageNo);
                for (TableRecord record : page.getPgRecords()) {
                    if (record.getAttributes().get(0).getFieldValue().equalsIgnoreCase(tableName)) {
                        this.rootPageNumber = Integer.parseInt(record.getAttributes().get(3).getFieldValue());
                        recordCount = Integer.parseInt(record.getAttributes().get(1).getFieldValue());
                        isTableExisting = true;
                        break;
                    }
                }
                if (isTableExisting)
                    break;
            }

            faradaybaseTablesCatalog.close();
            if (isTableExisting) {
                load_columnData();
            } else {
                throw new Exception("Table does not exist.");
            }

        } catch (Exception e) {
            log.error("! Error while checking Table " + tableName + " exists.");
            log.debug("ERROR:", e);
        }
    }

    public boolean validate_insertion(List<Attribute> row) throws IOException {
        RandomAccessFile table_file = new RandomAccessFile(FaradayBaseApplication.getTableFilePath(tableName), "r");
        FaradayBaseBinaryFile file = new FaradayBaseBinaryFile(table_file);


        for (int i = 0; i < columnNameAttributes.size(); i++) {

            Operation operation = new Operation(columnNameAttributes.get(i).getDataType());
            operation.setColumnName(columnNameAttributes.get(i).getColName());
            operation.setColumnOrdinal(i);
            operation.setOperator("=");

            if (columnNameAttributes.get(i).isUniqueCol()) {
                operation.setConditionValue(row.get(i).getFieldValue());
                if (file.doesRecordExists(this, Arrays.asList(columnNameAttributes.get(i).getColName()), operation)) {
                    System.err.println("ERROR: ! Insert failed: Column " + columnNameAttributes.get(i).getColName() + " should be unique.");
                    table_file.close();
                    return false;
                }

            }
        }
        table_file.close();
        return true;
    }


    public boolean is_column_existing(List<String> columns) {

        if (columns.size() == 0)
            return true;

        List<String> Icolumns = new ArrayList<>(columns);

        for (ColumnInformation column_name_attr : columnNameAttributes) {
            if (Icolumns.contains(column_name_attr.getColName()))
                Icolumns.remove(column_name_attr.getColName());
        }

        return Icolumns.isEmpty();
    }


    public void update_metaData() {

        try {
            RandomAccessFile table_file = new RandomAccessFile(
                    FaradayBaseApplication.getTableFilePath(tableName), "r");

            Integer root_page_number = FaradayBaseBinaryFile.getRootPageNumber(table_file);
            table_file.close();


            RandomAccessFile faradaybaseTablesCatalog = new RandomAccessFile(
                    FaradayBaseApplication.getTableFilePath(FaradayBaseBinaryFile.tables_table), "rw");

            FaradayBaseBinaryFile tablesBinaryFile = new FaradayBaseBinaryFile(faradaybaseTablesCatalog);

            TableMetaData tablesMetaData = new TableMetaData(FaradayBaseBinaryFile.tables_table);

            Operation operation = new Operation(DataType.TEXT);
            operation.setColumName("table_name");
            operation.setColumnOrdinal(0);
            operation.setConditionValue(tableName);
            operation.setOperator("=");

            List<String> columns = Arrays.asList("record_count", "root_page");
            List<String> newValues = new ArrayList<>();

        newValues.add(new Integer(recordCount).toString());
        newValues.add(new Integer(root_page_number).toString());

            tablesBinaryFile.update_records_operation(tablesMetaData, operation, columns, newValues);

            faradaybaseTablesCatalog.close();
        } catch (IOException e) {
            System.out.println("! Error updating meta data for " + tableName);
        }


    }

    public List<Integer> get_ordinal_postions(List<String> columns) {
        List<Integer> ordinalPostions = new ArrayList<>();
        for (String column : columns) {
            ordinalPostions.add(columnNames.indexOf(column));
        }
        return ordinalPostions;
    }

    private void load_columnData() {
        try {

            RandomAccessFile faradaybaseColumnsCatalog = new RandomAccessFile(
                    FaradayBaseApplication.getTableFilePath(FaradayBaseBinaryFile.columns_table), "r");
            int root_page_number = FaradayBaseBinaryFile.getRootPageNumber(faradaybaseColumnsCatalog);

            columnData = new ArrayList<>();
            columnNameAttributes = new ArrayList<>();
            columnNames = new ArrayList<>();
            BPlusTree bPlusOneTree = new BPlusTree(faradaybaseColumnsCatalog, root_page_number, tableName);

            for (Integer pageNo : bPlusOneTree.get_allLeaves()) {

                Page page = new Page(faradaybaseColumnsCatalog, pageNo);

                for (TableRecord record : page.getPgRecords()) {

                    if (record.getAttributes().get(0).getFieldValue().equalsIgnoreCase(tableName)) {
                        {
                            columnData.add(record);
                            columnNames.add(record.getAttributes().get(1).getFieldValue());
                            ColumnInformation colInfo = new ColumnInformation(
                                    tableName
                                    , DataType.get(record.getAttributes().get(2).getFieldValue())
                                    , record.getAttributes().get(1).getFieldValue()
                                    , record.getAttributes().get(6).getFieldValue().equalsIgnoreCase("YES")
                                    , record.getAttributes().get(4).getFieldValue().equalsIgnoreCase("YES")
                                    , Short.parseShort(record.getAttributes().get(3).getFieldValue())
                            );

                            if (record.getAttributes().get(5).getFieldValue().equalsIgnoreCase("PRI"))
                                colInfo.set_key_asPrimary();

                            columnNameAttributes.add(colInfo);
                        }
                    }
                }
            }

            faradaybaseColumnsCatalog.close();
        } catch (Exception e) {
            log.error("! Error while getting column data for " + tableName);
            log.debug("ERROR:", e);
        }

    }
}