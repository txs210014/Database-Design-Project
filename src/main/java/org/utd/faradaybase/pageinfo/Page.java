package org.utd.faradaybase.pageinfo;

import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.utd.faradaybase.DataType;
import org.utd.faradaybase.FaradayBaseBinaryFile;
import org.utd.faradaybase.attribute.Attribute;
import org.utd.faradaybase.columninfo.ColumnInformation;
import org.utd.faradaybase.constants.Constants;
import org.utd.faradaybase.index.IndexNode;
import org.utd.faradaybase.index.IndexRecord;
import org.utd.faradaybase.operation.Operation;
import org.utd.faradaybase.tableinfo.InternalTableRecord;
import org.utd.faradaybase.tableinfo.TableMetaData;
import org.utd.faradaybase.tableinfo.TableRecord;
import org.utd.faradaybase.util.Util;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

@Data
@Log4j2
public class Page {

    private PageType pageType;
    private short cellCount = 0;
    private int pageNumber;
    private short contentStartingOffset;
    private int rightPage;
    private int parentPgNumber;
    private List<TableRecord> records;
    private boolean isTableRecordsRefreshed = false;
    private long pgStart;
    private int lastRowId;
    private int spaceAvailable;
    private RandomAccessFile binaryFile;
    private boolean isIndexPgClean;
    private List<InternalTableRecord> leftChildren;
    private IndexNode incomingInsert;
    private DataType indexValDatatype;
    private TreeSet<Long> lindexVals;
    private TreeSet<String> sindexVals;
    private HashMap<String, IndexRecord> indexValPointer;
    private Map<Integer, TableRecord> recordsMap;

    public Page(RandomAccessFile file, int pageNumber) {
        try {
            this.pageNumber = pageNumber;
            indexValDatatype = null;
            lindexVals = new TreeSet<>();
            sindexVals = new TreeSet<>();
            indexValPointer = new HashMap<>();
            recordsMap = new HashMap<>();
            binaryFile = file;
            lastRowId = 0;
            pgStart = FaradayBaseBinaryFile.page_size * pageNumber;
            binaryFile.seek(pgStart);
            pageType = PageType.get(binaryFile.readByte()); // pagetype
            binaryFile.readByte(); // unused
            cellCount = binaryFile.readShort();
            contentStartingOffset = binaryFile.readShort();
            spaceAvailable = contentStartingOffset - 0x10 - (cellCount * 2);
            rightPage = binaryFile.readInt();
            parentPgNumber = binaryFile.readInt();
            binaryFile.readShort();
            if (pageType == PageType.LEAF)
                fill_table_records();
            if (pageType == PageType.INTERIOR)
                fill_left_children();
            if (pageType == PageType.INTERIORINDEX || pageType == PageType.LEAFINDEX)
                fill_index_records();

        } catch (IOException ex) {
            System.out.println("! Error while reading the page " + ex.getMessage());
        }
    }

    public List<TableRecord> getPgRecords() {

        if (isTableRecordsRefreshed)
            fill_table_records();

        isTableRecordsRefreshed = false;

        return records;
    }

    private void del_pg_record(short recordIndex) {
        try {

            for (int i = recordIndex + 1; i < cellCount; i++) {
                binaryFile.seek(pgStart + 0x10 + (i * 2));
                short cell_start = binaryFile.readShort();

                if (cell_start == 0)
                    continue;

                binaryFile.seek(pgStart + 0x10 + ((i - 1) * 2));
                binaryFile.writeShort(cell_start);
            }

            cellCount--;

            binaryFile.seek(pgStart + 2);
            binaryFile.writeShort(cellCount);

        } catch (IOException e) {
            System.err.println("ERROR: Unable to delete record at " + recordIndex + "in page " + pageNumber);
        }
    }

    public void del_table_record(String table_name, short recordIndex) {
        del_pg_record(recordIndex);
        TableMetaData metaData = new TableMetaData(table_name);
        metaData.setRecordCount(metaData.getRecordCount() - 1);
        metaData.update_metaData();
        isTableRecordsRefreshed = true;

    }

    private void add_new_pg_record(Byte[] record_head, Byte[] record_content) throws IOException {
        if (record_head.length + record_content.length + 4 > spaceAvailable) {
            try {
                if (pageType == PageType.LEAF || pageType == PageType.INTERIOR) {
                    table_overflow_handling();
                } else {
                    index_overflow_handling();
                    return;
                }
            } catch (IOException e) {
                System.out.println("! Error while table_overflow_handling");
            }
        }

        short cell_start = contentStartingOffset;

        short new_cell_start = Integer.valueOf((cell_start - record_content.length - record_head.length - 2))
                .shortValue();
        binaryFile.seek(pageNumber * FaradayBaseBinaryFile.page_size + new_cell_start);

        // record head
        binaryFile.write(Util.BytesToBytes(record_head)); // datatypes

        // record body
        binaryFile.write(Util.BytesToBytes(record_content));

        binaryFile.seek(pgStart + 0x10 + (cellCount * 2));
        binaryFile.writeShort(new_cell_start);

        contentStartingOffset = new_cell_start;

        binaryFile.seek(pgStart + 4);
        binaryFile.writeShort(contentStartingOffset);

        cellCount++;
        binaryFile.seek(pgStart + 2);
        binaryFile.writeShort(cellCount);

        spaceAvailable = contentStartingOffset - 0x10 - (cellCount * 2);

    }

    private void index_overflow_handling() throws IOException {
        if (pageType == PageType.LEAFINDEX) {
            if (parentPgNumber == -1) {
                parentPgNumber = add_new_pg(binaryFile, PageType.INTERIORINDEX, pageNumber, -1);
            }
            int newLeftLeafPageNo = add_new_pg(binaryFile, PageType.LEAFINDEX, pageNumber, parentPgNumber);

            setParent(parentPgNumber);
            IndexNode incoming_insert_temp = this.incomingInsert;

            Page left_leaf_pg = new Page(binaryFile, newLeftLeafPageNo);
            IndexNode to_insert_parent_index_node = index_split_records_between_pgs(left_leaf_pg);
            Page parent_pg = new Page(binaryFile, parentPgNumber);
            int comparison_result = Operation.compare(incoming_insert_temp.getIndexVal().getFieldValue(),
                    to_insert_parent_index_node.getIndexVal().getFieldValue(), incomingInsert.getIndexVal().getDataType());

            if (comparison_result == 0) {
                to_insert_parent_index_node.getRowIds().addAll(incoming_insert_temp.getRowIds());
                parent_pg.add_index(to_insert_parent_index_node, newLeftLeafPageNo);
                shift_pg(parent_pg);
                return;
            } else if (comparison_result < 0) {
                left_leaf_pg.add_index(incoming_insert_temp);
                shift_pg(left_leaf_pg);
            } else {
                add_index(incoming_insert_temp);
            }

            parent_pg.add_index(to_insert_parent_index_node, newLeftLeafPageNo);

        } else {

            if (cellCount < 3 && !isIndexPgClean) {
                isIndexPgClean = true;
                String[] temp_index_vals = get_index_vals().toArray(new String[get_index_vals().size()]);
                @SuppressWarnings("unchecked")
                HashMap<String, IndexRecord> indexValuePointerTemp = (HashMap<String, IndexRecord>) indexValPointer
                        .clone();
                IndexNode incoming_insert_temp = this.incomingInsert;
                clean_page();
                for (int i = 0; i < temp_index_vals.length; i++) {
                    add_index(indexValuePointerTemp.get(temp_index_vals[i]).getIndexNode(),
                            indexValuePointerTemp.get(temp_index_vals[i]).getLeftPgno());
                }

                add_index(incoming_insert_temp);
                return;
            }

            if (isIndexPgClean) {
                System.out.println(
                        "! org.utd.faradaybase.pageinfo.Page overflow, increase the page size. Reached Max number of rows for an Index value");
                return;
            }

            if (parentPgNumber == -1) {
                parentPgNumber = add_new_pg(binaryFile, PageType.INTERIORINDEX, pageNumber, -1);
            }
            int new_left_internal_pgnum = add_new_pg(binaryFile, PageType.INTERIORINDEX, pageNumber, parentPgNumber);

            setParent(parentPgNumber);

            IndexNode incoming_insert_temp = this.incomingInsert;
            Page leftInteriorPage = new Page(binaryFile, new_left_internal_pgnum);

            IndexNode to_insert_parent_index_node = index_split_records_between_pgs(leftInteriorPage);

            Page parent_pg = new Page(binaryFile, parentPgNumber);
            int comparison_result = Operation.compare(incoming_insert_temp.getIndexVal().getFieldValue(),
                    to_insert_parent_index_node.getIndexVal().getFieldValue(), incomingInsert.getIndexVal().getDataType());
            Page middle_orphan = new Page(binaryFile, to_insert_parent_index_node.getLeftPageNumber());
            middle_orphan.setParent(parentPgNumber);
            leftInteriorPage.setRightPageNo(middle_orphan.pageNumber);

            if (comparison_result == 0) {
                to_insert_parent_index_node.getRowIds().addAll(incoming_insert_temp.getRowIds());
                parent_pg.add_index(to_insert_parent_index_node, new_left_internal_pgnum);
                shift_pg(parent_pg);
                return;
            } else if (comparison_result < 0) {
                leftInteriorPage.add_index(incoming_insert_temp);
                shift_pg(leftInteriorPage);
            } else {
                add_index(incoming_insert_temp);
            }

            parent_pg.add_index(to_insert_parent_index_node, new_left_internal_pgnum);

        }

    }

    private void clean_page() throws IOException {

        cellCount = 0;
        contentStartingOffset = Long.valueOf(FaradayBaseBinaryFile.page_size).shortValue();
        spaceAvailable = contentStartingOffset - 0x10 - (cellCount * 2);
        byte[] emptybytes = new byte[Constants.PAGE_SIZE - 16];
        Arrays.fill(emptybytes, (byte) 0);
        binaryFile.seek(pgStart + 16);
        binaryFile.write(emptybytes);
        binaryFile.seek(pgStart + 2);
        binaryFile.writeShort(cellCount);
        binaryFile.seek(pgStart + 4);
        binaryFile.writeShort(contentStartingOffset);
        lindexVals = new TreeSet<>();
        sindexVals = new TreeSet<>();
        indexValPointer = new HashMap<>();

    }

    private IndexNode index_split_records_between_pgs(Page newleftPage) throws IOException {

        try {
            int mid = get_index_vals().size() / 2;
            String[] temp_index_vals = get_index_vals().toArray(new String[get_index_vals().size()]);

            IndexNode to_insert_parent_index_node = indexValPointer.get(temp_index_vals[mid]).getIndexNode();
            to_insert_parent_index_node.setLeftPageNumber(indexValPointer.get(temp_index_vals[mid]).getLeftPgno());

            @SuppressWarnings("unchecked")
            HashMap<String, IndexRecord> indexValuePointerTemp = (HashMap<String, IndexRecord>) indexValPointer
                    .clone();

            for (int i = 0; i < mid; i++) {
                newleftPage.add_index(indexValuePointerTemp.get(temp_index_vals[i]).getIndexNode(),
                        indexValuePointerTemp.get(temp_index_vals[i]).getLeftPgno());
            }

            clean_page();
            sindexVals = new TreeSet<>();
            lindexVals = new TreeSet<>();
            indexValPointer = new HashMap<String, IndexRecord>();
            for (int i = mid + 1; i < temp_index_vals.length; i++) {
                add_index(indexValuePointerTemp.get(temp_index_vals[i]).getIndexNode(),
                        indexValuePointerTemp.get(temp_index_vals[i]).getLeftPgno());
            }

            return to_insert_parent_index_node;
        } catch (IOException e) {
            System.err.println("ERROR: ! Insert into Index File failed. Error while splitting index pages");
            throw e;
        }

    }

    private void table_overflow_handling() throws IOException {
        if (pageType == PageType.LEAF) {
            int new_rightleaf_pgnum = add_new_pg(binaryFile, pageType, -1, -1);
            if (parentPgNumber == -1) {
                int new_parent_pgnum = add_new_pg(binaryFile, PageType.INTERIOR, new_rightleaf_pgnum, -1);
                setRightPageNo(new_rightleaf_pgnum);
                setParent(new_parent_pgnum);
                Page new_parent_pg = new Page(binaryFile, new_parent_pgnum);
                new_parent_pgnum = new_parent_pg.add_left_table_child(pageNumber, lastRowId);
                new_parent_pg.setRightPageNo(new_rightleaf_pgnum);
                Page new_leaf_pg = new Page(binaryFile, new_rightleaf_pgnum);
                new_leaf_pg.setParent(new_parent_pgnum);
                shift_pg(new_leaf_pg);
            } else {
                Page parent_pg = new Page(binaryFile, parentPgNumber);
                parentPgNumber = parent_pg.add_left_table_child(pageNumber, lastRowId);
                parent_pg.setRightPageNo(new_rightleaf_pgnum);
                setRightPageNo(new_rightleaf_pgnum);
                Page new_leaf_pg = new Page(binaryFile, new_rightleaf_pgnum);
                new_leaf_pg.setParent(parentPgNumber);
                shift_pg(new_leaf_pg);
            }
        } else {
            int new_rightleaf_pgnum = add_new_pg(binaryFile, pageType, -1, -1);
            int new_parent_pgnum = add_new_pg(binaryFile, PageType.INTERIOR, new_rightleaf_pgnum, -1);
            setRightPageNo(new_rightleaf_pgnum);
            setParent(new_parent_pgnum);
            Page new_parent_pg = new Page(binaryFile, new_parent_pgnum);
            new_parent_pgnum = new_parent_pg.add_left_table_child(pageNumber, lastRowId);
            new_parent_pg.setRightPageNo(new_rightleaf_pgnum);
            Page new_leaf_pg = new Page(binaryFile, new_rightleaf_pgnum);
            new_leaf_pg.setParent(new_parent_pgnum);
            shift_pg(new_leaf_pg);
        }
    }

    private int add_left_table_child(int left_child_pgnum, int rowId) throws IOException {
        for (InternalTableRecord intRecord : leftChildren) {
            if (intRecord.getRow_id() == rowId)
                return pageNumber;
        }
        if (pageType == PageType.INTERIOR) {
            List<Byte> record_head = new ArrayList<>();
            List<Byte> record_content = new ArrayList<>();

            record_head.addAll(Arrays.asList(Util.int_to_Bytes(left_child_pgnum)));
            record_content.addAll(Arrays.asList(Util.int_to_Bytes(rowId)));

            add_new_pg_record(record_head.toArray(new Byte[record_head.size()]),
                    record_content.toArray(new Byte[record_content.size()]));
        }
        return pageNumber;

    }

    public void add_index(IndexNode node) throws IOException {
        add_index(node, -1);
    }

    public void add_index(IndexNode node, int left_pgno) throws IOException {
        incomingInsert = node;
        incomingInsert.setLeftPageNumber(left_pgno);
        List<Integer> rowIds = new ArrayList<>();
        List<String> ixValues = get_index_vals();
        if (get_index_vals().contains(node.getIndexVal().getFieldValue())) {
            left_pgno = indexValPointer.get(node.getIndexVal().getFieldValue()).getLeftPgno();
            incomingInsert.setLeftPageNumber(left_pgno);
            rowIds = indexValPointer.get(node.getIndexVal().getFieldValue()).getRowIds();
            rowIds.addAll(incomingInsert.getRowIds());
            incomingInsert.setRowIds(rowIds);
            del_pg_record(indexValPointer.get(node.getIndexVal().getFieldValue()).getPgHeadIndex());
            if (indexValDatatype == DataType.TEXT || indexValDatatype == null)
                sindexVals.remove(node.getIndexVal().getFieldValue());
            else
                lindexVals.remove(Long.parseLong(node.getIndexVal().getFieldValue()));
        }

        rowIds.addAll(node.getRowIds());

        rowIds = new ArrayList<>(new HashSet<>(rowIds));

        List<Byte> recordHead = new ArrayList<>();
        List<Byte> record_content = new ArrayList<>();
        record_content.addAll(Arrays.asList(Integer.valueOf(rowIds.size()).byteValue()));
        if (node.getIndexVal().getDataType() == DataType.TEXT)
            record_content.add(Integer
                    .valueOf(node.getIndexVal().getDataType().getValue() + node.getIndexVal().getFieldValue().length()).byteValue());
        else
            record_content.add(node.getIndexVal().getDataType().getValue());

        // index value
        record_content.addAll(Arrays.asList(node.getIndexVal().getFieldValueByteObject()));

        // list of rowids
        for (int i = 0; i < rowIds.size(); i++) {
            record_content.addAll(Arrays.asList(Util.int_to_Bytes(rowIds.get(i))));
        }

        short payload = Integer.valueOf(record_content.size()).shortValue();
        if (pageType == PageType.INTERIORINDEX)
            recordHead.addAll(Arrays.asList(Util.int_to_Bytes(left_pgno)));

        recordHead.addAll(Arrays.asList(Util.shortToBytes(payload)));

        add_new_pg_record(recordHead.toArray(new Byte[recordHead.size()]),
                record_content.toArray(new Byte[record_content.size()]));

        fill_index_records();
        refresh_head_offset();

    }

    private void refresh_head_offset() {
        try {
            binaryFile.seek(pgStart + 0x10);
            for (String indexVal : get_index_vals()) {
                binaryFile.writeShort(indexValPointer.get(indexVal).getPgOffset());
            }

        } catch (IOException ex) {
            System.out.println("! Error while refrshing header offset " + ex.getMessage());
        }
    }

    private void fill_table_records() {
        short payLoadSize = 0;
        byte noOfcolumns = 0;
        records = new ArrayList<TableRecord>();
        recordsMap = new HashMap<>();
        try {
            for (short i = 0; i < cellCount; i++) {
                binaryFile.seek(pgStart + 0x10 + (i * 2));
                short cell_start = binaryFile.readShort();
                if (cell_start == 0)
                    continue;
                binaryFile.seek(pgStart + cell_start);

                payLoadSize = binaryFile.readShort();
                int rowId = binaryFile.readInt();
                noOfcolumns = binaryFile.readByte();

                if (lastRowId < rowId)
                    lastRowId = rowId;

                byte[] colDatatypes = new byte[noOfcolumns];
                byte[] record_content = new byte[payLoadSize - noOfcolumns - 1];

                binaryFile.read(colDatatypes);
                binaryFile.read(record_content);

                TableRecord record = new TableRecord(i, rowId, cell_start, colDatatypes, record_content);
                records.add(record);
                recordsMap.put(rowId, record);
            }
        } catch (IOException ex) {
            System.out.println("! Error while filling records from the page " + ex.getMessage());
        }
    }

    private void fill_left_children() {
        try {
            leftChildren = new ArrayList<>();

            int left_child_pgnum;
            int rowId;
            for (int i = 0; i < cellCount; i++) {
                binaryFile.seek(pgStart + 0x10 + (i * 2));
                short cell_start = binaryFile.readShort();
                if (cell_start == 0)// ignore deleted cells
                    continue;
                binaryFile.seek(pgStart + cell_start);

                left_child_pgnum = binaryFile.readInt();
                rowId = binaryFile.readInt();
                leftChildren.add(new InternalTableRecord(rowId, left_child_pgnum));
            }
        } catch (IOException ex) {
            System.out.println("! Error while filling records from the page " + ex.getMessage());
        }

    }

    private void fill_index_records() {
        try {
            lindexVals = new TreeSet<>();
            sindexVals = new TreeSet<>();
            indexValPointer = new HashMap<>();

            int left_pgno = -1;
            byte noOfRowIds = 0;
            byte dataType = 0;
            for (short i = 0; i < cellCount; i++) {
                binaryFile.seek(pgStart + 0x10 + (i * 2));
                short cell_start = binaryFile.readShort();
                if (cell_start == 0)// ignore deleted cells
                    continue;
                binaryFile.seek(pgStart + cell_start);

                if (pageType == PageType.INTERIORINDEX)
                    left_pgno = binaryFile.readInt();

                short payload = binaryFile.readShort(); // payload

                noOfRowIds = binaryFile.readByte();
                dataType = binaryFile.readByte();

                if (indexValDatatype == null && DataType.get(dataType) != DataType.NULL)
                    indexValDatatype = DataType.get(dataType);

                byte[] indexValue = new byte[DataType.getLength(dataType)];
                binaryFile.read(indexValue);

                List<Integer> lstRowIds = new ArrayList<>();
                for (int j = 0; j < noOfRowIds; j++) {
                    lstRowIds.add(binaryFile.readInt());
                }

                IndexRecord record = new IndexRecord(i, DataType.get(dataType), noOfRowIds, indexValue, lstRowIds,
                        left_pgno, rightPage, pageNumber, cell_start);

                if (indexValDatatype == DataType.TEXT || indexValDatatype == null)
                    sindexVals.add(record.getIndexNode().getIndexVal().getFieldValue());
                else
                    lindexVals.add(Long.parseLong(record.getIndexNode().getIndexVal().getFieldValue()));

                indexValPointer.put(record.getIndexNode().getIndexVal().getFieldValue(), record);

            }
        } catch (IOException ex) {
            System.out.println("Error while filling records from the page " + ex.getMessage());
        }
    }

    public List<String> get_index_vals() {
        List<String> strIndexValues = new ArrayList<>();

        if (sindexVals.size() > 0)
            strIndexValues.addAll(Arrays.asList(sindexVals.toArray(new String[sindexVals.size()])));
        if (lindexVals.size() > 0) {
            Long[] lArray = lindexVals.toArray(new Long[lindexVals.size()]);
            for (int i = 0; i < lArray.length; i++) {
                strIndexValues.add(lArray[i].toString());
            }
        }

        return strIndexValues;

    }

    public boolean is_root() {
        return parentPgNumber == -1;
    }

    public static PageType get_pg_type(RandomAccessFile file, int pgnum) throws IOException {
        try {
            int pg_start = FaradayBaseBinaryFile.page_size * pgnum;
            file.seek(pg_start);
            return PageType.get(file.readByte());
        } catch (IOException ex) {
            System.out.println("Error while getting the page type " + ex.getMessage());
            throw ex;
        }
    }

    public static int add_new_pg(RandomAccessFile file, PageType pg_type, int right_page, int parent_pg_number) {
        try {
            int pgnum = Long.valueOf((file.length() / FaradayBaseBinaryFile.page_size)).intValue();
            file.setLength(file.length() + FaradayBaseBinaryFile.page_size);
            file.seek(FaradayBaseBinaryFile.page_size * pgnum);
            file.write(pg_type.getValue());
            file.write(0x00); // unused
            file.writeShort(0); // no of cells
            file.writeShort((short) (FaradayBaseBinaryFile.page_size)); // cell
            // start
            // offset

            file.writeInt(right_page);

            file.writeInt(parent_pg_number);

            return pgnum;
        } catch (IOException ex) {
            System.out.println("Error while adding new page" + ex.getMessage());
            return -1;
        }
    }

    public void update_record(TableRecord record, int ordinal_pos, Byte[] newValue) throws IOException {
        binaryFile.seek(pgStart + record.getRecord_offset() + 7);
        int value_offset = 0;
        for (int i = 0; i < ordinal_pos; i++) {
            value_offset += DataType.getLength((byte) binaryFile.readByte());
        }

        binaryFile.seek(pgStart + record.getRecord_offset() + 7 + record.getCol_data_types().length + value_offset);
        binaryFile.write(Util.BytesToBytes(newValue));

    }

    // Copies all the members from the new page to the current page
    private void shift_pg(Page newPage) {
        pageType = newPage.pageType;
        cellCount = newPage.cellCount;
        pageNumber = newPage.pageNumber;
        contentStartingOffset = newPage.contentStartingOffset;
        rightPage = newPage.rightPage;
        parentPgNumber = newPage.parentPgNumber;
        leftChildren = newPage.leftChildren;
        sindexVals = newPage.sindexVals;
        lindexVals = newPage.lindexVals;
        indexValPointer = newPage.indexValPointer;
        records = newPage.records;
        pgStart = newPage.pgStart;
        spaceAvailable = newPage.spaceAvailable;
    }

    public void setParent(int parent_pg_number) throws IOException {
        binaryFile.seek(FaradayBaseBinaryFile.page_size * pageNumber + 0x0A);
        binaryFile.writeInt(parent_pg_number);
        this.parentPgNumber = parent_pg_number;
    }

    public void setRightPageNo(int rightPageNo) throws IOException {
        binaryFile.seek(FaradayBaseBinaryFile.page_size * pageNumber + 0x06);
        binaryFile.writeInt(rightPageNo);
        this.rightPage = rightPageNo;
    }

    public void del_index(IndexNode node) throws IOException {
        del_pg_record(indexValPointer.get(node.getIndexVal().getFieldValue()).getPgHeadIndex());
        fill_index_records();
        refresh_head_offset();
    }

    public void add_new_col(ColumnInformation columnInfo) throws IOException {
        try {
            add_table_row(FaradayBaseBinaryFile.columns_table,
                    Arrays.asList(new Attribute[]{new Attribute(DataType.TEXT, columnInfo.getTableName()),
                            new Attribute(DataType.TEXT, columnInfo.getColName()),
                            new Attribute(DataType.TEXT, columnInfo.getDataType().toString()),
                            new Attribute(DataType.SMALLINT, columnInfo.getOrdinalPos().toString()),
                            new Attribute(DataType.TEXT, columnInfo.isNullCol() ? "YES" : "NO"),
                            columnInfo.isKeyPrimary() ? new Attribute(DataType.TEXT, "PRI")
                                    : new Attribute(DataType.NULL, "NULL"),
                            new Attribute(DataType.TEXT, columnInfo.isUniqueCol() ? "YES" : "NO")}));
        } catch (Exception e) {
            log.error("! Could not add column");
            log.debug("ERROR:", e);
        }
    }

    public int add_table_row(String table_name, List<Attribute> attributes) throws IOException {
        List<Byte> colDataTypes = new ArrayList<Byte>();
        List<Byte> record_content = new ArrayList<Byte>();

        TableMetaData metaData = null;
        if (FaradayBaseBinaryFile.is_data_store_initialized) {
            metaData = new TableMetaData(table_name);
            if (!metaData.validate_insertion(attributes))
                return -1;
        }

        for (Attribute attribute : attributes) {
            record_content.addAll(Arrays.asList(attribute.getFieldValueByteObject()));
            if (attribute.getDataType() == DataType.TEXT) {
                colDataTypes.add(Integer
                        .valueOf(DataType.TEXT.getValue() + (new String(attribute.getFieldValue()).length())).byteValue());
            } else {
                colDataTypes.add(attribute.getDataType().getValue());
            }
        }

        lastRowId++;

        short payLoadSize = Integer.valueOf(record_content.size() + colDataTypes.size() + 1).shortValue();

        List<Byte> record_head = new ArrayList<>();

        record_head.addAll(Arrays.asList(Util.shortToBytes(payLoadSize)));
        record_head.addAll(Arrays.asList(Util.int_to_Bytes(lastRowId)));
        record_head.add(Integer.valueOf(colDataTypes.size()).byteValue());
        record_head.addAll(colDataTypes);

        add_new_pg_record(record_head.toArray(new Byte[record_head.size()]),
                record_content.toArray(new Byte[record_content.size()]));

        isTableRecordsRefreshed = true;
        if (FaradayBaseBinaryFile.is_data_store_initialized) {
            metaData.setRecordCount(metaData.getRecordCount() + 1);
            metaData.update_metaData();
        }
        return lastRowId;
    }

}
