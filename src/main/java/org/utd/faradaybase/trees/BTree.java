package org.utd.faradaybase.trees;

import lombok.Data;
import org.utd.faradaybase.*;
import org.utd.faradaybase.attribute.Attribute;
import org.utd.faradaybase.index.IndexRecord;
import org.utd.faradaybase.index.IndexNode;
import org.utd.faradaybase.operation.OperandType;
import org.utd.faradaybase.operation.Operation;
import org.utd.faradaybase.pageinfo.Page;
import org.utd.faradaybase.pageinfo.PageType;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Data
public class BTree {
    private Page root;
    private RandomAccessFile binary_file;

    public BTree(RandomAccessFile file) {
        this.binary_file = file;
        this.root = new Page(binary_file, FaradayBaseBinaryFile.getRootPageNumber(binary_file));
    }

    private String binarySearch(String[] values, String searchValue, int start, int end, DataType dataType) {

        if (end - start <= 3) {
            int i;
            for (i = start; i < end; i++) {
                if (Operation.compare(values[i], searchValue, dataType) < 0)
                    continue;
                else
                    break;
            }
            return values[i];
        } else {

            int mid = (end - start) / 2 + start;
            if (values[mid].equalsIgnoreCase(searchValue))
                return values[mid];

            if (Operation.compare(values[mid], searchValue, dataType) < 0)
                return binarySearch(values, searchValue, mid + 1, end, dataType);
            else
                return binarySearch(values, searchValue, start, mid - 1, dataType);

        }

    }


    private int get_closest_pagenum(Page page, String value) {
        if (page.getPageType() == PageType.LEAFINDEX) {
            return page.getPageNumber();
        } else {
            if (Operation.compare(value, page.get_index_vals().get(0), page.getIndexValDatatype()) < 0)
                return get_closest_pagenum
                        (new Page(binary_file, page.getIndexValPointer().get(page.get_index_vals().get(0)).getLeftPgno()),
                                value);
            else if (Operation.compare(value, page.get_index_vals().get(page.get_index_vals().size() - 1), page.getIndexValDatatype()) > 0)
                return get_closest_pagenum(
                        new Page(binary_file, page.getRightPage()),
                        value);
            else {
                String closest_value = binarySearch(page.get_index_vals().toArray(new String[page.get_index_vals().size()]), value, 0, page.get_index_vals().size() - 1, page.getIndexValDatatype());
                int i = page.get_index_vals().indexOf(closest_value);
                List<String> index_values = page.get_index_vals();
                if (closest_value.compareTo(value) < 0 && i + 1 < index_values.size()) {
                    return page.getIndexValPointer().get(index_values.get(i + 1)).getLeftPgno();
                } else if (closest_value.compareTo(value) > 0) {
                    return page.getIndexValPointer().get(closest_value).getLeftPgno();
                } else {
                    return page.getPageNumber();
                }
            }
        }
    }

    public void insert(Attribute attribute, List<Integer> row_ids) {
        try {
            int page_num = get_closest_pagenum(root, attribute.getFieldValue());
            Page page = new Page(binary_file, page_num);
            page.add_index(new IndexNode(attribute, row_ids));
        } catch (IOException e) {
            System.err.println("ERROR: Failed to insert " + attribute.getFieldValue() + " into index file");
        }
    }

    public void insert(Attribute attribute, int row_id) {
        insert(attribute, Arrays.asList(row_id));
    }

    public void delete(Attribute attribute, int row_id) {

        try {
            int page_num = get_closest_pagenum(root, attribute.getFieldValue());
            Page page = new Page(binary_file, page_num);

            IndexNode temp_node = page.getIndexValPointer().get(attribute.getFieldValue()).getIndexNode();
            temp_node.getRowIds().remove(temp_node.getRowIds().indexOf(row_id));
            page.del_index(temp_node);
            if (temp_node.getRowIds().size() != 0)
                page.add_index(temp_node);
        } catch (IOException e) {
            System.err.println("ERROR: Failed to delete " + attribute.getFieldValue() + " from index file");
        }

    }

    public List<Integer> get_row_ids(Operation operation) {
        List<Integer> row_ids = new ArrayList<>();
        Page page = new Page(binary_file, get_closest_pagenum(root, operation.getComparatorValue()));
        String[] index_values = page.get_index_vals().toArray(new String[page.get_index_vals().size()]);
        OperandType operationType = operation.getOperation();
        for (int i = 0; i < index_values.length; i++) {
            if (operation.condition_check(page.getIndexValPointer().get(index_values[i]).getIndexNode().getIndexVal().getFieldValue()))
                row_ids.addAll(page.getIndexValPointer().get(index_values[i]).getRowIds());
        }
        if (operationType == OperandType.LESSTHAN || operationType == OperandType.LESSTHANOREQUAL) {
            if (page.getPageType() == PageType.LEAFINDEX)
                row_ids.addAll(getAllRowIdsLeftOf(page.getParentPgNumber(), index_values[0]));
            else
                row_ids.addAll(getAllRowIdsLeftOf(page.getPageNumber(), operation.getComparatorValue()));
        }

        if (operationType == OperandType.GREATERTHAN || operationType == OperandType.GREATERTHANOREQUAL) {
            if (page.getPageType() == PageType.LEAFINDEX)
                row_ids.addAll(get_all_rowids_atRight(page.getParentPgNumber(), index_values[index_values.length - 1]));
            else
                row_ids.addAll(get_all_rowids_atRight(page.getPageNumber(), operation.getComparatorValue()));
        }

        return row_ids;

    }

    private List<Integer> getAllRowIdsLeftOf(int page_num, String index_value) {
        List<Integer> row_ids = new ArrayList<>();
        if (page_num == -1)
            return row_ids;
        Page page = new Page(this.binary_file, page_num);
        List<String> index_values = Arrays.asList(page.get_index_vals().toArray(new String[page.get_index_vals().size()]));


        for (int i = 0; i < index_values.size() && Operation.compare(index_values.get(i), index_value, page.getIndexValDatatype()) < 0; i++) {
            row_ids.addAll(page.getIndexValPointer().get(index_values.get(i)).getIndexNode().getRowIds());
            add_all_children_rowids(page.getIndexValPointer().get(index_values.get(i)).getLeftPgno(), row_ids);
        }
        if (page.getIndexValPointer().get(index_value) != null)
            add_all_children_rowids(page.getIndexValPointer().get(index_value).getLeftPgno(), row_ids);
        return row_ids;
    }

    private List<Integer> get_all_rowids_atRight(int page_num, String index_value) {

        List<Integer> row_ids = new ArrayList<>();

        if (page_num == -1)
            return row_ids;
        Page page = new Page(this.binary_file, page_num);
        List<String> index_values = Arrays.asList(page.get_index_vals().toArray(new String[page.get_index_vals().size()]));
        for (int i = index_values.size() - 1; i >= 0 && Operation.compare(index_values.get(i), index_value, page.getIndexValDatatype()) > 0; i--) {
            row_ids.addAll(page.getIndexValPointer().get(index_values.get(i)).getIndexNode().getRowIds());
            add_all_children_rowids(page.getRightPage(), row_ids);
        }

        if (page.getIndexValPointer().get(index_value) != null)
            add_all_children_rowids(page.getIndexValPointer().get(index_value).getRightPgno(), row_ids);

        return row_ids;
    }

    private void add_all_children_rowids(int page_num, List<Integer> row_ids) {
        if (page_num == -1)
            return;
        Page page = new Page(this.binary_file, page_num);
        for (IndexRecord record : page.getIndexValPointer().values()) {
            row_ids.addAll(record.getRowIds());
            if (page.getPageType() == PageType.INTERIORINDEX) {
                add_all_children_rowids(record.getLeftPgno(), row_ids);
                add_all_children_rowids(record.getRightPgno(), row_ids);
            }
        }
    }

}