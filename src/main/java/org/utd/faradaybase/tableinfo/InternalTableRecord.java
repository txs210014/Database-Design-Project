package org.utd.faradaybase.tableinfo;

import lombok.Data;

@Data
public class InternalTableRecord {
    private int left_child_pgnum;
    private int row_id;

    public InternalTableRecord(int rowId, int leftChildPageNo) {
        this.row_id = rowId;
        this.left_child_pgnum = leftChildPageNo;
    }
}
