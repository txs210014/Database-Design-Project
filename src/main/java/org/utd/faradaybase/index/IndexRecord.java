package org.utd.faradaybase.index;

import lombok.Data;
import org.utd.faradaybase.attribute.Attribute;
import org.utd.faradaybase.util.Util;
import org.utd.faradaybase.DataType;

import java.util.List;

@Data
public class IndexRecord {
    private Byte rowidsCount;
    private DataType dataType;
    private Byte[] indexValue;
    private List<Integer> rowIds;
    private short pgHeadIndex;
    private short pgOffset;
    private int leftPgno;
    private int rightPgno;
    private int pgNum;
    private IndexNode indexNode;

    public IndexRecord(short pgHeadIndex, DataType dataType, Byte NoOfRowIds, byte[] index_value, List<Integer> rowIds,
                       int leftPgno, int rightPgno, int pgNum, short pgOffset) {

        this.pgOffset = pgOffset;
        this.pgHeadIndex = pgHeadIndex;
        this.rowidsCount = NoOfRowIds;
        this.dataType = dataType;
        this.indexValue = Util.byteToBytes(index_value);
        this.rowIds = rowIds;

        this.indexNode = new IndexNode(new Attribute(this.dataType, index_value), rowIds);
        this.leftPgno = leftPgno;
        this.rightPgno = rightPgno;
        this.pgNum = pgNum;
    }
}