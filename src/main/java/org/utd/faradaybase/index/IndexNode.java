package org.utd.faradaybase.index;

import lombok.Data;
import org.utd.faradaybase.attribute.Attribute;

import java.util.List;

@Data
public class IndexNode {
    private Attribute indexVal;
    private List<Integer> rowIds;
    private boolean isInteriorNode;
    private int leftPageNumber;

    public IndexNode(Attribute indexValue, List<Integer> rowids) {
        this.indexVal = indexValue;
        this.rowIds = rowids;
    }
}