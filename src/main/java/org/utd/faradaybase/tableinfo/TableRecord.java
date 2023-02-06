package org.utd.faradaybase.tableinfo;

import lombok.Data;
import org.utd.faradaybase.util.Util;
import org.utd.faradaybase.DataType;
import org.utd.faradaybase.attribute.Attribute;

import java.util.List;

import java.util.Arrays;
import java.util.ArrayList;


@Data
public class TableRecord
{
    private int row_id;
    private Byte[] col_data_types;
    private Byte[] record_content;
    private List<Attribute> attributes;
    private short record_offset;
    private short pg_head_index;

    public List<Attribute> getAttributes()
    {
        return attributes;
    }

    private void setAttributes()
    {
        attributes = new ArrayList<>();
        int pointer = 0;
        for(Byte col_data_type : col_data_types)
        {
             byte[] fieldValue = Util.BytesToBytes(Arrays.copyOfRange(record_content,pointer, pointer + DataType.getLength(col_data_type)));
             attributes.add(new Attribute(DataType.get(col_data_type), fieldValue));
                    pointer =  pointer + DataType.getLength(col_data_type);
        }
    }

    public TableRecord(short pg_head_index,int row_id, short record_offset, byte[] col_data_types, byte[] record_content)
    {
        this.row_id = row_id;
        this.record_content= Util.byteToBytes(record_content);
        this.col_data_types = Util.byteToBytes(col_data_types);
        this.record_offset =  record_offset;
        this.pg_head_index = pg_head_index;
        setAttributes();
    }
    
}