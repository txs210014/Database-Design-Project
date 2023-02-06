package org.utd.faradaybase.columninfo;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.utd.faradaybase.DataType;
import org.utd.faradaybase.FaradayBaseApplication;

import java.io.File;

@Data
@NoArgsConstructor
public class ColumnInformation {
    private DataType dataType;
    private String colName;
    private boolean isUniqueCol;
    private boolean isNullCol;
    private Short ordinalPos;
    private boolean hasIndex;
    private String tableName;
    private boolean isKeyPrimary;

    public void set_key_asPrimary() {
        isKeyPrimary = true;
    }

    public ColumnInformation(String tableName, DataType datatype, String column_name, boolean is_Unique,
                             boolean is_Null, short ordinalPos) {
        this.dataType = datatype;
        this.colName = column_name;
        this.isUniqueCol = is_Unique;
        this.isNullCol = is_Null;
        this.ordinalPos = ordinalPos;
        this.tableName = tableName;

        this.hasIndex = (new File(FaradayBaseApplication.getNDXFilePath(tableName, column_name)).exists());

    }
}