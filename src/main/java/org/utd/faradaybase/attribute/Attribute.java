package org.utd.faradaybase.attribute;

import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.utd.faradaybase.constants.Constants;
import org.utd.faradaybase.util.Util;
import org.utd.faradaybase.DataType;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;

@Data
@Log4j2
public class Attribute {
    private byte[] fieldValueByte;
    private Byte[] fieldValueByteObject;
    private DataType dataType;
    private String fieldValue;

    public Attribute(DataType dataType, byte[] fieldValue) {
        this.dataType = dataType;
        this.fieldValueByte = fieldValue;
        try {
            //Convert the byte array into string
            switch (dataType) {
                case NULL:
                    this.fieldValue = Constants.NULL_STR;
                    break;
                case TINYINT:
                    this.fieldValue = Byte.valueOf(Util.byte_from_Byte_Array(fieldValueByte)).toString();
                    break;
                case SMALLINT:
                    this.fieldValue = Short.valueOf(Util.short_from_Byte_Array(fieldValueByte)).toString();
                    break;
                case INT:
                    this.fieldValue = Integer.valueOf(Util.int_from_Byte_Array(fieldValueByte)).toString();
                    break;
                case BIGINT:
                    this.fieldValue = Long.valueOf(Util.long_from_Byte_Array(fieldValueByte)).toString();
                    break;
                case FLOAT:
                    this.fieldValue = Float.valueOf(Util.float_from_Byte_Array(fieldValueByte)).toString();
                    break;
                case DOUBLE:
                    this.fieldValue = Double.valueOf(Util.double_from_Byte_Array(fieldValueByte)).toString();
                    break;
                case YEAR:
                    this.fieldValue = Integer.valueOf((int) Byte.valueOf(Util.byte_from_Byte_Array(fieldValueByte)) + 2000).toString();
                    break;
                case TIME:
                    int millisec_after_midnight = Util.int_from_Byte_Array(fieldValueByte) % 86400000;
                    int seconds = millisec_after_midnight / 1000;
                    int hours = seconds / 3600;
                    int rem_hour_seconds = seconds % 3600;
                    int minutes = rem_hour_seconds / 60;
                    int remSeconds = rem_hour_seconds % 60;
                    this.fieldValue = String.format("%02d", hours) + ":" + String.format("%02d", minutes) + ":" + String.format("%02d", remSeconds);
                    break;
                case DATETIME:
                    Date raw_date_time = new Date(Long.valueOf(Util.long_from_Byte_Array(fieldValueByte)));
                    this.fieldValue = String.format("%02d", raw_date_time.getYear() + 1900) + "-" + String.format("%02d", raw_date_time.getMonth() + 1)
                            + "-" + String.format("%02d", raw_date_time.getDate()) + "_" + String.format("%02d", raw_date_time.getHours()) + ":"
                            + String.format("%02d", raw_date_time.getMinutes()) + ":" + String.format("%02d", raw_date_time.getSeconds());
                    break;
                case DATE:
                    // YYYY-MM-DD
                    Date rawdate = new Date(Long.valueOf(Util.long_from_Byte_Array(fieldValueByte)));
                    this.fieldValue = String.format("%02d", rawdate.getYear() + 1900) + "-" + String.format("%02d", rawdate.getMonth() + 1)
                            + "-" + String.format("%02d", rawdate.getDate());
                    break;
                case TEXT:
                    this.fieldValue = new String(fieldValueByte, "UTF-8");
                    break;
                default:
                    this.fieldValue = new String(fieldValueByte, "UTF-8");
                    break;
            }
            this.fieldValueByteObject = Util.byteToBytes(fieldValueByte);
        } catch (Exception ex) {
            System.out.println("Incorrect Format!!");
        }
    }

    public Attribute(DataType dataType, String fieldValue) throws Exception {
        this.dataType = dataType;
        this.fieldValue = fieldValue;

        try {
            switch (dataType) {
                case NULL:
                    this.fieldValueByte = null;
                    break;
                case YEAR:
                    this.fieldValueByte = new byte[]{(byte) (Integer.parseInt(fieldValue) - 2000)};
                    break;
                case TIME:
                    this.fieldValueByte = Util.int_to_bytes(Integer.parseInt(fieldValue));
                    break;
                case DATETIME:
                    SimpleDateFormat sdftime = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
                    Date datetime = sdftime.parse(fieldValue);
                    this.fieldValueByte = Util.long_to_bytes(datetime.getTime());
                    break;
                case DATE:
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    Date date = sdf.parse(fieldValue);
                    this.fieldValueByte = Util.long_to_bytes(date.getTime());
                    break;
                case TEXT:
                    this.fieldValueByte = fieldValue.getBytes();
                    break;
                case TINYINT:
                    this.fieldValueByte = new byte[]{Byte.parseByte(fieldValue)};
                    break;
                case SMALLINT:
                    this.fieldValueByte = Util.short_to_bytes(Short.parseShort(fieldValue));
                    break;
                case INT:
                    this.fieldValueByte = Util.int_to_bytes(Integer.parseInt(fieldValue));
                    break;
                case BIGINT:
                    this.fieldValueByte = Util.long_to_bytes(Long.parseLong(fieldValue));
                    break;
                case FLOAT:
                    this.fieldValueByte = Util.float_to_bytes(Float.parseFloat(fieldValue));
                    break;
                case DOUBLE:
                    this.fieldValueByte = Util.doubl_to_bytes(Double.parseDouble(fieldValue));
                    break;
                default:
                    this.fieldValueByte = fieldValue.getBytes(StandardCharsets.US_ASCII);
                    break;
            }
            this.fieldValueByteObject = Util.byteToBytes(fieldValueByte);
        } catch (Exception e) {
            log.error("Conversion " + fieldValue + " to " + dataType.toString() + " not permitted.");
            log.debug("ERROR:", e);
            throw e;
        }
    }
}