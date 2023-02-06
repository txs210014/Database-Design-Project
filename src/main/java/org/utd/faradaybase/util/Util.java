package org.utd.faradaybase.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;


public class Util {
    public static Byte[] byteToBytes(final byte[] data) {
        int length = data == null ? 0 : data.length;
        Byte[] result = new Byte[length];
        for (int i = 0; i < length; i++)
            result[i] = data[i];
        return result;
    }

    public static Byte[] shortToBytes(final short data) {
        return byteToBytes(ByteBuffer.allocate(Short.BYTES).order(ByteOrder.BIG_ENDIAN).putShort(data).array());
    }

    public static byte[] BytesToBytes(final Byte[] data) {

        if (data == null) System.out.println("! Data is null");

        int length = data == null ? 0 : data.length;
        byte[] result = new byte[length];
        for (int i = 0; i < length; i++)
            result[i] = data[i];
        return result;
    }

    public static byte[] lst_to_byte_list(final List<Byte> lst) {
        return BytesToBytes(lst.toArray(new Byte[lst.size()]));
    }

    public static byte[] short_to_bytes(final short data) {
        return ByteBuffer.allocate(Short.BYTES).order(ByteOrder.BIG_ENDIAN).putShort(data).array();
    }

    public static Byte[] int_to_Bytes(final int data) {
        return byteToBytes(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(data).array());
    }

    public static byte[] int_to_bytes(final int data) {
        return ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(data).array();
    }

    public static byte[] long_to_bytes(final long data) {
        return ByteBuffer.allocate(Long.BYTES).putLong(data).array();
    }

    public static Byte[] long_to_Bytes(final long data) {
        return byteToBytes(ByteBuffer.allocate(Long.BYTES).putLong(data).array());
    }

    public static Byte[] float_to_Bytes(final float data) {
        return byteToBytes(ByteBuffer.allocate(Float.BYTES).putFloat(data).array());
    }

    public static byte[] float_to_bytes(final float data) {
        return (ByteBuffer.allocate(Float.BYTES).putFloat(data).array());
    }

    public static Byte[] double_to_Bytes(final double data) {
        return byteToBytes(ByteBuffer.allocate(Double.BYTES).putDouble(data).array());
    }

    public static byte[] doubl_to_bytes(final double data) {
        return (ByteBuffer.allocate(Double.BYTES).putDouble(data).array());
    }

    public static byte byte_from_Byte_Array(byte[] bytes) {
        return ByteBuffer.wrap(bytes).get();
    }

    public static short short_from_Byte_Array(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getShort();
    }

    public static int int_from_Byte_Array(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
    }

    public static long long_from_Byte_Array(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getLong();
    }

    public static float float_from_Byte_Array(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getFloat();
    }

    public static double double_from_Byte_Array(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getDouble();
    }
}