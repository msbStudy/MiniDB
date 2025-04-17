package top.guoziyang.mydb.backend.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.google.common.primitives.Bytes;

public class Parser {

    public static byte[] short2Byte(short value) {
        return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(value).array();
    }

    public static short parseShort(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 2);
        return buffer.getShort();
    }

    public static byte[] int2Byte(int value) {
        return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();
    }

    public static int parseInt(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 4);
        return buffer.getInt();
    }


    /**
     * 将数组前八位转换成长整数
     * @param buf 需要转换的字节数组
     * @return 转换后的数据
     */
    public static long parseLong(byte[] buf) {
        // 使用ByteBuffer包装字节数组，并指定从位置0开始，长度为8的子区域
        // 这里明确指定了处理的字节范围，以确保正确解析长整型数字
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 8);
        // 从ByteBuffer中获取一个长整型数字
        // ByteBuffer的getLong方法会根据当前的字节序（默认为大端序）解析字节数组为一个长整型数字
        return buffer.getLong();
    }

    /**
     * 将长整型数值转换为字节数组
     * 此方法用于处理需要将长整型数据转换成字节流的场景，例如网络传输或存储
     *
     * @param value 需要转换的长整型数值
     * @return 转换后的字节数组
     */
    public static byte[] long2Byte(long value) {
        // 使用ByteBuffer进行长整型到字节数组的转换
        // ByteBuffer.allocate(Long.SIZE / Byte.SIZE) 创建一个足够大的字节数组来存储长整型值
        // .putLong(value) 将长整型值写入ByteBuffer
        // .array() 将ByteBuffer转换为字节数组
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }


    public static ParseStringRes parseString(byte[] raw) {
        int length = parseInt(Arrays.copyOf(raw, 4));
        String str = new String(Arrays.copyOfRange(raw, 4, 4 + length));
        return new ParseStringRes(str, length + 4);
    }

    public static byte[] string2Byte(String str) {
        byte[] l = int2Byte(str.length());
        return Bytes.concat(l, str.getBytes());
    }

    public static long str2Uid(String key) {
        long seed = 13331;
        long res = 0;
        for (byte b : key.getBytes()) {
            res = res * seed + (long) b;
        }
        return res;
    }

}
