package cn.edu.thssdb.utils;

import cn.edu.thssdb.exception.InsertException;
import cn.edu.thssdb.type.ColumnType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedList;

public class mNumber {
    /**
     * 把Object对象先转换成字符串然后转换成字节写入数组，Object是5种基本数据类型之一。
     * 当是String类型时，在前面填充6位整数，表示该字符串的长度，最大长度不超过999999（整数位数不足时前面补0）
     * 对于null的处理：字符串类型填充000000，其他类型不填充，只占长度
     * @param bytes 待写入内容的字符数组
     * @param pos 写入时的偏移量
     * @param value 待写入数组的Object
     * @param type value的数据类型
     * @return 写入的字节数
     */
    public static int toBytes(byte[] bytes, int pos, Object value, ColumnType type)
        throws InsertException, IOException {
        byte[] tmp; //存储字节数组的中间变量
        int pos_after_write; //此次写操作之后，需要在字节数组中偏移的量
        if (value == null) {
            if (type == ColumnType.STRING) {
                tmp = "000000".getBytes();
                System.arraycopy(tmp, 0, bytes, pos, tmp.length);
                return tmp.length;
            }
            else
                return byteOfType(type);
        }
        else {
            switch (type) {
                case INT:
                    tmp = Integer.toString((int) value).getBytes();
                    pos_after_write = byteOfType(ColumnType.INT);
                    break;
                case LONG:
                    tmp = Long.toString((long) value).getBytes();
                    pos_after_write = byteOfType(ColumnType.LONG);
                    break;
                case FLOAT:
                    tmp = Float.toString((float) value).getBytes();
                    pos_after_write = byteOfType(ColumnType.FLOAT);
                    break;
                case DOUBLE:
                    tmp = Double.toString((double) value).getBytes();
                    pos_after_write = byteOfType(ColumnType.DOUBLE);
                    break;
                case STRING:
                    //str_len是6位字节数组，保存字符串转成字节数组之后的长度
                    String length = String.valueOf(value.toString().getBytes().length);
                    byte[] str_len = new byte[6];
                    ByteArrayInputStream in = new ByteArrayInputStream(length.getBytes());
                    in.read(str_len);

                    //把字符串长度str_len和字符串本身value拼接到tmp里
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    out.write(str_len);
                    out.write(value.toString().getBytes());
                    tmp = out.toByteArray();
                    pos_after_write = tmp.length;
                    break;
                default:
                    throw new InsertException(InsertException.TYPE_MATCH_ERROR);

            }
            System.arraycopy(tmp, 0, bytes, pos, tmp.length);
            return pos_after_write;
        }
    }

    /**
     * 从字节数组中恢复一个Object类型的字段，Object是5种基本数据类型之一
     * @param list 空的记录存储空间
     * @param buffer 源字节数组
     * @param pos 偏移量，指定从哪里开始读
     * @param type 字段数据类型
     * @return 从字符数组中读了多少字节
     * @throws InsertException
     */
    public static int fromBytes(LinkedList list, byte[] buffer, int pos, ColumnType type)
            throws InsertException {
        int len = 0;    //要从buffer读出来的字节数
        String value;   //从buffer读出来的字节数组对应的字符串值
        byte[] tmp;
        switch (type) {
            case INT:
                len = byteOfType(ColumnType.INT);
                //从buffer中读出固定长度的字节
                tmp = new byte[len];
                System.arraycopy(buffer, pos, tmp, 0, len);
                //把字节数组变成字符串。trim()是去掉字符串首尾的空白符
                value = new String(tmp).trim();
                //说明该字段是null
                if(value.equals(""))
                    list.add(null);
                    //否则转为整数.
                else
                    list.add(Integer.parseInt(value));
                break;
            case LONG:
                len = byteOfType(ColumnType.LONG);
                tmp = new byte[len];
                System.arraycopy(buffer, pos, tmp, 0, len);
                value = new String(tmp).trim();
                if(value.equals(""))
                    list.add(null);
                else
                    list.add(Long.parseLong(value));
                break;
            case FLOAT:
                len = byteOfType(ColumnType.FLOAT);
                tmp = new byte[len];
                System.arraycopy(buffer, pos, tmp, 0, len);
                value = new String(tmp).trim();
                if(value.equals(""))
                    list.add(null);
                else
                    list.add(Float.parseFloat(value));
                break;
            case DOUBLE:
                len = byteOfType(ColumnType.DOUBLE);
                tmp = new byte[len];
                System.arraycopy(buffer, pos, tmp, 0, len);
                value = new String(tmp).trim();
                if(value.equals(""))
                    list.add(null);
                else
                    list.add(Double.parseDouble(value));
                break;
            case STRING:
                byte[] tmp_strLength = new byte[6];
                System.arraycopy(buffer, pos, tmp_strLength, 0, 6);
                int strByteLength = Integer.parseInt(new String(tmp_strLength).trim());
                if(strByteLength == 0) {
                    list.add(null);
                }
                else {
                    byte[] strBytes = new byte[strByteLength];
                    System.arraycopy(buffer, pos+6, strBytes, 0, strByteLength);
                    String str = new String(strBytes);
                    list.add(str);
                }
                len = tmp_strLength.length + strByteLength;
                break;
            default:
                break;
        }
        return len;
    }

    /**
     * 获得5种基本数据类型存储在文件中时所占的字节数。对于数字而言，每一个字节用于表示一个十进制整数或小数点。
     * @param type ColumnType类型
     * @return 字节数
     */
    public static int byteOfType(ColumnType type) {
        int typeByte = 0;
        switch (type) {
            case INT:
                typeByte = 16;
                break;
            case LONG:
                typeByte = 24;
                break;
            case FLOAT:
                typeByte = 32;
                break;
            case DOUBLE:
                typeByte = 64;
                break;
            case STRING:
                typeByte = 128;
                break;
            default:
                break;
        }
        return typeByte;
    }
}
