package cn.edu.thssdb.type;

import cn.edu.thssdb.exception.CustomizedException;
import cn.edu.thssdb.exception.InsertException;

import static jdk.nashorn.internal.runtime.JSType.isNumber;

public enum ColumnType {
  INT, LONG, FLOAT, DOUBLE, STRING;

  public static ColumnType fromStrToType(String type) {
    if(type.equals("INT"))
      return INT;
    else if(type.equals("LONG"))
      return LONG;
    else if(type.equals("FLOAT"))
      return FLOAT;
    else if(type.equals("DOUBLE"))
      return DOUBLE;
    else if(type.equals("STRING"))
      return STRING;
    else
      throw new InsertException(InsertException.TYPE_CONVERT_ERROR);
  }

  public static Object convert(Object obj, ColumnType type){
    String str = obj.toString();
    switch(type){
      case INT:
        return Integer.parseInt(str);
      case LONG:
        return Long.parseLong(str);
      case FLOAT:
        return Float.parseFloat(str);
      case DOUBLE:
        return Double.parseDouble(str);
      case STRING:
        return str;
      default:
        throw new InsertException(InsertException.NOT_BASE_TYPE);
    }
  }

  //根据计算的两数类型，得出结果的数据类型
  public static ColumnType lift(ColumnType t1, ColumnType t2) {
    if ((isNumber(t1) && !isNumber(t2)) || (!isNumber(t1) && isNumber(t2)))
      throw new CustomizedException("Type " + t1+ " & " + t2 + ": error.");

    if (!isNumber(t1) && !isNumber(t2))
      return STRING;
    if (t1 == DOUBLE || t2 == DOUBLE)
      return DOUBLE;
    if (t1 == FLOAT || t2 == FLOAT)
      return FLOAT;
    if (t1 == LONG || t2 == LONG)
      return LONG;
    return INT;
  }

  private static Boolean isNumber(ColumnType type){
    if(type==INT || type==DOUBLE || type==LONG || type==FLOAT)
      return true;

    return false;
  }

  /*比较两个对象的值
    compare obj1 to obj2, on Type "type"
    params:
        obj1: first value
        obj2: second value
        type: Type
    return:
        0: equal
        -1: obj1 < obj2
        1: obj1 > obj2
        2: obj1 or obj2 is null
 */
  public static int compare(Object obj1, Object obj2, ColumnType type) {
    if (obj1 == null || obj2 == null) {
      return 2;
    }
    if (obj1.equals(obj2))
      return 0;
    switch (type) {
      case INT:
        return (int) obj1 < (int) obj2 ? -1 : 1;
      case LONG:
        return (long) obj1 < (long) obj2 ? -1 : 1;
      case FLOAT:
        return (float) obj1 < (float) obj2 ? -1 : 1;
      case DOUBLE:
        return (double) obj1 < (double) obj2 ? -1 : 1;
      default:
        return obj1.toString().compareTo(obj2.toString()) < 0 ? -1 : 1;
    }
  }
}
