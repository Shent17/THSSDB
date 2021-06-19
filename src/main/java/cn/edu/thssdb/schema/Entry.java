package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.InsertException;
import cn.edu.thssdb.type.ColumnType;

import java.io.Serializable;

public class Entry implements Comparable<Entry>, Serializable {
  private static final long serialVersionUID = -5809782578272943999L;
  public Comparable value;

  public Entry(Comparable value) {
    this.value = value;
  }

  @Override
  public int compareTo(Entry e) {
    return value.compareTo(e.value);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null)
      return false;
    if (this.getClass() != obj.getClass())
      return false;
    Entry e = (Entry) obj;
    return value.equals(e.value);
  }

  public String toString() {
    return value.toString();
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }

  //把ori的数据类型转化为ColumnType中的一种
  public static Entry convertType(Object ori, ColumnType type) {
    if(ori == null) {
      return null;
    }
    Entry entry;
    String str = ori.toString();
    switch (type) {
      case INT:
        entry = new Entry(Integer.parseInt(str));
        break;
      case LONG:
        entry = new Entry(Long.parseLong(str));
        break;
      case FLOAT:
        entry = new Entry(Float.parseFloat(str));
        break;
      case DOUBLE:
        entry = new Entry(Double.parseDouble(str));
        break;
      case STRING:
        entry = new Entry(str);
        break;
      default:
        throw new InsertException(InsertException.TYPE_CONVERT_ERROR);
    }
    return entry;
  }
}
