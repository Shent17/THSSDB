package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.NameNotExistException;
import cn.edu.thssdb.schema.Column;
import java.util.ArrayList;
import java.util.List;

class MetaInfo {

  private String tableName;
  private List<Column> columns;

  MetaInfo(String tableName, ArrayList<Column> columns) {
    this.tableName = tableName;
    this.columns = columns;
  }

  int columnFind(String name) {
    // TODO
    int index = -1;
    int cursor = 0;
    for(Column col: columns){
      if(col.getName().equals(name)){
        index = cursor;
        break;
      }
      cursor++;
    }

    if(index == -1){
      throw new NameNotExistException(NameNotExistException.ColumnName);
    }
    else
      return index;
  }
}