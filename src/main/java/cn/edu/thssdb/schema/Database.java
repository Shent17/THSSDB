package cn.edu.thssdb.schema;


import cn.edu.thssdb.exception.FileException;
import cn.edu.thssdb.exception.NameAlreadyExistException;
import cn.edu.thssdb.exception.NameNotExistException;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database {

  private String name;
  private HashMap<String, Table> tables;
  ReentrantReadWriteLock lock;

  public Database(String name) {
    this.name = name;
    this.tables = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
    recover();
  }

  private void persist() {
    // TODO
    //持久化，将hashmap中的key写入db文件中
    File f = new File("data/" + name + "/" + name + ".db" );
    if(!f.exists()){
      try{
        boolean sign = f.createNewFile();
        if(!sign)
          throw new FileException(FileException.CreateError);
      }
      catch (Exception e){
        throw new FileException(FileException.CreateError);
      }
    }

    try {
      FileWriter writer = new FileWriter(f);
      Set<String> set = tables.keySet();
      for (String key : set) {
        tables.get(key).close();
        writer.write(key + "\n");
      }
      writer.close();
    }
    catch (Exception e){
      e.printStackTrace();
      throw new FileException(FileException.IOError);
    }
  }

  public void create(String table_name, Column[] columns) throws NameAlreadyExistException {
    // TODO
    //判断是否有重名，创建表，对应create语句
    if(this.tables.containsKey(table_name))
      throw new NameAlreadyExistException(NameAlreadyExistException.TableName);

    try
    {
      Table new_table = new Table(this.name, table_name, columns);
      this.tables.put(table_name, new_table);
      new_table.close();
    }
    catch (Exception e){
      throw new FileException(FileException.CreateError);
    }
  }

  public void drop(String table_name) throws IOException {
    // TODO
    //判断表名是否存在，删除表，对应drop语句
    if(!this.tables.containsKey(table_name))
      throw new NameNotExistException(NameNotExistException.TableName);

    tables.get(table_name).release();
    this.tables.remove(table_name);
    File f = new File("data/" + name + "/" + table_name + ".data");
    File f2 = new File("data/" + name + "/" + table_name + ".tree");
    if(f.exists()){
      boolean sign = f.delete();
      if(!sign)
        throw new FileException(FileException.DeleteError);
    }
    if(f2.exists()){
      boolean sign = f2.delete();
      if(!sign)
        throw new FileException(FileException.DeleteError);
    }
  }

  public String select(QueryTable[] queryTables) {
    // TODO
    QueryResult queryResult = new QueryResult(queryTables);
    return null;
  }

  private void recover() {
    // TODO
    //数据恢复，从db文件中读入所含表名
    File dir = new File("data/" + name + "/");

    if(!dir.exists())
      dir.mkdirs();

    File f = new File("data/" + name + "/" + name + ".db");
    if(!f.exists()){
      try{
        boolean sign = f.createNewFile();
        if(!sign)
          throw new FileException(FileException.CreateError);
      }
      catch (Exception e){
        throw new FileException(FileException.CreateError);
      }
    }
    else {
      try{
        BufferedReader reader = new BufferedReader(new FileReader(f));
        String str;
        tables.clear();
        while((str=reader.readLine())!=null) {
          System.out.println(str);
          Column[] columns = new Column[0];

          Table t = new Table(name, str, columns);
          tables.put(str, t);

        }
      }
      catch (Exception e){
        e.printStackTrace();
        throw new FileException(FileException.IOError);
      }
    }
  }

  public void quit() {
    // TODO
    persist();
  }
}
