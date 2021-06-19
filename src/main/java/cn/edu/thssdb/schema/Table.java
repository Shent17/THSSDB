package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.SearchException;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.exception.InsertException;
import cn.edu.thssdb.exception.NDException;
import cn.edu.thssdb.utils.mNumber;
import cn.edu.thssdb.query.Conditions;
import cn.edu.thssdb.query.Expression;
import javafx.util.Pair;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

//public class Table implements Iterable<Row> {
public class Table {
  ReentrantReadWriteLock lock;
  private String databaseName;
  public String tableName;
  public ArrayList<Column> columns;
  //B+树的非叶节点只保存树的索引结构，叶节点中保存每条记录的详细信息，
  //具体来说，叶子节点 的key保存记录的主键，value保存记录在文件中的索引。
  //value是一个ArrayList<Integer>类型的 变量，有两个元素，
  //第一个元素是该记录在文件中的起始位置，第二个元素是记录所占的字节数。
  public BPlusTree<Entry, ArrayList<Integer>> index;
  //一条记录可能的最大长度（字节）
  private int maxRowSize = 0;
  //指明下一条记录要写在文件中的起始位置
  public int nextRowStartPos = 0;
  private int primaryIndex;
  private RandomAccessFile dataFile;
  private ArrayList<String> colNames;
  private ArrayList<ColumnType> colTypes;

  //存储路径的前缀
  private String prefix = "data/";

  public ArrayList<ColumnType> getColTypes() { return this.colTypes; }

  public Table(String databaseName, String tableName, Column[] columns)
    throws IOException,ClassNotFoundException {
    // TODO
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columns = new ArrayList<Column>();
    this.colNames = new ArrayList<String>();
    this.colTypes = new ArrayList<>();
    Collections.addAll(this.columns, columns);

    //计算记录的最大长度
    maxRowSize = 0;
    for (Column col : columns) {
      maxRowSize = maxRowSize + mNumber.byteOfType(col.getType());
      colTypes.add(col.getType());
      colNames.add(col.getName());
    }

    //如果文件存在，从文件中恢复B+树
    File treeFile = new File(prefix + databaseName + "/" + tableName + ".tree");
    if (treeFile.exists())
      index = deserialize_tree(tableName);

    //如果数据文件不存在，则新建
    File f = new  File(prefix + databaseName + "/" + tableName + ".data");
    if(!f.exists()) {
      f.createNewFile();
    }

    dataFile = new RandomAccessFile(prefix + databaseName + "/" + tableName + ".data", "rw");
  }

  //反序列化B+树
  public BPlusTree<Entry, ArrayList<Integer>> deserialize_tree(String tableName)
        throws IOException, ClassNotFoundException{
    //创建一个ObjectInputStream输入流
    ObjectInputStream oIS = new ObjectInputStream(new FileInputStream(prefix + databaseName + "/" + tableName + ".tree"));
    databaseName = (String) oIS.readObject();
    tableName = (String) oIS.readObject();
    columns = (ArrayList<Column>) oIS.readObject();
    nextRowStartPos = (Integer) oIS.readObject();
    index = (BPlusTree<Entry, ArrayList<Integer>>) oIS.readObject();
    oIS.close();
    return index;
  }

  private void recover() {
    // TODO
  }

  public void insert(LinkedList values)
          throws InsertException, IOException{
    // TODO
    if(values == null)
      return;
    //对插入数据进行合法性检查
    legalCheck(values);

    //把values转换成Row
    Entry key = getKey(values);
    Row newRow = new Row();
    for (int i = 0; i < columns.size(); i++) {
      newRow.appendOneEntry(Entry.convertType(values.get(i), columns.get(i).getType()));
    }

    //将该条记录持久化存储，获得文件的存储位置
    ArrayList<Integer> storePos = serialize_row(newRow);
    //在B+树中插入新的节点
    index.put(key, storePos);
  }

  //序列化该条记录
  protected ArrayList<Integer> serialize_row(Row row)
          throws IOException {
    byte[] rowData = new byte[maxRowSize];
    Arrays.fill(rowData, (byte)0);

    int recordSize = 0;//该条记录所占的字节数
    int n = columns.size();
    for (int i = 0; i < n; i++) {
      recordSize = recordSize + mNumber.toBytes(rowData, recordSize, row.get(i), columns.get(i).getType());
    }

    this.dataFile.seek(nextRowStartPos);
    this.dataFile.write(rowData, 0, recordSize);

    //这条记录存储的位置
    ArrayList<Integer> storePos = new ArrayList<>(2);
    storePos.add(nextRowStartPos);
    storePos.add(recordSize);

    //文件末尾的指针向后移动一次
    nextRowStartPos += recordSize;
    return storePos;
  }

  //字段合法性检查
  private void legalCheck(LinkedList values) {
    //列数
    if (values.size() != columns.size())
      throw new InsertException(InsertException.COLUMN_LENGTH_MATCH_ERROR);

    //各列元素类型
    for (int i = 0; i < values.size(); i++) {
      Object value = values.get(i);
      if (value == null) {
        if(columns.get(i).canBeNull())
          continue;
        else
            throw new InsertException(InsertException.NOTNULL_COLUMN_ERROR);
      }
      boolean typeErr = false;
      switch (columns.get(i).getType()) {
        case INT:
          if (!(value instanceof Integer))
            typeErr = true;
          break;
        case LONG:
          if (!(value instanceof Long))
            typeErr = true;
          break;
        case FLOAT:
          if (!(value instanceof Float || value instanceof Double))
            typeErr = true;
          break;
        case DOUBLE:
          //如果输进来的是Float也可以接受
          if (!(value instanceof Double || value instanceof Float))
            typeErr = true;
          break;
        case STRING:
          if (!(value instanceof String))
            typeErr = true;
          break;
        default:
          break;
      }
      if (typeErr)
        throw new InsertException(InsertException.TYPE_MATCH_ERROR);
    }

    //检查主键是否重复
    Entry primaryKey = getKey(values);
    if (index.contains(primaryKey)) {
      throw new InsertException(InsertException.KEY_DUPLICATE_ERROR);
    }
  }

  //获得待操作数据的主键
  public Entry getKey(LinkedList values) {
    Entry key = null;
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).isPrimary()) {
        key = Entry.convertType(values.get(i), columns.get(i).getType());
        break;
      }
    }
    return key;
  }

  //获取所有数据
  public ArrayList<Row> getAllRows()
          throws SearchException, IOException{
    ArrayList<Entry> allKeys = index.getAllKeys();
    ArrayList<Row> allRows = new ArrayList<>();
    for(Entry key: allKeys) {
      allRows.add(getRowFromFile(key));
    }
    return allRows;

  }

  public void close()
          throws IOException {
    serialize_tree();
  }

  //在删除table之前执行的清理工作，如关闭文件等
  public void release()
          throws IOException{
    dataFile.close();
  }

  public void persist()
          throws IOException {
    serialize_tree();
  }

  public ArrayList<Column> getColumns(){
    return columns;
  }

  public String getTableName(){return tableName;}

  //返回列名字的list
  public ArrayList<String> getColNames(){
    ArrayList<String> colName = new ArrayList<>();
    for(Column col:columns){
      colName.add(col.getName());
    }
    return colName;
  }

  public LinkedList<String> combineTableColumn() {
    LinkedList<String> list = new LinkedList<>();
    for (String colName: this.colNames)
      list.add(this.tableName + "." + colName);
    return list;
  }

  public ArrayList<Entry> getAllRowsKey() throws SearchException, IOException {
    ArrayList<Entry> allKeys = index.getAllKeys();
    ArrayList<Entry> notnullKeys = new ArrayList<>();
    for(Entry key: allKeys) {
      if(key != null)
        notnullKeys.add(key);
    }
    return notnullKeys;
  }

  public ArrayList<Entry> search(Conditions cond) throws IOException, NDException {
    if (cond == null) return getAllRowsKey();
    ArrayList<Entry> res = new ArrayList<>();
    ArrayList<Entry> allRow = this.getAllRowsKey();
    for (Entry key: allRow) {
      if (cond.satisfied(new LinkedList<String>(this.colNames),
              new LinkedList<ColumnType>(this.colTypes),
              getRowAsList(key)))
        res.add(key);
    }
    return res;
  }

  //序列化B+树
  public void serialize_tree() throws IOException {
    //检查文件是否存在
    File treeFile = new File(prefix + databaseName + "/" + tableName + ".tree");
    if(!treeFile.exists()) {
      treeFile.createNewFile();
    }

    ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(prefix + databaseName + "/" + tableName + ".tree"));
    oos.writeObject(databaseName);
    oos.writeObject(tableName);
    oos.writeObject(columns);
    oos.writeObject((Integer)nextRowStartPos);
    oos.writeObject(index);
    oos.close();
  }

  //按主键从文件中获取一条记录（即对记录的查询）
  public Row getRowFromFile(Entry key)
          throws SearchException, IOException {
    ArrayList storePos = index.get(key);
    int offset = (int) storePos.get(0); //文件起始位置
    int length = (int) storePos.get(1); //读入的字节数（记录的长度）
    byte[] buffer = new byte[length];
    dataFile.seek(offset);
    dataFile.read(buffer, 0, length);

    LinkedList row = new LinkedList();
    int pos = 0;
    for (int i = 0; i < columns.size(); ++i) {
      pos += mNumber.fromBytes(row, buffer, pos, columns.get(i).getType());
    }
    //把row转换成Row类型
    Row newRow = new Row();
    for (int i = 0; i < columns.size(); i++) {
      newRow.appendOneEntry(Entry.convertType(row.get(i), columns.get(i).getType()));
    }

    return newRow;
  }

  public LinkedList getRowAsList(Entry key)
          throws SearchException, IOException {

    ArrayList storePos = index.get(key);
    int offset = (int) storePos.get(0);   //文件起始位置
    int length = (int) storePos.get(1);   //读入的字节数（记录长度）
    byte[] buffer = new byte[length];
    dataFile.seek(offset);
    dataFile.read(buffer, 0, length);

    LinkedList row = new LinkedList();

    int pos = 0;
    for (int i = 0; i < columns.size(); ++i) {
      pos += mNumber.fromBytes(row, buffer, pos, columns.get(i).getType());
    }
    return row;
  }

  public void delete(Entry key) {
    // TODO
    index.remove(key);
  }

  //对记录的改操作：一次删除加一次插入
  public LinkedList update(Entry key, LinkedList<String> colList, LinkedList<Expression> exprList)
          throws InsertException, IOException {
    // TODO
    LinkedList oldData = getRowAsList(key);
    LinkedList newData = new LinkedList(oldData);
    LinkedList<String> nameList = this.combineTableColumn();
    LinkedList<ColumnType> typeList = new LinkedList<ColumnType>(colTypes);
    int n = colList.size();
    for (int i = 0; i < n; ++i) {
      Pair<Object, ColumnType> val = exprList.get(i).calcValue(nameList, typeList, oldData);
      int idx = this.colNames.indexOf(colList.get(i));
      Object newVal = ColumnType.convert(val.getKey(), this.colTypes.get(idx));
      newData.set(idx, newVal);
    }

    delete(key);
    insert(newData);
    return newData;

  }


//  private class TableIterator implements Iterator<Row> {
//    private Iterator<Pair<Entry, Row>> iterator;
//
//    TableIterator(Table table) {
//      this.iterator = table.index.iterator();
//    }
//
//    @Override
//    public boolean hasNext() {
//      return iterator.hasNext();
//    }
//
//    @Override
//    public Row next() {
//      return iterator.next().right;
//    }
//  }
//
//  @Override
//  public Iterator<Row> iterator() {
//    return new TableIterator(this);
//  }
}
