package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.InsertException;
import cn.edu.thssdb.exception.NDException;
import cn.edu.thssdb.exception.SearchException;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.query.Conditions;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.mNumber;
import javafx.util.Pair;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.*;

public class TempTable {

    private String fileName;
    private ArrayList<ColumnType> colTypes;
    private ArrayList<String> colNames;
    public LinkedList<LinkedList> values;
    private RandomAccessFile dataFile;

    //private static Logger logger = MyLogger.getLogger("temp_table");
    //private PersistenceData persistence;

// ------------------------------------ Interfaces ------------------------------------
    /**
     * create a new temp table object
     * parameter: cols definition
     */
    public TempTable(ArrayList<Pair<String, ColumnType>> cols)
            throws IOException, NDException {
        // generate a none taken name
//        this.fileName = generateFileName();
//        while (new File("tempdata/" + this.fileName + ".data").exists())
//            this.fileName = generateFileName();

        this.colTypes = new ArrayList<ColumnType>();
        this.colNames = new ArrayList<String>();

        HashSet<String> names = new HashSet<String>();
        for (Pair<String, ColumnType> pair : cols) {
            this.colNames.add(pair.getKey());
            names.add(pair.getKey());
            this.colTypes.add(pair.getValue());
        }
        if (cols.size() != names.size()) throw new NDException("input names have duplicate.");

//        // 创建文件：首先检查路径是否存在
//        File dir = new File("tempdata/");
//        if(!dir.exists())
//            dir.mkdirs();
//        File checkFile = new File("tempdata/" + this.fileName + ".tempdata");
//        if(!checkFile.exists())
//            checkFile.createNewFile();
//        dataFile = new RandomAccessFile("tempdata/" + fileName + ".tempdata", "rw");

        //this.persistence = new PersistenceData(this.fileName, this.colTypes, null);
        values = new LinkedList<>();
    }

    public void insert(LinkedList values) throws InsertException, IOException {

        this.values.add(values);

    }

    public ArrayList<Integer> search(Conditions cond) throws IOException, NDException {
        ArrayList<Integer> allRowNum = new ArrayList<>();
        for(int i = 0; i < this.values.size(); i++){
            allRowNum.add(i);
        }

        //如果没有选择条件：直接返回全部数据
        if(cond == null)
            return allRowNum;

        ArrayList<Integer> res = new ArrayList<>();
        for (Integer rowNum: allRowNum) {
            if (cond.satisfied(new LinkedList<String>(this.colNames),
                    new LinkedList<ColumnType>(this.colTypes),
                    getRowAsList(rowNum)))
                res.add(rowNum);
        }
        return res;
    }

    public LinkedList getRowAsList(Integer rowNum) {
        return values.get(rowNum);
    }


    /**
     * close a temp table, delete file
     */
    public void close() throws IOException, NDException {
//        dataFile.close();
//        File f = new File("tempdata/" + this.fileName + "tempdata");
//        if (!f.delete()) throw new NDException("temp table delete failed");
    }

//    public LinkedList getRowFromFile(Entry key)
//            throws SearchException, IOException {
//
//        ArrayList storePos = index.get(key);
//        int offset = (int) storePos.get(0);   //文件起始位置
//        int length = (int) storePos.get(1);   //读入的字节数（记录长度）
//        byte[] buffer = new byte[length];
//        dataFile.seek(offset);
//        dataFile.read(buffer, 0, length);
//
//        LinkedList row = new LinkedList();
//
//        int pos = 0;
//        for (int i = 0; i < columns.size(); ++i) {
//            pos += mNumber.fromBytes(row, buffer, pos, columns.get(i).getType());
//        }
//        //把row转换成Row类型
//        return row;
//    }
//
//
//    public ArrayList<Entry> getAllRowsKey() throws SearchException, IOException {
//        ArrayList<Entry> allKeys = index.getAllKeys();
//
//        return allKeys;
//    }
//
//    /**
//     * insert a row
//     * param: a linked list of given values
//     * return: the new row index
//     */
//    public void insert(LinkedList values) throws IOException, NDException {
//        if (values.size() != this.colTypes.size()) throw new NDException("row value number wrong");
//
//        // automatically pass type check
////        int i = 0;
////        for (Object val: values) {
////            // otherwise check
////            if (val == null) i++;
////            //else if (this.colTypes.get(i).check(val)) i++;
////            else throw new NDException("row values type check error!");
////        }
//
//        // type check pass
//        long rowNum = this.persistence.add(values);
//        return rowNum;
//    }
//
//    /**
//     * search
//     * param: conditions
//     * return: result of rows
//     */
//    public ArrayList<Entry> search(Conditions cond) throws IOException, NDException {
//        if (cond == null) return getAllRowsKey();
//        ArrayList<Entry> res = new ArrayList<>();
//        ArrayList<Entry> allRow = this.getAllRowsKey();
//        for (Entry key: allRow) {
//            if (cond.satisfied(new LinkedList<String>(this.colNames),
//                    new LinkedList<ColumnType>(this.colTypes),
//                    getRowFromFile(key)))
//                res.add(key);
//        }
//        return res;
//    }
//
//    /**
//     * delete
//     * param: row number
//     * return: no return value
//     */
//    public void delete(Entry key) throws IOException, NDException {
//        this.index.remove(key);
//    }
//
//
//    /**
//     * get single row number data
//     * param: row number
//     * return: an linked list of row data in line "row"
//     * @return
//     */
//    public LinkedList getSingleRowData(Entry key) throws NDException, IOException {
//        return new LinkedList(this.index.get(key));
//    }

    public ArrayList<String> getColNames() {
        return colNames;
    }

    public ArrayList<ColumnType> getColTypes() {
        return colTypes;
    }

    // ----------------------------- Private methods ---------------------------------------
    private static String generateFileName() {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
        Date now = new Date();
        Random rand = new Random();
        return "tmp_" + format.format(now) + Integer.toString(rand.nextInt(9999) + 1);
    }
}