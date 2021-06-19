package cn.edu.thssdb.query;


import cn.edu.thssdb.exception.NameNotExistException;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.transaction.Session;
import cn.edu.thssdb.type.ColumnType;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ExecResult {
    private LinkedList<String> colNames;
    private LinkedList<ColumnType> typeList;
    private LinkedList<LinkedList> dataList;
    private LinkedList<LinkedList> oldValue;
    private LinkedList<LinkedList> newValue;
    private String msg;
    private int type;      //1: insert  2:delete   3:update
    private Session session;

    public ExecResult() {
        this.colNames = new LinkedList<>();
        this.dataList = new LinkedList<>();
    }

    //for  createTable  dropTable
    public ExecResult(String msg) {
        this.msg = msg;
    }

    //for  insert, update, delete
    //insert 传入newvalue为一行插入的值
    //delete 传入oldvalue为多行删除的值
    //updat  传入oldvalue为多行删除的值，传入newvalue为多行插入的值
    public ExecResult(String msg, int type, LinkedList<LinkedList> oldvalue, LinkedList<LinkedList> newvalue) throws IOException {
        this.type = type;
        this.msg = msg;
        this.oldValue = oldvalue;
        this.newValue = newvalue;
        /*
        Writer out =  new FileWriter(session.f);
        out.write(type);
        out.write(" ");
        out.write(String.valueOf(oldvalue));
        out.write(" ");
        out.write(String.valueOf(newvalue));
        out.write("\n");
        out.close();*/
    }

    //for  showTable
    public ExecResult(LinkedList<String> colNames, LinkedList<ColumnType> typeList, String msg) {
        this.colNames = new LinkedList<>(colNames);
        this.typeList = new LinkedList<>(typeList);
        this.msg = msg;
    }

    //for  select
    public ExecResult(LinkedList<String> colNames) {
        this.colNames = new LinkedList<>(colNames);
        this.dataList = new LinkedList<>();
    }

    //for  select
    public ExecResult(LinkedList<String> colNames, LinkedList<String> ignoreNames) {
        this.colNames = new LinkedList<>(colNames);
        for (String ignoreName : ignoreNames) {
            this.colNames.remove(ignoreName);
        }
        this.dataList = new LinkedList<>();
    }


    public void insert(LinkedList<String> allNames, LinkedList curData, LinkedList<String> ignoreNames, boolean distinct) throws IOException {
        LinkedList data = new LinkedList<>();
        int n = allNames.size();
        for (int i = 0; i < n; ++i) {
            if (ignoreNames.indexOf(allNames.get(i)) < 0)
                data.add(curData.get(i));
        }
        insert(data, distinct);

    }

    public void insert(LinkedList<String> curNames, LinkedList curData, ArrayList<Table> tableList, LinkedList<String> ignoreNames, boolean distinct) throws IOException {
        LinkedList data = new LinkedList();
        for (String colName : this.colNames) {
            Expression tmp = new Expression(1, colName);
            tmp.normalize(tableList, ignoreNames);
            String tableCol = tmp.getSymbol();
            int idx = curNames.indexOf(tableCol);
            if (idx < 0) {
                throw new NameNotExistException(NameNotExistException.ColumnName);
            }
            data.add(curData.get(idx));
        }
        insert(data, distinct);
    }

    public void insert(LinkedList data, boolean distinct) throws IOException {
        boolean is_duplicate = false;
        if (distinct) {
            for (LinkedList values : dataList) {
                is_duplicate = true;
                for (int i = 0; i < values.size(); i++) {
                    if (!values.get(i).equals(data.get(i))) {
                        is_duplicate = false;
                        break;
                    }
                }
                if (is_duplicate)
                    break;
            }
        }
        if (!is_duplicate) {
            dataList.add(data);
        }


    }

    public void show() {
        for (String colName : colNames) {
            System.out.printf("%15s ", colName);
        }
        System.out.println();
        for (LinkedList line : dataList) {
            for (Object o : line) {
                if (o == null) {
                    System.out.printf("           null ");
                } else
                    System.out.printf("%15s ", o.toString());
            }
            System.out.println();
        }
    }

    public String zipString() {
        StringBuffer buffer = new StringBuffer();
        int i = 0;
        int len = this.colNames.size();

        for (String name : this.colNames) {
            if (i == len - 1) buffer.append(name + "\n");
            else buffer.append(name + "|");
            i++;
        }

        for (LinkedList line : this.dataList) {
            i = 0;
            len = line.size();
            for (Object obj : line) {
                if (i == len - 1) {
                    if (obj == null) buffer.append("null\n");
                    else buffer.append(obj.toString() + "\n");
                } else {
                    if (obj == null) buffer.append("null|");
                    else buffer.append(obj.toString() + "|");
                }
                i++;
            }
        }
        return buffer.toString();
    }

    public LinkedList<String> getColNames() {
        return colNames;
    }

    public LinkedList<LinkedList> getDataList() {
        return dataList;
    }

    public List<List<String>>  getDataListAsList() {
        List<List<String>> lists = new LinkedList<>();
        for(int i = 0; i < dataList.size(); i++) {
            lists.add(new LinkedList<>());
            for(int j = 0; j < dataList.get(i).size(); j++) {
                Object value = dataList.get(i).get(j);
                if(value == null)
                    lists.get(i).add("null");
                else
                    lists.get(i).add(value.toString());
            }
        }
        return lists;
    }

    public List<List<String>> getTypeListAsList() {
        List<List<String>> lists = new LinkedList<>();
        //只有一行
        lists.add(new LinkedList<>());
        for (int i = 0; i < typeList.size(); i++) {
            Object value = typeList.get(i);
            lists.get(0).add(value.toString());
        }
        return lists;
    }

    public LinkedList<LinkedList> getOldValue() {
        return oldValue;
    }

    public LinkedList<LinkedList> getNewValue() {
        return newValue;
    }

    public String getMsg() {
        return msg;
    }
}
