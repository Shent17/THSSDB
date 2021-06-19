package cn.edu.thssdb.transaction;

import cn.edu.thssdb.schema.Row;

import java.util.LinkedList;

public class RowAction {
    private LinkedList<LinkedList> oldRow;
    private LinkedList<LinkedList> newRow;
    private String table_name;
    private int type;      //1 INSERT   2 DELETE   3 UPDATE

    public RowAction(String table_name, int type, LinkedList oldRow, LinkedList newRow){
        this.table_name = table_name;
        this.type = type;
        this.oldRow = oldRow;
        this.newRow = newRow;
    }

    public int getType(){return type;}

    public LinkedList<LinkedList> getOldRow(){return oldRow;}

    public LinkedList<LinkedList> getNewRow(){return newRow;}

    public String getTable_name(){return table_name;}
}
