package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.GrammarException;
import cn.edu.thssdb.exception.NullPointerException;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.transaction.Session;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.LinkedList;

public class StatementInsert extends AbstractStatement{
    private String table_name;
    public LinkedList values;
    private LinkedList<String> col;
    private Session session;

    public StatementInsert(String table_name, LinkedList value) {
        this.table_name = table_name;
        this.values = value;
    }

    public StatementInsert(String table_name, LinkedList<String> col, LinkedList value){
        this.table_name = table_name;
        this.col = col;
        this.values = value;
    }


    //执行一行插入
    @Override
    public ExecResult exec(Database db)
            throws IOException {
        if(db == null)
            throw new NullPointerException(NullPointerException.Database);

        Table targetTable = db.getTable(table_name);
        if (this.col != null) {
            LinkedList valueList = new LinkedList<>();
            ArrayList<String> colNames = targetTable.getColNames();

            for(int i=0 ; i<colNames.size(); i++)
                valueList.add(null);

            for (int j = 0; j < this.col.size(); ++j) {
                String attr = this.col.get(j);
                int idx = colNames.indexOf(attr);
                if (idx >= 0) {
                    valueList.set(idx, values.get(j));
                } else {
                    throw new GrammarException("Unknown column name '" + attr + "'!");
                }
            }
            values = valueList;
        }


        int succeed = 0;

        targetTable.insert(values);
        succeed += 1;

//        LinkedList val = new LinkedList();
//        val.add(succeed);
//        execResult.insert(val);

        LinkedList<LinkedList> insertRow = new LinkedList<>();
        insertRow.add(values);

        return new ExecResult("insert " + succeed + " rows!", 1, null, insertRow);
    }

    public String gettable_name(){return this.table_name;}
}
