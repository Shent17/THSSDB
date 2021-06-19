package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.NDException;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.transaction.Session;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.LinkedList;

public class StatementUpdate extends AbstractStatement{
    private String targetTableName;
    private LinkedList<String> colList;
    private LinkedList<Expression> exprList;
    private Conditions cond;
    private Session session;


    public StatementUpdate(String targetTableName,
                           LinkedList<String> colList,
                           LinkedList<Expression> exprList) {
        this(targetTableName, colList, exprList, null);
    }

    public StatementUpdate(String targetTableName,
                           LinkedList<String> colList,
                           LinkedList<Expression> exprList,
                           Conditions cond) {
        this.targetTableName = targetTableName;
        this.colList = colList;
        this.exprList = exprList;
        this.cond = cond;
    }

    /*
        execute update operation
        params:
            db: current database
        return:
            the number of updated rows
    */
    @Override
    public ExecResult exec(Database db) throws IOException, NDException {
        if (db == null) throw new NDException("not using any database");
        Table targetTable = db.getTable(this.targetTableName);
        ArrayList<Table> param = new ArrayList<>();
        param.add(targetTable);
        for (Expression expr: exprList) {
            expr.normalize(param);
        }
        //这里不需要正则化：无需ID->PERSON.ID
//        if (cond != null)
//            cond.normalize(param);
        ArrayList<Entry> toUpdate = targetTable.search(cond);

        LinkedList<LinkedList> oldvalue = new LinkedList<>();
        LinkedList<LinkedList> newvalue = new LinkedList<>();
        int succeed = 0;
        for (Entry row: toUpdate) {
            LinkedList oldrow = targetTable.getRowAsList(row);
            oldvalue.add(oldrow);

            LinkedList newrow = targetTable.update(row, colList, exprList);
            newvalue.add(newrow);
            succeed += 1;
        }
//        targetTable.close();

        return new ExecResult("update " + succeed + " records!", 3, oldvalue, newvalue);
    }

    public String gettable_name(){return this.targetTableName;}
}
