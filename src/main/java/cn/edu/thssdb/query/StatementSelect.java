package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.NDException;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.schema.TempTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;

public class StatementSelect extends AbstractStatement{
    private LinkedList<String> targetList;
    private RangeVariable rv;
    private Conditions cond;
    boolean distinct;

    public StatementSelect(LinkedList<String> targetList,
                           RangeVariable rv,
                           Conditions cond,
                           boolean distinct) {
        this.targetList = targetList;
        this.rv = rv;
        this.cond = cond;
        this.distinct = distinct;
    }

    @Override
    public ExecResult exec(Database db) throws IOException, NDException {
        if (db == null) throw new NDException("not using any database");
        ExecResult res;
        Object t = rv.exec(db);
        ArrayList<Table> tableList = rv.getTableList();
        //从单张表select
        if (t instanceof Table) {
            Table table = (Table) t;
            LinkedList<String> colNames = new LinkedList<>(table.getColNames());
//            LinkedList<Type> colTypes = new LinkedList<>(table.getColTypes());
            if (!targetList.isEmpty()) {
                if (targetList.getFirst().compareTo("*") == 0)
                    res = new ExecResult(colNames);
                else
                    res = new ExecResult(targetList);
            } else
            {
                res = new ExecResult();
            }
            colNames = table.combineTableColumn();
            tableList = new ArrayList<>();
            tableList.add(table);
            if (cond != null) {
                cond.normalize(tableList);
            }
            ArrayList<Entry> rowList = table.search(cond);
            for (Entry row: rowList) {
                LinkedList data = table.getRowAsList(row);

                if (!targetList.isEmpty()) {
                    if (targetList.getFirst().compareTo("*") == 0) {
                        res.insert(data, distinct);
                    } else
                    {
                        res.insert(colNames, data, tableList, null, distinct);
                    }
                } else
                {
                    res.insert(new LinkedList(), distinct);
                }
            }
//            table.close(false);
            return res;
        }
        //从多张表select
        else {
            TempTable tempTable = (TempTable) t;
            LinkedList<String> colNames = new LinkedList<>(tempTable.getColNames());
//            LinkedList<Type> colTypes = new LinkedList<>(tempTable.getColTypes());
            LinkedList<String> ignoreColumns;
            if (rv.getIgnoredColumns() == null) {
                ignoreColumns = new LinkedList<>();
            } else {
                ignoreColumns =new LinkedList<>(rv.getIgnoredColumns());
            }
            if (!targetList.isEmpty()) {
                if (targetList.getFirst().compareTo("*") == 0)
                    res = new ExecResult(colNames, ignoreColumns);
                else
                    res = new ExecResult(targetList);
            } else
            {
                res = new ExecResult();
            }

            if (cond != null) {
                cond.normalize(tableList, ignoreColumns);
            }
            ArrayList<Integer> rowNumList = tempTable.search(cond);
            for (Integer rowNum: rowNumList) {
                LinkedList data = tempTable.getRowAsList(rowNum);

                if (!targetList.isEmpty()) {
                    if (targetList.getFirst().compareTo("*") == 0) {
                        res.insert(colNames, data, ignoreColumns, distinct);
                    } else
                    {
                        res.insert(colNames, data, tableList, ignoreColumns, distinct);
                    }
                } else
                {
                    res.insert(new LinkedList(), distinct);
                }
            }
            tempTable.close();
            return res;
        }
    }

    public LinkedList<String> getTargetList(){
        /*
        LinkedList<String> tname = new LinkedList<>();
        for(Table t: rv.getTableList()){
            tname.add(t.getTableName());
        }
        return tname;*/
        return targetList;
    }
}