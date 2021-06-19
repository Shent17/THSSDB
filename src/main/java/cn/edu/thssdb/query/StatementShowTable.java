package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.exception.NullPointerException;
import cn.edu.thssdb.type.ColumnType;

import java.util.ArrayList;
import java.util.LinkedList;

public class StatementShowTable extends AbstractStatement {
    private String table_name;

    public StatementShowTable(String table_name) {
        this.table_name = table_name;
    }

    @Override
    public ExecResult exec(Database db) {
        if (db == null)
            throw new NullPointerException(NullPointerException.Database);

        ArrayList<Column> cols = db.show(table_name);
        LinkedList<String> colName = new LinkedList<>();
        LinkedList<ColumnType> typeList = new LinkedList<>();
        for (Column col : cols) {
            colName.add(col.getName());
            typeList.add(col.getType());
        }

        return new ExecResult(colName, typeList, "Show all Columns");
    }
}