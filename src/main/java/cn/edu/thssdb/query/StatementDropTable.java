package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.exception.NullPointerException;

import java.io.IOException;

public class StatementDropTable extends AbstractStatement {
    private String table_name;

    public StatementDropTable(String table_name) {
        this.table_name = table_name;
    }

    //execute drop table
    @Override
    public ExecResult exec(Database db)
            throws IOException {
        if (db == null)
            throw new NullPointerException(NullPointerException.Database);

        db.drop(table_name);

        return new ExecResult("Drop 1 table");
    }
}

