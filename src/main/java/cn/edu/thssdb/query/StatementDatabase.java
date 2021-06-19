package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.NDException;
import cn.edu.thssdb.schema.Database;

import java.io.IOException;

//数据库的创建、切换、删除语句。不在exec里执行，而是由客户端调用ThssDB的接口执行
public class StatementDatabase extends AbstractStatement {
    public String db;
    public int type;    //1创建，2切换，3删除

    public StatementDatabase(String dbname, int type) {
        this.db = dbname;
        this.type = type;
    }

    @Override
    public ExecResult exec(Database db) throws IOException, NDException {
        return null;
    }
}
