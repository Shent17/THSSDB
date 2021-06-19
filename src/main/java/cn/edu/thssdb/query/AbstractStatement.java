package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.exception.NDException;
import java.io.IOException;

public abstract class AbstractStatement {
    abstract public ExecResult exec(Database db) throws IOException, NDException;
}
