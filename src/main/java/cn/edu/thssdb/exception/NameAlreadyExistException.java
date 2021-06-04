package cn.edu.thssdb.exception;

public class NameAlreadyExistException extends RuntimeException {
    private int errorType;
    public static int TableName = 1;
    public static int DatabaseName = 2;

    public NameAlreadyExistException(int type){errorType = type;}

    @Override
    public String getMessage() {
        String msg = "";
        if(errorType == TableName)
            msg = "Exception: table name already exists!";
        else if(errorType == DatabaseName)
            msg = "Exception: database name already exists!";

        return msg;
    }
}
