package cn.edu.thssdb.exception;

public class InsertException extends RuntimeException {
    private int errortype;
    public static int COLUMN_LENGTH_MATCH_ERROR = 1;
    public static int TYPE_MATCH_ERROR = 2;
    public static int KEY_DUPLICATE_ERROR = 3;
    public static int TYPE_CONVERT_ERROR = 4;
    public static int NOTNULL_COLUMN_ERROR = 5;
    public static int NOT_BASE_TYPE = 6;

    public InsertException(int type) {
        errortype = type;
    }

    @Override
    public String getMessage() {
        String msg = "";
        if(errortype == COLUMN_LENGTH_MATCH_ERROR)
            msg =  "Exception: size of columns doesn't match!";
        else if(errortype == TYPE_MATCH_ERROR)
            msg = "Exception: data type doesn't match!";
        else if(errortype == KEY_DUPLICATE_ERROR)
            msg = "Exception: primary key already exists!";
        else if(errortype == TYPE_CONVERT_ERROR)
            msg = "Exception: unexpected type! Legal type: int,long,float,double,string.";
        else if(errortype == NOTNULL_COLUMN_ERROR)
            msg = "Exception: you must assign a value to a notnull column!";
        else if(errortype == NOT_BASE_TYPE)
            msg = "Exception: not base type!";
        return msg;
    }
}
