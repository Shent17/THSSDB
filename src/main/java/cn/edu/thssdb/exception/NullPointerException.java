package cn.edu.thssdb.exception;

public class NullPointerException extends RuntimeException {
    private int errorType;
    public static int Table = 1;
    public static int Database = 2;
    public static int Column = 3;
    public static int Session = 4;

    public NullPointerException(int type){errorType = type;}

    @Override
    public String getMessage() {
        String msg = "";
        if(errorType == Database)
            msg = "Exception: Null database pointer!";
        if(errorType == Session)
            msg = "Exception: Null session pointer!";

        return msg;
    }
}
