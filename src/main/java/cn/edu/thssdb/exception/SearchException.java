package cn.edu.thssdb.exception;

public class SearchException extends RuntimeException {
    private int errortype;
    public static int ROW_NUM_ERROR = 1;

    public SearchException(int type) {
        errortype = type;
    }

    @Override
    public String getMessage() {
        String msg = "";
        if (errortype == ROW_NUM_ERROR)
            msg = "Exception: row number out of boundary!";
        return msg;

    }
}