package cn.edu.thssdb.exception;

/**
 * 语句不合法，在parse时抛出的异常
 */
public class StatementException extends RuntimeException {
    private int errortype;
    public static int LACK_OF_MAXLENGTH_ERROR = 1;
    public static int SELECT_SYNTAX_ERROR = 2;
    public static int OPERATION_TYPE_ERROR = 3;
    public static int LOGICAL_OPERATION_TYPE_ERROR = 4;

    public StatementException(int type) {
        errortype = type;
    }

    @Override
    public String getMessage() {
        String msg = "";
        if (errortype == LACK_OF_MAXLENGTH_ERROR)
            msg = "Exception: You must assign maxLength to String type explicitly!";
        else if(errortype == SELECT_SYNTAX_ERROR)
            msg = "Exception: Select statement syntax error!";
        else if(errortype == OPERATION_TYPE_ERROR)
            msg = "Exception: Unexpected operation. Mathematical operation should be one of the followings: " +
                    "=, <>, <, >, <=, >=.";
        else if(errortype == LOGICAL_OPERATION_TYPE_ERROR)
            msg = "Exception: Unexpected logical operation. Logical operation should be one of the followings: " +
                    "AND, OR.";
        return msg;

    }
}
