package cn.edu.thssdb.exception;

public class FileException extends RuntimeException {
    private int errorType;
    public static int NotExist = 1;
    public static int DeleteError = 2;
    public static int CreateError = 3;
    public static int OpenError = 4;
    public static int IOError = 5;

    public FileException(int type){errorType = type;}

    @Override
    public String getMessage() {
        String msg = "";
        if(errorType == NotExist)
            msg = "Exception: file does not exist!";
        else if(errorType == DeleteError)
            msg = "Exception: fail to delete!";
        else if(errorType == CreateError)
            msg = "Exception: fail to create!";
        else if(errorType == OpenError)
            msg = "Exception: fail to open the file!";
        else if(errorType == IOError)
            msg = "Exception: Error happens in inputstream or outputstream!";

        return msg;
    }
}
