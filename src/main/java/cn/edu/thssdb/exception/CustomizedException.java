package cn.edu.thssdb.exception;

public class CustomizedException extends RuntimeException {
    private String msg;
    public CustomizedException(String msg){this.msg = msg;}

    @Override
    public String getMessage(){
        return msg;
    }
}
