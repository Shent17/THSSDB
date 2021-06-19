package cn.edu.thssdb.exception;

public class GrammarException extends RuntimeException{
    private String msg;
    public GrammarException(String msg){this.msg = msg;}

    @Override
    public String getMessage(){
        return msg;
    }
}
