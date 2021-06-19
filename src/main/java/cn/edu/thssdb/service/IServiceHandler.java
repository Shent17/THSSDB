package cn.edu.thssdb.service;

import cn.edu.thssdb.exception.NDException;
import cn.edu.thssdb.parser.MyVisitor;
import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.query.AbstractStatement;
import cn.edu.thssdb.query.ExecResult;
import cn.edu.thssdb.query.StatementDatabase;
import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.server.ThssDB;
import cn.edu.thssdb.transaction.Session;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.query.*;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.thrift.TException;

import java.nio.charset.Charset;
import java.util.*;

public class IServiceHandler implements IService.Iface {

  Database database;
  ThssDB server = ThssDB.getInstance();

  @Override
  public GetTimeResp getTime(GetTimeReq req) throws TException {
    GetTimeResp resp = new GetTimeResp();
    resp.setTime(new Date().toString());
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public ConnectResp connect(ConnectReq req) throws TException {
    // TODO
    ConnectResp resp = new ConnectResp();
    resp.setSessionId(Global.SessionID);
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public DisconnectResp disconnect(DisconnectReq req) throws TException {
    // TODO
    server.quit();
    DisconnectResp resp = new DisconnectResp();
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public synchronized ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
    if (req.getSessionId() != Global.SessionID)
      throw new NDException("Please first connect to database!");

    //TODO: 处理传来的字符串 req_text，执行完后得到 ExecResult，根据内容创建 ExecuteStatementResp对象返回
    String req_text = req.getStatement();
    System.out.println(req_text);

    ExecuteStatementResp resp = new ExecuteStatementResp();
    resp.setResultInfo(new LinkedList<>());
    //开始解析
    CharStream stream = CharStreams.fromString(req_text);
    SQLLexer lexer = new SQLLexer(stream);
    CommonTokenStream tokenStream = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokenStream);
    MyVisitor visitor = new MyVisitor();
    ArrayList res = (ArrayList) visitor.visit(parser.parse());


    //如果第一条语句是begin transaction
    try{
      if(res.get(0) instanceof StatementTransaction){
        boolean abort = true;
        if(res.get(res.size()-1) instanceof StatementTransaction){
          StatementTransaction begin = (StatementTransaction) res.get(0);
          StatementTransaction commit = (StatementTransaction) res.get(res.size()-1);

          //符合以begin开头 commit结尾
          if(begin.getType() == 1 && commit.getType() == 2){
            abort = false;
            int count = res.size()-1;
            Random r = new Random();
            int sid = r.nextInt(100);

            Session session = new Session(database, sid);

            //将中间语句逐条加入session中
            for(int i=1; i<count; i++){
              Object cs = res.get(i);
              if(cs instanceof StatementInsert)
                session.AddInsert((StatementInsert)cs);
              else if(cs instanceof StatementDelete)
                session.AddDelete((StatementDelete) cs);
              else if(cs instanceof StatementUpdate)
                session.AddUpdate((StatementUpdate)cs);
              else if(cs instanceof StatementSelect)
                session.AddSelect((StatementSelect)cs);
              else{
                abort = true;
                break;
              }
            }

            //语句有效，开始执行
            if(!abort){
              server.execTransaction(session);

              //若事务abort，返回错误信息
              if(session.isAbort){
                resp.setStatus(new Status(Global.FAILURE_CODE));
                resp.setIsAbort(true);
                resp.setErrorInfo("Transaction executes fail!");
                return resp;
              }
              //若事务执行成功，返回结果
              else{
                resp.getResultInfo().add("Transaction executes successfully!");
                if(session.result != null){
                  //System.out.println("Transaction has select!");
                  resp.setHasResult(true);
                  resp.setColumnsList(session.result.getColNames());
                  resp.setRowList(session.result.getDataListAsList());
                }

                resp.setStatus(new Status(Global.SUCCESS_CODE));
                return resp;
              }
            }
          }
        }

        //事务语句解析错误
        if(abort){
          resp.setStatus(new Status(Global.FAILURE_CODE));
          resp.setIsAbort(true);
          resp.setErrorInfo("Invalid Transaction Statement!");
          return resp;
        }
      }
    }
    catch (Exception e){
      resp.setStatus(new Status(Global.FAILURE_CODE));
      resp.setIsAbort(true);
      resp.setErrorInfo(e.toString());
      return resp;
    }

    for (Object statement : res) {
      try {
        //如果是操作数据库的语句：直接送到ThssDB里执行
        if (statement instanceof StatementDatabase) {
          switch (((StatementDatabase) statement).type) {
            case 1:
              database = server.createDatabase(((StatementDatabase) statement).db);
              break;
            case 2:
              database = server.switchDatabase(((StatementDatabase) statement).db);
              break;
            case 3:
              server.deleteDatabase(((StatementDatabase) statement).db);
              break;
            default:
              break;
          }
        }
        else {
          ExecResult result = ((AbstractStatement) statement).exec(database);
          //如果有要返回的结果
          //select
          if(statement instanceof StatementSelect) {
            resp.setHasResult(true);
            resp.setColumnsList(result.getColNames());
            resp.setRowList(result.getDataListAsList());
          }
          //show table: 把列名的数据类型作为datalist传回来
          else if(statement instanceof StatementShowTable) {
            resp.setHasResult(true);
            resp.setColumnsList(result.getColNames());
            resp.setRowList(result.getTypeListAsList());
          }
          else {
            //如果不是select：只把msg添加进去
            resp.getResultInfo().add(result.getMsg());
          }
        }
        resp.setStatus(new Status(Global.SUCCESS_CODE));

      } catch (Exception e) {
        resp.setStatus(new Status(Global.FAILURE_CODE));
        resp.setIsAbort(true);
        resp.setErrorInfo(e.toString());
        break;
      }
    }

    return resp;
  }
}
