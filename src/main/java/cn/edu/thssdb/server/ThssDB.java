package cn.edu.thssdb.server;

import cn.edu.thssdb.query.ExecResult;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.exception.NullPointerException;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.service.IServiceHandler;
import cn.edu.thssdb.transaction.Session;
import cn.edu.thssdb.transaction.TransactionManager2PL;
import cn.edu.thssdb.utils.Global;
import com.sun.corba.se.spi.orbutil.threadpool.ThreadPoolManager;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.crypto.Data;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.channels.Selector;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThssDB {

  private static final Logger logger = LoggerFactory.getLogger(ThssDB.class);

  private static IServiceHandler handler;
  private static IService.Processor processor;
  private static TServerSocket transport;
  private static TNonblockingServerTransport non_transport;
  private static TServer server;

  private Manager manager;
  private TransactionManager2PL transactionManager;
  private Database database;

  public static ThssDB getInstance() {
    return ThssDBHolder.INSTANCE;
  }

  private ThssDB() {
    manager = new Manager();
  }

  public static void main(String[] args) {
    ThssDB server = ThssDB.getInstance();
//    Manager manager = new Manager();
    server.start();
  }

  private void start() {
    handler = new IServiceHandler();
    processor = new IService.Processor(handler);
    Runnable setup = () -> setUp(processor);
    new Thread(setup).start();
    recovery();
  }

  private static void setUp(IService.Processor processor) {

    try {
      //transport = new TServerSocket(Global.DEFAULT_SERVER_PORT);
      //server = new TSimpleServer(new TServer.Args(transport).processor(processor));

      //将 TSimpleServer 替换为 TThreadedSelectorServer

/*
      TServerSocket serverSocket=new TServerSocket(Global.DEFAULT_SERVER_PORT);
      TThreadPoolServer.Args serverParams=new TThreadPoolServer.Args(serverSocket);
      //ThreadPoolManager tmanager = new
      serverParams.protocolFactory(new TBinaryProtocol.Factory());
      serverParams.processor(processor);
      TServer server=new TThreadPoolServer(serverParams); //简单的单线程服务模型，常用于测试
*/
/*
      TNonblockingServerTransport serverSocket=new TNonblockingServerSocket(Global.DEFAULT_SERVER_PORT);
      TNonblockingServer.Args serverParams=new TNonblockingServer.Args(serverSocket);
      serverParams.protocolFactory(new TBinaryProtocol.Factory());
      serverParams.transportFactory(new TFramedTransport.Factory()); //非阻塞
      serverParams.processor(processor);
      TServer server=new TNonblockingServer(serverParams); //简单的单线程服务模型，常用于测试
*/


      TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(Global.DEFAULT_SERVER_PORT);
      TThreadedSelectorServer.Args args = new TThreadedSelectorServer.Args(serverTransport);
      args.processor(processor);
      args.protocolFactory(new TCompactProtocol.Factory());
      args.transportFactory(new TFramedTransport.Factory());
      args.processorFactory(new TProcessorFactory(processor));
      args.selectorThreads(2);

      ExecutorService pool = Executors.newFixedThreadPool(3);

      args.executorService(pool);

      TThreadedSelectorServer server = new TThreadedSelectorServer(args);


      logger.info("Starting ThssDB ...");
      server.serve();
    } catch (TTransportException e) {
      logger.error(e.getMessage());
    }
  }

  public Database createDatabase(String name) {
    database = manager.createDatabaseIfNotExists(name);
    return database;
  }

  public Database switchDatabase(String name){
    database = manager.switchDatabase(name);
    return database;
  }

  public void deleteDatabase(String name){
    manager.deleteDatabase(name);
  }

  public Database getDatabase() {
    return database;
  }

  public synchronized void execTransaction(Session session){
    if(database == null){
      throw new NullPointerException(NullPointerException.Database);
    }

    if(transactionManager == null){
      transactionManager = new TransactionManager2PL(database, 2);
    }

    try{
      System.out.println("session" + session.getID() + " go to transaction");
      transactionManager.beginTransaction(session);
    }
    catch (Exception e){

    }

    while(!session.done);

  }

  public void quit(){
    manager.persist();
  }

  private void recovery(){
    File log = new File("log/");
    File[] logfiles = log.listFiles();
    for(File f: logfiles){
      try{
        BufferedReader reader = new BufferedReader(new FileReader(f));
        String str;
        while((str=reader.readLine())!=null) {
          String buf[] = str.split(" ");
          String table_name = buf[0];
          String op = buf[1];
          int count = buf.length;
          LinkedList row = new LinkedList();
          for(int i=2; i<count; i++){
            row.add(buf[i]);
          }

          if(op.equals("insert")){
            database.getTable(table_name).insert(row);
          }
          else if(op.equals("delete")){
            Table t = database.getTable(table_name);
            t.delete(t.getKey(row));
          }
        }
      }
      catch (Exception e){}
    }
  }

  private static class ThssDBHolder {
    private static final ThssDB INSTANCE = new ThssDB();
    private ThssDBHolder() {

    }
  }
}
