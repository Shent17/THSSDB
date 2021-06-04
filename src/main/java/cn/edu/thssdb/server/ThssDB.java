package cn.edu.thssdb.server;

import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.service.IServiceHandler;
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ThssDB {

  private static final Logger logger = LoggerFactory.getLogger(ThssDB.class);

  private static IServiceHandler handler;
  private static IService.Processor processor;
  private static TServerSocket transport;
  private static TServer server;

  private static Manager manager;
  private Database database;
  private static long sessionNum;
  private static List<Long> sessionList;

  public static ThssDB getInstance() {
    return ThssDBHolder.INSTANCE;
  }

  public static void main(String[] args) {
    sessionNum = 0;
    sessionList = new ArrayList<>();
    manager = Manager.getInstance();
    ThssDB server = ThssDB.getInstance();
    server.start();
  }

//  public long addSession() {
//    sessionNum++;
//    System.out.println(sessionNum);
//    long sessionId = sessionNum;
//    sessionList.add(sessionId);
//    return sessionId;
//  }
//
//  public boolean deleteSession(long sessionId) {
//    return sessionList.remove(sessionId);
//  }
//
//  public boolean checkSession(long sessionId) {
//    return sessionList.contains(sessionId);
//  }

  private void start() {
    handler = new IServiceHandler();
    processor = new IService.Processor(handler);
    Runnable setup = () -> setUp(processor);
    new Thread(setup).start();
  }

  private static void setUp(IService.Processor processor) {
    try {
      transport = new TServerSocket(Global.DEFAULT_SERVER_PORT);
      server = new TSimpleServer(new TServer.Args(transport).processor(processor));
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

  public void quit(){
    manager.persist();
  }

  private static class ThssDBHolder {
    private static final ThssDB INSTANCE = new ThssDB();
    private ThssDBHolder() {

    }
  }
}
