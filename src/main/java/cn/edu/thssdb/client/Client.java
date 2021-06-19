package cn.edu.thssdb.client;

import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.utils.Global;
import org.antlr.v4.runtime.CharStreams;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.Name;
import javax.swing.*;
import java.awt.*;
import java.io.*;

import java.util.List;
import java.util.LinkedList;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Scanner;

public class Client {

  private static final Logger logger = LoggerFactory.getLogger(Client.class);

  static final String HOST_ARGS = "h";
  static final String HOST_NAME = "host";

  static final String HELP_ARGS = "help";
  static final String HELP_NAME = "help";

  static final String PORT_ARGS = "p";
  static final String PORT_NAME = "port";
  private long sid = 0;

  private static final PrintStream SCREEN_PRINTER = new PrintStream(System.out);
  private static final Scanner SCANNER = new Scanner(System.in);

  private static TTransport transport;
  private static TProtocol protocol;
  private static IService.Client client;
  private static CommandLine commandLine;

  public static void main(String[] args) {
    commandLine = parseCmd(args);
    if (commandLine.hasOption(HELP_ARGS)) {
      showHelp();
      return;
    }

    Client c = new Client();

    try {
      echoStarting();
      String host = commandLine.getOptionValue(HOST_ARGS, Global.DEFAULT_SERVER_HOST);
      int port = Integer.parseInt(commandLine.getOptionValue(PORT_ARGS, String.valueOf(Global.DEFAULT_SERVER_PORT)));
      System.out.println(host + " " + port);

      //transport = new TSocket(host, port);
      transport = new TFramedTransport(new TSocket(host, port));
      transport.open();
      //protocol = new TBinaryProtocol(transport);
      protocol = new TCompactProtocol(transport);

      client = new IService.Client(protocol);

      boolean open = true;
      while (true) {
        print(Global.CLI_PREFIX);
        String msg = SCANNER.nextLine();
        long startTime = System.currentTimeMillis();
        switch (msg.trim()) {
          case Global.SHOW_TIME:
            getTime();
            break;
          case Global.QUIT:
            open = false;
            break;
          case Global.CONNECT:
            c.getSessionID();
            break;
          case Global.DISCONNECT:
            c.cancelSessionID();
            break;
          // TODO: Delete this!
          // 为了调用测试语句
          case "fortest":
            c.test();
            break;
          default:
            //println("Invalid statements!");
            c.execStatement(msg);
            break;
        }
        long endTime = System.currentTimeMillis();
        println("It costs " + (endTime - startTime) + " ms.");
        if (!open) {
          break;
        }
      }
      transport.close();

    } catch (TTransportException e) {
      logger.error(e.getMessage());
    }
  }

  private static void getTime() {
    GetTimeReq req = new GetTimeReq();
    try {
      println(client.getTime(req).getTime());
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  private void getSessionID() {
    ConnectReq req = new ConnectReq("username", "password");
    try {
      ConnectResp resp = client.connect(req);
      if (resp.getStatus().code == Global.SUCCESS_CODE){
        sid = resp.getSessionId();
        println("Connect Successfully!");
      }
      else{
        println("Connection Fail!");
      }
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  private void cancelSessionID() {
    DisconnectReq req = new DisconnectReq(sid);
    try {
      DisconnectResp resp = client.disconnect(req);
      if (resp.getStatus().code == Global.SUCCESS_CODE){
        sid = 0;
        println("Disconnect Successfully!");
      }
      else{
        println("Disconnection Fail!");
      }
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  private void execStatement(String msg) {
    //TODO 根据ExecuteStatementResp的内容显示

    ExecuteStatementReq req = new ExecuteStatementReq(sid, msg);
//    System.out.println("send!");
    try {
      ExecuteStatementResp result = client.executeStatement(req);
      //如果有select数据，就显示
      if(result.isHasResult()) {
        int colSize = result.getColumnsListSize();
        int rowSize = result.getRowListSize();
        String[] Names = new String[colSize];
        for(int i = 0; i < Names.length; i++) {
          Names[i] = result.getColumnsList().get(i);
        }
        String[][] playerInfo = new String[rowSize][colSize];
        for(int i = 0; i < rowSize; i++) {
          for(int j = 0; j < colSize; j++) {
            playerInfo[i][j] = result.getRowList().get(i).get(j);
          }
        }

        // 以Names和playerInfo为参数，创建一个表格
        JFrame f = new JFrame();
        JTable table = new JTable(playerInfo, Names);
        // 设置此表视图的首选大小
        table.setPreferredScrollableViewportSize(new Dimension(550, 100));
        // 将表格加入到滚动条组件中
        JScrollPane scrollPane = new JScrollPane(table);
        f.getContentPane().add(scrollPane, BorderLayout.CENTER);
        // 再将滚动条组件添加到中间容器中
        f.setTitle("操作结果");
        f.pack();
        f.setVisible(true);
        f.addWindowListener(new WindowAdapter() {
          @Override
          public void windowClosing(WindowEvent e) {
            f.dispose();
          }
        });
      }
      //执行出错：输出错误信息
      else if(result.isIsAbort()) {
        System.out.println("Error! "+result.getErrorInfo());
      }
      //其它操作语句：输出操作结果
      else if(result.getResultInfo() != null) {
        List<String> infos = result.getResultInfo();
        for(String info: infos) {
          System.out.println(info);
        }
      }

    } catch (TException e) {
      e.printStackTrace();
      logger.error(e.getMessage());
    }
  }

  static Options createOptions() {
    Options options = new Options();
    options.addOption(Option.builder(HELP_ARGS)
            .argName(HELP_NAME)
            .desc("Display help information(optional)")
            .hasArg(false)
            .required(false)
            .build()
    );
    options.addOption(Option.builder(HOST_ARGS)
            .argName(HOST_NAME)
            .desc("Host (optional, default 127.0.0.1)")
            .hasArg(false)
            .required(false)
            .build()
    );
    options.addOption(Option.builder(PORT_ARGS)
            .argName(PORT_NAME)
            .desc("Port (optional, default 6667)")
            .hasArg(false)
            .required(false)
            .build()
    );
    return options;
  }

  static CommandLine parseCmd(String[] args) {
    Options options = createOptions();
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      logger.error(e.getMessage());
      println("Invalid command line argument!");
      System.exit(-1);
    }
    return cmd;
  }

  static void showHelp() {
    // TODO
    println("DO IT YOURSELF");
  }

  static void echoStarting() {
    println("----------------------");
    println("Starting ThssDB Client");
    println("----------------------");
  }

  static void print(String msg) {
    SCREEN_PRINTER.print(msg);
  }

  static void println() {
    SCREEN_PRINTER.println();
  }

  static void println(String msg) {
    SCREEN_PRINTER.println(msg);
  }

  public void test() {
    ArrayList<String> inputs = new ArrayList<>();
    inputs.add("CREATE DATABASE testdb;"+"CREATE TABLE person (name String(256), ID Int not null, PRIMARY KEY(ID));"+"CREATE TABLE course (stu_name String(256), course_name String(128) not null, PRIMARY KEY(course_name));"+"CREATE TABLE teach (ID Int not null, t_name String(256), course_name String(128) not null, s_name String(128), PRIMARY KEY(ID));");
    inputs.add("insert into person (name, ID) values ('Bob', 15);");
    inputs.add("insert into person values ('Allen', 22);");
    inputs.add("insert into person values ('Nami', 18);");
    inputs.add("insert into person (ID) values (23);");
    inputs.add("insert into course values ('Allen', 'RENZHIDAO');");
    inputs.add("insert into course values ('Allen', 'RUANJIANFENXI');");
    inputs.add("insert into course values ('Bob', 'SHUJUKU');");
    inputs.add("insert into teach values (1, 'JiLiang', 'YIDONG', 'Allen');");
    inputs.add("insert into teach values (2, 'JianMin', 'SHUJUKU', 'Bob');");
    inputs.add("insert into teach values (3, 'JianMin', 'SHUJUKU', 'Zera');");
    inputs.add("insert into teach values (4, 'ChunPing', 'RENZHIDAO', 'Allen');");
    inputs.add("show table person;");
//        inputs.add("update person set name = 'Emily' where name = 'Bob';");
    inputs.add("select * from person;");
//        inputs.add("select * from course where stu_name = 'Allen';");
//        inputs.add("select ID from person join course on person.name=course.stu_name;");
//    inputs.add("select distinct person.name, course.course_name from person join course on person.name=course.stu_name join teach on person.name=teach.s_name;");
    inputs.add("delete from person where ID = 15;");
    inputs.add("drop table teach;");

    for(String input: inputs) {
      execStatement(input);
    }
  }

  /*
  public void test()
          throws IOException {

    String createDatabaseStatement = "create database test;";

    String[] createTableStatements = {
        "create table department (dept_name String(20), building String(15), budget Double, primary key(dept_name));",
        "create table course (course_id String(8), title String(50), dept_name String(20), credits Int, primary key(course_id));",
        "create table instructor (i_id String(5), i_name String(20) not null, dept_name String(20), salary Float, primary key(i_id));",
        "create table student (s_id String(5), s_name String(20) not null, dept_name String(20), tot_cred Int, primary key(s_id));",
        "create table advisor (s_id String(5), i_id String(5), primary key (s_id));"
    };

    List<String> insertStatements = loadInsertStatements();

    execStatement(createDatabaseStatement);
    for(String s: createTableStatements) {
      execStatement(s);
    }
    for(String s: insertStatements) {
      execStatement(s);
    }

//    ArrayList<String> inputs = new ArrayList<>();
//    inputs.add("CREATE DATABASE testdb;"+"CREATE TABLE person (name String(256), ID Int not null, PRIMARY KEY(ID));"+"CREATE TABLE course (stu_name String(256), course_name String(128) not null, PRIMARY KEY(course_name));"+"CREATE TABLE teach (ID Int not null, t_name String(256), course_name String(128) not null, s_name String(128), PRIMARY KEY(ID));");
//    inputs.add("insert into person (name, ID) values ('Bob', 15);");
//    inputs.add("insert into person values ('Allen', 22);");
//    inputs.add("insert into person values ('Nami', 18);");
//    inputs.add("insert into person (ID) values (23);");
//    inputs.add("insert into course values ('Allen', 'RENZHIDAO');");
//    inputs.add("insert into course values ('Allen', 'RUANJIANFENXI');");
//    inputs.add("insert into course values ('Bob', 'SHUJUKU');");
//    inputs.add("insert into teach values (1, 'JiLiang', 'YIDONG', 'Allen');");
//    inputs.add("insert into teach values (2, 'JianMin', 'SHUJUKU', 'Bob');");
//    inputs.add("insert into teach values (3, 'JianMin', 'SHUJUKU', 'Zera');");
//    inputs.add("insert into teach values (4, 'ChunPing', 'RENZHIDAO', 'Allen');");
//    inputs.add("show table person;");
////        inputs.add("update person set name = 'Emily' where name = 'Bob';");
//    inputs.add("select * from person;");
////        inputs.add("select * from course where stu_name = 'Allen';");
////        inputs.add("select ID from person join course on person.name=course.stu_name;");
////    inputs.add("select distinct person.name, course.course_name from person join course on person.name=course.stu_name join teach on person.name=teach.s_name;");
//    inputs.add("delete from person where ID = 15;");
//    inputs.add("drop table teach;");
//
//    for(String input: inputs) {
//      execStatement(input);
//    }
  }*/

  private static List<String> loadInsertStatements() throws IOException {
    List<String> statements = new ArrayList<>();
    File file = new File("insert_into.sql");
    if (file.exists() && file.isFile()) {
      FileInputStream fileInputStream = new FileInputStream(file);
      InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
      BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        statements.add(line);
      }
      bufferedReader.close();
      inputStreamReader.close();
      fileInputStream.close();
    }
    return statements;
  }

}
