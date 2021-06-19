package cn.edu.thssdb.transaction;

import cn.edu.thssdb.exception.FileException;
import cn.edu.thssdb.exception.NullPointerException;
import cn.edu.thssdb.query.*;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Table;

import java.awt.*;
import java.io.*;
import java.util.*;



public class TransactionManager2PL{
    public Database db;
    private HashMap<String, Session> tableWriteLock;
    private HashMap<String, Session> tableReadLock;
    public static int READ_COMMITED = 1;
    public static int REPEATABLE_READ = 2;
    public static int SERIALIZABLE = 3;
    private int isolation;

    public TransactionManager2PL(Database db, int level){
        this.db = db;
        this.tableWriteLock = new HashMap<>();
        this.tableReadLock = new HashMap<>();
        this.isolation = level;
    }

    //根据锁判断是否能进行该session的一系列操作
    public synchronized void beginTransaction(Session session) throws IOException, ClassNotFoundException {
        System.out.println("session" + session.getID() + " add into 2PL");
        if(session == null)
            throw new NullPointerException(NullPointerException.Session);

        db.lock.writeLock().lock();
        boolean canProceed = setWaitedSession(session);
        if(canProceed){
            session.inTransaction = true;
            lockTables(session);
            beginExecution(session);
        }
        else if(!session.isAbort){
            setWaitingSession(session);
            System.out.println("session" + session.getID() + " waiting");
        }
        db.lock.writeLock().unlock();

        if(session.isAbort)
            commit(session);
    }


    //逐条执行session中的语句
    public void beginExecution(Session session) throws IOException, ClassNotFoundException {
        System.out.println("session " + session.getID() + " start exec");
//        try{
//            Thread.currentThread().sleep(7000);
//        }
//        catch (Exception e){}

        if(session == null)
            throw new NullPointerException(NullPointerException.Session);

        /*
        if (session.f.exists()) {
            //session.f.createNewFile();
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(session.f));
            while(ois.readObject()!=null) {
                String tablename = (String) ois.readObject();
                int type = (int) ois.readObject();
                LinkedList<LinkedList> oldvalue = (LinkedList<LinkedList>) ois.readObject();
                LinkedList<LinkedList> newvalue = (LinkedList<LinkedList>) ois.readObject();
                ois.close();
                Table table = db.getTable(tablename);
                if (type == 1) {
                    table.insert(newvalue);
                }
                if (type == 2) {
                    Entry key = table.getKey(oldvalue);
                    table.delete(key);
                }
                if (type == 3) {
                    table.insert(newvalue);
                    Entry key = table.getKey(oldvalue);
                    table.delete(key);
                }
            }
            session.f.delete();

        }
        else {

        }
        */

        int count = session.statement.size();
        for(int i=0; i<count; i++){
            Object cs = session.statement.get(i);


            try {
                String table_name = null;

                if (cs instanceof StatementInsert) {
                    StatementInsert cs1 = (StatementInsert) cs;
                    ExecResult res = cs1.exec(db);
                    table_name = cs1.gettable_name();
                    RowAction action = new RowAction(table_name, 1, null, res.getNewValue());

                    session.rowActionList.add(action);
                    //newvalue = res.getNewValue();
                    //type = 1;
                }
                else if(cs instanceof  StatementDelete){
                    StatementDelete cs2 = (StatementDelete) cs;
                    ExecResult res = cs2.exec(db);
                    table_name = cs2.gettable_name();
                    RowAction action = new RowAction(table_name, 2, res.getOldValue(), null);
                    session.rowActionList.add(action);
                    //oldvalue = res.getOldValue();
                    //type = 2;
                }
                else if(cs instanceof  StatementUpdate){
                    StatementUpdate cs3 = (StatementUpdate) cs;
                    ExecResult res = cs3.exec(db);
                    table_name = cs3.gettable_name();
                    RowAction action = new RowAction(table_name, 3, res.getOldValue(), res.getNewValue());
                    session.rowActionList.add(action);
                    //newvalue = res.getNewValue();
                    //oldvalue = res.getOldValue();
                    //type = 3;
                }

                else if(cs instanceof  StatementSelect){
                    StatementSelect cs4 = (StatementSelect) cs;
                    ExecResult res = cs4.exec(db);
                    unlockReadLock(session, cs4);
                    session.result = res;
                    //System.out.println("Transaction select exec");
                    //System.out.println(session.result == null);
                }

            }
            catch (Exception e){
                session.isAbort = true;
                rollback(session);
                break;
            }
            //session.f.delete();
        }

        System.out.println("session " + session.getID() + " finish exec");
        commit(session);
    }

    //判断该session要读写的表是否正在被别的session锁住
    //TODO:死锁判断
    private boolean setWaitedSession(Session session){
        if(session == null)
            throw new NullPointerException(NullPointerException.Session);

        session.temp.clear();
        for(String s: session.TableForWrite){
            Session holder = tableWriteLock.get(s);
            if(holder!=null && holder!=session){
                session.temp.add(holder);
            }

            /*
            if(isolation > READ_COMMITED) {
                holder = tableReadLock.get(s);
                if (holder != null && holder != session) {
                    session.temp.add(holder);
                }
            }
             */
        }

        if(isolation > READ_COMMITED) {
            for (String s : session.TableForRead) {
                Session holder = tableWriteLock.get(s);
                if (holder != null && holder != session) {
                    session.temp.add(holder);
                }
            }
        }

        //System.out.println(session.temp.isEmpty());

        if(session.temp.isEmpty())
            return true;


        if(!checkDeadLock(session, session.temp))
            session.isAbort = true;

        return false;
    }

    private boolean checkDeadLock(Session session, LinkedHashSet newWait){
        if(session == null)
            throw new NullPointerException(NullPointerException.Session);

        int count = session.waitingSession.size();
        Iterator<Session> it = session.waitingSession.iterator();
        while(it.hasNext()){
            Session current = it.next();
            if(newWait.contains(current))
                return false;
            if(!checkDeadLock(current, newWait))
                return false;
        }

        return true;
    }

    //将该session正在等待的目标session加入目标session中
    private void setWaitingSession(Session session){
        if(session == null)
            throw new NullPointerException(NullPointerException.Session);

        for(Session s: session.temp){
            s.waitingSession.add(session);
        }
        session.temp.clear();
    }

    //事务语句执行前，给要读写的表上锁
    private void lockTables(Session session){
        if(session == null)
            throw new NullPointerException(NullPointerException.Session);

        ArrayList<String> TableToWrite = session.getTableForWrite();
        for(String s: TableToWrite){
            this.tableWriteLock.put(s, session);
            System.out.println("add write lock: "+ s);
        }

        if(this.isolation > READ_COMMITED){
            ArrayList<String> TableToRead = session.getTableForRead();
            for(String s: TableToRead){
                this.tableReadLock.put(s, session);
                System.out.println("add read lock");
            }
        }

    }


    //持久化存储，并释放所有锁

    public void commit(Session session) throws IOException, ClassNotFoundException {

        if(session == null)
            throw new NullPointerException(NullPointerException.Session);

        db.lock.writeLock().lock();
        //持久化存储写过的表
        if(!session.isAbort) {
            writeLog(session);
            //FileOutputStream fileInputStream = new FileOutputStream(session.f);
            //ObjectOutputStream oos = new ObjectOutputStream(fileInputStream);
            //oos.writeObject(session.rowActionList);
            //oos.close();
            //fileInputStream.close();

            for (String s : session.TableForWrite) {
                try {
                    db.getTable(s).persist();
                } catch (Exception e) {
                    rollback(session);
                }
            }
            //session.result.add(new ExecResult("transaction execution succeed!"));
        }

        unlockTables(session);
        db.lock.writeLock().unlock();


        System.out.println("session " + session.getID() + " finish commit");
        session.done = true;
    }

    //commit后，释放所有锁
    private void unlockTables(Session session) throws IOException, ClassNotFoundException {
        if(session == null)
            throw new NullPointerException(NullPointerException.Session);

        Iterator it = tableWriteLock.values().iterator();
        while (it.hasNext()) {
            Session s = (Session) it.next();
            if (s == session) {
                System.out.println("free write lock");
                it.remove();
            }
        }

        it = tableReadLock.values().iterator();
        while (it.hasNext()) {
            Session s = (Session) it.next();
            if (s == session) {
                System.out.println("free read lock");
                it.remove();
            }
        }
        resetlock(session);
    }

    private void resetlock(Session session) throws IOException, ClassNotFoundException {
        if(session == null)
            throw new NullPointerException(NullPointerException.Session);

        int count = session.waitingSession.size();
        if(count <= 0)
            return;

        Iterator<Session> it = session.waitingSession.iterator();
        while(it.hasNext()){
            Session cur = it.next();
            boolean canProceed = setWaitedSession(cur);
            if(canProceed){
                lockTables(cur);
                beginExecution(cur);
            }
            else{
                setWaitingSession(cur);
            }
        }
    }


    private void unlockReadLock(Session session, StatementSelect cs){
        if(isolation == REPEATABLE_READ) {
            LinkedList<String> table_list = cs.getTargetList();
            for (String s : table_list) {
                System.out.println("free read lock");
                tableReadLock.remove(s);
            }
        }
    }

    //TODO：回滚具体操作，后续完成
    private void rollback(Session session){
        db.lock.writeLock().lock();
        int count = session.rowActionList.size();
        try{
            for(int i=count-1; i>=0; i++){
                RowAction tem = session.rowActionList.get(i);
                if (tem.getType() == 1) {
                    for(LinkedList row: tem.getNewRow()){
                        Table target = db.getTable(tem.getTable_name());
                        Entry key = target.getKey(row);
                        target.delete(key);
                    }
                }
                if (tem.getType() == 2) {
                    for(LinkedList row: tem.getOldRow()){
                        Table target = db.getTable(tem.getTable_name());
                        target.insert(row);
                    }
                }
                if (tem.getType() == 3) {
                    for(LinkedList row: tem.getNewRow()){
                        Table target = db.getTable(tem.getTable_name());
                        Entry key = target.getKey(row);
                        target.delete(key);
                    }

                    for(LinkedList row: tem.getOldRow()){
                        Table target = db.getTable(tem.getTable_name());
                        target.insert(row);
                    }
                }
            }
        }
        catch (Exception e){

        }

        db.lock.writeLock().unlock();
    }

    private void writeLog(Session session){
        try{
            StringBuilder sb = new StringBuilder();
            for(RowAction action: session.rowActionList){
                String insert = "insert";
                String delete = "delete";
                if(action.getNewRow()!=null){
                    for(LinkedList row: action.getNewRow()){
                        int count = row.size();
                        sb.append(action.getTable_name()+" "+insert);
                        for(int i=0; i<count; i++){
                            sb.append(" "+ row.get(i).toString());
                        }
                        sb.append("\n");
                    }
                }

                if(action.getOldRow()!=null){
                    for(LinkedList row: action.getOldRow()){
                        int count = row.size();
                        sb.append(action.getTable_name()+" "+delete);
                        for(int i=0; i<count; i++){
                            sb.append(" "+row.get(i).toString());
                        }
                        sb.append("\n");
                    }
                }
            }

            FileWriter writer = new FileWriter(session.f);
            writer.write(sb.toString());
            writer.close();
        }
        catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

}
