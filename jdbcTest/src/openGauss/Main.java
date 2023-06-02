package openGauss;

import java.sql.*;
import java.util.*;
import java.sql.ResultSet;

import openGauss.MyTable;

public class Main {
    public static Connection c;
    public static HashMap<String, MyTable> dir = new HashMap<String, MyTable>();
    public static int threshold = 1;

    public static int max(int a, int b){
        return a>b?a:b;
    }
    public static void loadMap(){
        dir.put("company1",new MyTable(
                "(ID INT PRIMARY KEY     NOT NULL," +
                " NAME           TEXT    NOT NULL, " +
                " AGE            INT     NOT NULL, " +
                " ADDRESS        CHAR(50), " +
                " SALARY         REAL)",true,2,0,0));
    }
    public  static Connection getConnect(String username, String passwd)
    {
        //驱动类。
        String driver = "org.postgresql.Driver";
        //数据库连接描述符。
        String sourceURL = "jdbc:postgresql://10.181.35.109:33333/testdb";

        try
        {
            //加载驱动。
            Class.forName(driver);
        }
        catch( Exception e )
        {
            e.printStackTrace();
            return null;
        }

        try
        {
            //创建连接。

//            Statement stmt = null;
            try {
                // 这个是连接，该一下中文的部分，就是之前你用database连数据库的那些参数
                c = DriverManager.getConnection(sourceURL, username, passwd);
                // 连接成功
                System.out.println("Opened database successfully");
//          这里创建一个类似于可视化工具中的console的那个脚本文件
//                stmt = c.createStatement();
////            这里写sql语句，做创建表的演示
//                String sql = "CREATE TABLE COMPANY1 " +
//                        "(ID INT PRIMARY KEY     NOT NULL," +
//                        " NAME           TEXT    NOT NULL, " +
//                        " AGE            INT     NOT NULL, " +
//                        " ADDRESS        CHAR(50), " +
//                        " SALARY         REAL)";
////            String sql = "DROP TABLE COMPANY";
//                stmt.executeUpdate(sql);
////            关闭脚本文件
//                stmt.close();
//            结束连接
//                c.close();
            } catch ( Exception e ) {
                System.err.println( e.getClass().getName()+": "+ e.getMessage() );
                System.exit(0);
            }
            System.out.println("Connection succeed!");
        }
        catch(Exception e)
        {
            e.printStackTrace();
            return null;
        }

        return c;
    }

    public static void execupdate(String sql) throws SQLException {
        Statement stmt = c.createStatement();
//            这里写sql语句，做创建表的演示
        stmt.executeUpdate(sql);
//            关闭脚本文件
        stmt.close();
    }

    public static void updateValue() throws SQLException {
        DatabaseMetaData metaData = c.getMetaData();
        Statement stmt = c.createStatement();
        ResultSet hotTable = stmt.executeQuery("select relname,n_tup_ins,n_tup_upd,n_tup_del from pg_stat_user_tables");
        while (hotTable.next()) {
            String TABLE_NAME = hotTable.getString("relname");
            if(dir.get(TABLE_NAME)!=null){
                int ins = hotTable.getInt("n_tup_ins");
                int dele = hotTable.getInt("n_tup_del");
                int upd = hotTable.getInt("n_tup_upd");
               dir.get(TABLE_NAME).insert = hotTable.getInt("n_tup_ins");
                dir.get(TABLE_NAME).del = hotTable.getInt("n_tup_del");
                dir.get(TABLE_NAME).update = hotTable.getInt("n_tup_upd");
            }
        }
        stmt.close();
    }

    public static void detect() throws SQLException {
        DatabaseMetaData metaData = c.getMetaData();
        Statement stmt = c.createStatement();
        ResultSet hotTable = stmt.executeQuery("select relname,n_tup_ins,n_tup_upd,n_tup_del from pg_stat_user_tables");
        while (hotTable.next()) {
            // 获取表名
            String TABLE_NAME = hotTable.getString("relname");
            //在这里做行转列or列转行
            if(dir.get(TABLE_NAME)!=null){
                //相加大于阈值，作为行表。否则变成列表
                int ins = hotTable.getInt("n_tup_ins");
                int del = hotTable.getInt("n_tup_del");
                int upd = hotTable.getInt("n_tup_upd");
                int hot = hotTable.getInt("n_tup_ins")+hotTable.getInt("n_tup_del")+hotTable.getInt("n_tup_upd")-dir.get(TABLE_NAME).del-dir.get(TABLE_NAME).update-dir.get(TABLE_NAME).insert;
                if( hot >= threshold){
                    //列转行
                    if(!dir.get(TABLE_NAME).isRow){
                        System.out.println("It's hot:"+ins+":"+del + ":" + upd +":"+ dir.get(TABLE_NAME).del+":"+dir.get(TABLE_NAME).update+":"+dir.get(TABLE_NAME).insert);
                        Statement stm = c.createStatement();
                        ResultSet t = stm.executeQuery("select count(*) as c from "+TABLE_NAME);
                        t.next();
                        dir.get(TABLE_NAME).insert = t.getInt("c");
                        stm.close();
                        dir.get(TABLE_NAME).del = 0;
                        dir.get(TABLE_NAME).update = 0;
                        execupdate("start transaction");
                        execupdate("CREATE TABLE temp " +dir.get(TABLE_NAME).createsql);
                        execupdate("insert into temp select * from "+TABLE_NAME);
                        execupdate("drop table "+TABLE_NAME);
                        execupdate("alter table temp rename to "+TABLE_NAME);
                        execupdate("commit");
                        dir.get(TABLE_NAME).isRow = true;
                    }
                    else{
                        System.out.println("热不转");
                        dir.get(TABLE_NAME).insert = max(hotTable.getInt("n_tup_ins"),dir.get(TABLE_NAME).insert);
                        dir.get(TABLE_NAME).del = max(hotTable.getInt("n_tup_del"),dir.get(TABLE_NAME).del);
                        dir.get(TABLE_NAME).update = max(hotTable.getInt("n_tup_upd"),dir.get(TABLE_NAME).update);
                    }
                }
                else{
                    //行转列
                    if(dir.get(TABLE_NAME).isRow){
                        System.out.println("It's cold:"+ins+":"+del + ":" + upd +":"+ dir.get(TABLE_NAME).del+":"+dir.get(TABLE_NAME).update+":"+dir.get(TABLE_NAME).insert);
                        Statement stm = c.createStatement();
                        ResultSet t = stm.executeQuery("select count(*) as c from "+TABLE_NAME);
                        t.next();
                        dir.get(TABLE_NAME).insert = t.getInt("c");
                        stm.close();
                        dir.get(TABLE_NAME).del = 0;
                        dir.get(TABLE_NAME).update = 0;
                        execupdate("start transaction");
                        execupdate("CREATE TABLE temp " +dir.get(TABLE_NAME).createsql+" with(orientation = column)");
                        execupdate("insert into temp select * from "+TABLE_NAME);
                        execupdate("drop table "+TABLE_NAME);
                        execupdate("alter table temp rename to "+TABLE_NAME);
                        execupdate("commit");
                        dir.get(TABLE_NAME).isRow = false;
                    }
                    else{
                        System.out.println("冷不转");
                        dir.get(TABLE_NAME).insert = max(hotTable.getInt("n_tup_ins"),dir.get(TABLE_NAME).insert);
                        dir.get(TABLE_NAME).del = max(hotTable.getInt("n_tup_del"),dir.get(TABLE_NAME).del);
                        dir.get(TABLE_NAME).update = max(hotTable.getInt("n_tup_upd"),dir.get(TABLE_NAME).update);
                    }
                }
            }
        }
        stmt.close();
    }

    public static void main(String[] args) throws SQLException {
        // TODO 自动生成的方法存根
        new Main();
        loadMap();
        Main.getConnect("ly","abc123456.");
        Timer timer = new Timer();
        updateValue();
        //5s做一次detect
        timer.schedule(new TimerTask() {
            int num = 1;
            @Override
            public void run(){
                try{
                    Main.detect();
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
                System.out.println(String.valueOf(num)+"detect end.");
                num+=1;
            }
        }, 0, 10000);
    }
}
