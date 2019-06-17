package com.atguigu.sparkmall.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Jdbc {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        String className = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://gz-cdb-eivmcin2.sql.tencentcdb.com:62539/hive";
        String user = "root";
        String password = "rrttcch123";

        //1. 加载驱动
        Class.forName(className);

        //2. 获取连接
        Connection conn = DriverManager.getConnection(url, user, password);

        System.out.println(conn);
    }
}
