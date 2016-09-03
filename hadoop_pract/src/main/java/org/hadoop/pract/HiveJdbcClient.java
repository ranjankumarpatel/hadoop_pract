package org.hadoop.pract;

/**
 * Created by RANJAN on 19-Jun-16.
 */

import java.sql.*;

public class HiveJdbcClient {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
// TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = DriverManager.getConnection("jdbc:hive2://192.168.0.104:10000/gmr", "maria_dev", "maria_dev");
        Statement stmt = con.createStatement();

// show tables
        String sql = "show tables";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }

    }
}