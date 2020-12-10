package ceng.estu;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class Utils {

    public static Connection getConnection()  {
        String DB_URL = "jdbc:mysql://localhost:3306/oraclejdbc";

        Properties connectionProps = new Properties();
        connectionProps.put("user", "root");
        connectionProps.put("password", "");

        Connection conn  = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(DB_URL,
                            connectionProps);
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }

        System.out.println("Connected to database");
        return conn;
    }
}
