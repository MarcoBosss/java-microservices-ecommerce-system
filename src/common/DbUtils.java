package common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DbUtils {
    public static Connection getDBConnection(String ip, int port, String name, String password) throws SQLException {
        return DriverManager.getConnection("jdbc:postgresql://" 
        + ip + ":" + port + "/" + name, "postgres", password);
    }
}