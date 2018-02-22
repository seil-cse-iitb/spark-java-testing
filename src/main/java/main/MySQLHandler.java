package main;

import java.sql.*;

public class MySQLHandler {
    String host;
    String password;
    String databaseName;
    String username;
    String url;
    private Connection con;

    public MySQLHandler(String host, String username, String password, String databaseName) {
        this.host = host;
        this.password = password;
        this.databaseName = databaseName;
        this.username = username;
        this.url ="jdbc:mysql://" + host + ":3306/"+this.databaseName ;
        this.buildConnection();
    }

    public Connection buildConnection() {
        if (this.con != null) {
            return con;
        }
        this.con = null;
        try {
//            Class.forName("com.mysql.jdbc.Driver"); //deprecated
            con = DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            LogHandler.logError("[MySQL Connection]" + e.getMessage());
            UtilsHandler.exit_thread();
        }
        if (con == null) {
            LogHandler.logError("[MySQL Connection]Con is null");
            UtilsHandler.exit_thread();
        }
        return con;
    }

    public ResultSet query(String sql) throws SQLException {
        Statement stmt = con.createStatement();
        return stmt.executeQuery(sql);
    }

}
