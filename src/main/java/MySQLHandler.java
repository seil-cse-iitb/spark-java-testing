import javax.rmi.CORBA.Util;
import java.sql.*;

public class MySQLHandler {
    String host;
    String password;
    String databaseName;
    String username;
    private Connection con;

    public MySQLHandler(String host, String username, String password, String databaseName) {
        this.host = host;
        this.password = password;
        this.databaseName = databaseName;
        this.username = username;
    }

    public Connection getConnection() {
        if (this.con != null) {
            return con;
        }
        this.con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection(
                    "jdbc:mysql://" + host + ":3306/" + databaseName, username, password);
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
